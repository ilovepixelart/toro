"""Integration: the data model has a version (docs/specs/one-point-oh.md).

Two toro versions share one Redis during any rolling upgrade, which is the normal
case rather than the exception. The API's version says what a caller may rely on; this
says what the *keys* mean, so a library that finds a shape it was not built for stops
instead of writing into it.
"""

import asyncio
import contextlib

import pytest

import toro
from toro import IncompatibleDataModelError, Queue, Worker

PREFIX = "torotest"


async def _noop(job):
    return None


async def test_a_queue_is_stamped_on_first_use(q):
    """ON-003: the marker is written by the first thing that writes, so an operator
    reading the keys can always tell which shape they are looking at."""
    assert await q.redis.hget(q.keys.meta, "model") is None

    await q.add("j", {})

    assert await q.redis.hget(q.keys.meta, "model") == str(toro.DATA_MODEL_VERSION)


async def test_a_newer_model_is_refused_by_name(q):
    """ON-003: reading a shape from the future is how a rolling upgrade corrupts a
    queue. Both numbers are in the message because the first question is "which side
    is old", and the answer is never in the traceback."""
    await q.redis.hset(q.keys.meta, "model", "99")

    with pytest.raises(IncompatibleDataModelError) as caught:
        await q.add("j", {})

    assert "99" in str(caught.value)
    assert str(toro.DATA_MODEL_VERSION) in str(caught.value)
    assert (await q.counts())["wait"] == 0, "it wrote anyway"


async def test_an_older_model_is_not_refused(q):
    """A model older than ours is what an upgrade looks like from the new side, and
    the new side is the one that knows how to read both."""
    await q.redis.hset(q.keys.meta, "model", "0")

    await q.add("j", {})

    assert (await q.counts())["wait"] == 1


async def test_an_unmarked_queue_is_adopted(q):
    """ON-005: a queue created before the marker existed has state and no marker.
    Refusing it would strand every queue in production on the day of the upgrade."""
    await q.add("before", {})
    await q.redis.hdel(q.keys.meta, "model")  # what a 0.x queue looks like
    # a process that has never seen this queue, which is the only way an unmarked
    # queue is ever met: the marker is asked about once per process
    fresh = Queue(q.name, prefix=PREFIX, connection=q.redis)

    await fresh.add("after", {})

    assert await q.redis.hget(q.keys.meta, "model") == str(toro.DATA_MODEL_VERSION)
    assert (await q.counts())["wait"] == 2  # the job from before is untouched


async def test_the_model_is_checked_once_per_process(q, monkeypatch):
    """ON-004: a check on every call would put a round trip on the hot path to catch
    a condition that changes once, during an upgrade."""
    checks = 0
    real = q._stamp  # the registered script: the round trip itself, not the call

    async def counting(*args, **kwargs):
        nonlocal checks
        checks += 1
        return await real(*args, **kwargs)

    monkeypatch.setattr(q, "_stamp", counting)

    for _ in range(5):
        await q.add("j", {})
    await q.add_flow("p", {}, children=[toro.FlowChild("c", {})])

    assert checks == 1, f"{checks} round trips for one question"


async def test_a_worker_checks_before_it_claims(q):
    """A worker writes more than a producer does: it must not start a claim loop
    against a model it cannot read."""
    await q.redis.hset(q.keys.meta, "model", "99")
    worker = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis, stalled_interval=0)

    with pytest.raises(IncompatibleDataModelError):
        await worker.run()


async def test_a_worker_on_a_readable_model_runs(q, run_until):
    """The other half: the check must not be a new way for a worker to fail."""
    done: list[str] = []

    async def proc(job):
        done.append(job.id)

    worker = Worker(q.name, proc, prefix=PREFIX, connection=q.redis, stalled_interval=0)
    task = asyncio.create_task(worker.run())
    try:
        await q.add("j", {})
        assert await run_until(lambda: len(done) >= 1, timeout=15)
    finally:
        await worker.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_the_marker_is_not_a_job_id(q):
    """`meta` is a key of the queue's own, so a job may not take that id: the keys
    module owns that rule and the marker has to be inside it."""
    with pytest.raises(ValueError, match="reserved"):
        await q.add("j", {}, job_id="meta")


async def test_a_reader_does_not_stamp_a_queue_it_only_reads(q):
    """A dashboard opens a Queue for every name it is given, including ones nobody
    has used yet. Stamping on a read would create those queues by looking at them."""
    reader = Queue(q.name, prefix=PREFIX, connection=q.redis)

    await reader.counts()
    await reader.get_jobs("wait", 0, 10)

    assert await q.redis.hget(q.keys.meta, "model") is None


# Everything that only READS. Anything else on the public surface writes, and a write
# against a data model this library does not understand is the thing the marker
# exists to stop. A new method has to be classified in one of these two places.
READS = {
    "children_results",
    "counts",
    "departed_workers",
    "failed_children",
    "flow_metrics",
    "flow_percentiles",
    "flow_progress",
    "flow_view",
    "get_flow",
    "get_job",
    "get_jobs",
    "get_jobs_roots",
    "get_logs",
    "is_paused",
    "latency",
    "lifetime_totals",
    "metrics",
    "metrics_by_name",
    "metrics_text",
    "pending",
    "percentiles",
    "result",
    "roots_counts",
    "schedulers",
    "search",
    "close",
    "workers",  # prunes expired presence records, which is housekeeping, not a write
}


def test_every_write_path_checks_the_data_model():
    """ON-003: the check covered `add`, `add_flow`, `flush` and `Worker.run`, so
    `add_scheduler` (a full enqueue path), `clean`, `pause`, `cancel_job` and every
    other admin write went into a queue whose model they had not read."""
    import inspect

    unguarded = [
        name
        for name, member in inspect.getmembers(Queue, inspect.iscoroutinefunction)
        if not name.startswith("_")
        and name not in READS
        and not getattr(member, "__toro_writes__", False)
    ]
    assert unguarded == [], f"these write without checking the model: {unguarded}"


async def test_an_admin_write_against_a_newer_model_is_refused(q):
    await q.add("j", {})  # stamp it while we still can
    await q.redis.hset(q.keys.meta, "model", "99")
    fresh = Queue(q.name, prefix=PREFIX, connection=q.redis)

    for call in (
        fresh.pause(),
        fresh.clean("wait"),
        fresh.add_scheduler("nightly", every=60_000, name="rollup"),
        fresh.cancel_job("1"),
        fresh.remove_job("1"),
    ):
        with pytest.raises(IncompatibleDataModelError):
            await call
