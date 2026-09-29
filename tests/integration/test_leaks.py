"""Resource hygiene: a job (or a whole flow) that runs its course must leave
NOTHING behind - no hash, lock, logs, or flow aux keys, and no stray entry in the
children index or the roots scratch - and a worker that stops must leak no
background tasks. The fuzzer checks orphans mid-run; this pins the end state.
"""

import asyncio
import contextlib

import pytest

from toro import FlowChild as c  # noqa: N813
from toro import Queue, Worker
from toro.job import Job

PREFIX = "torotest"

# Keys that legitimately outlive any single job: counters, the wakeup marker, the
# state sets themselves, and the self-expiring metrics buckets / presence records.
_INFRA = {
    "id", "pc", "marker", "prioritized", "active", "delayed", "completed", "failed",
    "waiting-children", "children", "stalled", "stalled-check", "meta-paused",
    "limiter", "repeat", "workers", "departed", "roots-scratch", "events",
    "cancelled", "cancel", "held", "totals", "meta",
}  # fmt: skip


async def _noop(job):
    return None


async def _leaked_keys(q: Queue) -> list[str]:
    """Per-job/aux keys left under the queue's namespace (infra keys excluded).

    An add() replay record (`add:<token>`) or a finish's answer (`fin:...`) is not a
    leak while it carries the expiry the script gives it: it goes on its own.
    """
    base = q.keys.base
    leaked = []
    for key in await q.redis.keys(base + "*"):
        suffix = key[len(base) :]
        if suffix in _INFRA or suffix.split(":")[0] in ("metrics", "worker", "repeat"):
            continue
        if suffix.startswith(("add:", "fin:")) and await q.redis.ttl(key) > 0:
            continue
        leaked.append(suffix)
    return leaked


async def _process(w: Worker, *, succeed: bool = True) -> bool:
    loaded = await w._acquire()
    if loaded is None:
        return False
    job = Job.from_hash(loaded[0], loaded[1])
    if succeed:
        await w._finish_completed(job, {"ok": 1})
    else:
        await w._finish_failed(job, RuntimeError("boom"))
    return True


# ---- a finished job leaves nothing behind ------------------------------------------


async def test_completed_then_autoremoved_leaves_no_keys(q):
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    job = await q.add("j", {"x": 1}, remove_on_complete=True)
    await q.redis.rpush(q.keys.logs(job.id), "a log line")  # the job logged something

    assert await _process(w, succeed=True)

    assert await q.redis.exists(q.keys.job(job.id)) == 0
    assert await q.redis.exists(q.keys.logs(job.id)) == 0  # logs cleaned too
    assert await q.redis.exists(q.keys.lock(job.id)) == 0
    assert await _leaked_keys(q) == []


async def test_failed_then_autoremoved_leaves_no_keys(q):
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    await q.add("j", {}, attempts=1, remove_on_fail=True)

    assert await _process(w, succeed=False)

    assert await _leaked_keys(q) == []


@pytest.mark.parametrize(
    ("succeed", "opts"),
    [(True, {}), (False, {"attempts": 1}), (False, {"attempts": 3, "backoff": 5000})],
    ids=["completed", "failed", "retry-with-backoff"],
)
async def test_a_retained_finish_leaves_no_lock(q, succeed, opts):
    """The finish script drops the lock itself. Without that, a job kept in its state
    set holds a lock key nothing renews or deletes until it expires."""
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    job = await q.add("j", {}, **opts)

    loaded = await w._acquire()
    assert await q.redis.exists(q.keys.lock(job.id)) == 1
    claimed = Job.from_hash(loaded[0], loaded[1])
    if succeed:
        await w._finish_completed(claimed, {"ok": 1})
    else:
        await w._finish_failed(claimed, RuntimeError("boom"))

    assert await q.redis.exists(q.keys.job(job.id)) == 1  # retained
    assert await q.redis.exists(q.keys.lock(job.id)) == 0


async def test_a_cancelled_running_job_leaves_no_lock(q):
    """Committing a cancellation drops the lock, as every other finish does."""
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    job = await q.add("j", {})
    loaded = await w._acquire()
    assert await q.cancel_job(job.id) is True  # running: only asks the worker to stop

    await w._finish_cancelled(Job.from_hash(loaded[0], loaded[1]))

    assert await q.redis.hget(q.keys.job(job.id), "state") == "cancelled"
    assert await q.redis.exists(q.keys.lock(job.id)) == 0


# ---- a whole flow leaves nothing behind --------------------------------------------


async def test_completed_flow_leaves_no_aux_keys_or_children_index(q):
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    # every node auto-removes (a retained child legitimately stays in the index,
    # so to prove the flow leaves NOTHING behind, remove the children too)
    parent = await q.add_flow(
        "p",
        {},
        children=[c("a", {}, remove_on_complete=True), c("b", {}, remove_on_complete=True)],
        remove_on_complete=True,
    )
    # children index is populated while the flow exists
    assert await q.redis.zcard(q.keys.children) == 2

    # process both children, then the released parent
    for _ in range(5):
        if not await _process(w, succeed=True):
            break

    assert await q.redis.exists(q.keys.deps(parent.id)) == 0
    assert await q.redis.exists(q.keys.results(parent.id)) == 0
    assert await q.redis.exists(q.keys.cfail(parent.id)) == 0
    assert await q.redis.zcard(q.keys.children) == 0  # index pruned with the nodes
    assert await _leaked_keys(q) == []


async def test_removed_flow_leaves_no_keys(q):
    parent = await q.add_flow("p", {}, children=[c("a", {}), c("b", {}, children=[c("d", {})])])
    assert await q.redis.zcard(q.keys.children) == 3  # a, b, d are all children

    await q.remove_job(parent.id)  # cascades the whole subtree

    assert await q.redis.zcard(q.keys.children) == 0
    assert await _leaked_keys(q) == []


# ---- the roots scratch key never persists ------------------------------------------


async def test_roots_queries_leave_no_scratch_key(q):
    await q.add_flow("p", {}, children=[c("a", {}), c("b", {})])
    await q.add("solo", {})

    for state in ("wait", "completed", "failed", "waiting-children", "active"):
        await q.get_jobs_roots(state, 0, 50)
    await q.roots_counts()

    # ZDIFFSTORE writes the scratch then DELs it inside the same atomic script
    assert await q.redis.exists(q.keys.roots_scratch) == 0


async def test_a_roots_page_leaves_no_scratch_key(q):
    """A page of roots on its own: the state holds a root, so the diff writes the
    scratch, and nothing after this call would clear it."""
    await q.add_flow("p", {}, children=[c("a", {})])
    await q.add("solo", {})

    total, roots = await q.get_jobs_roots("wait", 0, 10)

    assert (total, [j.name for j in roots]) == (1, ["solo"])
    assert await q.redis.exists(q.keys.roots_scratch) == 0


async def test_roots_counts_leave_no_scratch_key_after_the_last_state(q):
    """`cancelled` is the last state counted, so a cancelled root is what leaves the
    scratch written at the end of the call."""
    job = await q.add("doomed", {})
    assert await q.cancel_job(job.id) is True

    assert (await q.roots_counts())["cancelled"] == 1
    assert await q.redis.exists(q.keys.roots_scratch) == 0


# ---- a stopped worker leaks no background tasks ------------------------------------


async def test_stopped_worker_leaks_no_tasks(q, run_until):
    done = []

    async def proc(job):
        done.append(job.id)

    before = asyncio.all_tasks()
    worker = Worker(q.name, proc, prefix=PREFIX, connection=q.redis, heartbeat_interval=50)
    task = asyncio.create_task(worker.run())
    await q.add("j", {})
    assert await run_until(lambda: len(done) >= 1, timeout=10)
    await worker.stop()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # Nothing the worker started is still pending: its background loops, and the
    # per-run tasks (a job's lock renewer, its processor) that stop() does not gather.
    leaked = asyncio.all_tasks() - before
    assert not leaked, [t.get_name() for t in leaked]
    await worker.stop()  # idempotent: a second stop must not raise


def _open(client) -> int:
    """Connections this client's own pool still holds open. The server's total counts
    every other test's clients too, which makes it useless as an assertion. redis-py
    exposes no public accessor for a pool's connections, so the private one is reached
    here and nowhere else.
    """
    pool = client.connection_pool
    held = [*pool._available_connections, *pool._in_use_connections]
    return sum(1 for c in held if c.is_connected)


async def test_closing_a_queue_gives_its_sockets_back(q):
    """`close()` has to release the connections it opened, not just the one the client
    holds: the rest stay open until the garbage collector reaches them, which on a
    closed event loop is a traceback at exit."""
    other = Queue(q.name, prefix=PREFIX)
    await asyncio.gather(*(other.counts() for _ in range(5)))  # open a few
    assert _open(other.redis) > 1

    await other.close()

    assert _open(other.redis) == 0


async def test_stopping_a_worker_gives_its_sockets_back(q):
    """The same for a worker, which parks one connection per process loop."""
    w = Worker(q.name, _noop, prefix=PREFIX, concurrency=4, stalled_interval=0)
    task = asyncio.create_task(w.run())
    await asyncio.sleep(0.3)
    assert _open(w.redis) > 1

    await w.stop()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert _open(w.redis) == 0


async def test_a_queue_leaves_a_connection_it_was_given_alone(q):
    """A caller-provided connection belongs to the caller: closing a queue that shares
    it must not disconnect it under the others."""
    shared = Queue(q.name, prefix=PREFIX)
    try:
        one = Queue(q.name, prefix=PREFIX, connection=shared.redis)
        two = Queue(q.name, prefix=PREFIX, connection=shared.redis)
        await one.close()

        assert await two.counts() is not None  # still usable
    finally:
        await shared.close()


async def _noop(job: Job) -> None:
    return None


async def test_a_stopped_worker_gives_back_its_cancel_subscription(q):
    """A worker subscribes for cancellations for as long as it runs. On a connection
    the caller owns, `stop()` cannot disconnect the pool, so a subscription it fails
    to close keeps a connection checked out and still subscribed: enough start/stop
    cycles and every command blocks waiting for a free one."""
    for _ in range(3):
        worker = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis, stalled_interval=0)
        task = asyncio.create_task(worker.run())
        for _ in range(100):  # wait for the subscription to land
            if await q.redis.pubsub_channels(q.keys.cancel):
                break
            await asyncio.sleep(0.02)
        await worker.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert await q.redis.pubsub_channels(q.keys.cancel) == []


async def test_a_removed_jobs_cleanup_leaves_no_stub(q, run_worker, run_until):
    """A processor's cleanup may report progress or log after its job was removed.
    Written blindly, those recreated the hash as a stub with no state: unlistable,
    unremovable, and holding the custom id so a later add() with it did nothing."""
    started = asyncio.Event()
    cleaned_up = asyncio.Event()

    async def proc(job):
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            await job.update_progress(100)
            await job.log("cleaned up")
            cleaned_up.set()

    job = await q.add("order", {}, job_id="order-1")
    async with run_worker(q, proc):
        await asyncio.wait_for(started.wait(), 5)
        assert await q.remove_job(job.id) is True
        await asyncio.wait_for(cleaned_up.wait(), 5)

    assert await q.redis.exists(q.keys.job(job.id)) == 0
    assert await q.redis.exists(q.keys.logs(job.id)) == 0
    again = await q.add("order", {}, job_id="order-1")
    assert (await q.get_job(again.id)).state == "wait"


async def test_a_failure_after_the_sweep_removed_the_job_leaves_no_stub(q, run_worker, run_until):
    """A run that lost its lock and was failed by the stalled sweep, with its hash
    removed by retention, still fails in the end. Its traceback written blindly
    recreated the hash as a stub holding only `stacktrace`."""
    release = asyncio.Event()

    async def proc(job):
        await release.wait()
        raise RuntimeError("late failure")

    job = await q.add("order", {}, job_id="order-2", remove_on_fail=True)
    async with run_worker(q, proc, max_stalled_count=0, stalled_interval=0) as worker:
        assert await run_until(lambda: q.redis.exists(q.keys.lock(job.id)))
        await q.redis.delete(q.keys.lock(job.id))  # the run's lock expired
        await worker.check_stalled(throttle_ms=0)  # mark
        await worker.check_stalled(throttle_ms=0)  # fail it: over max_stalled_count
        assert await q.redis.exists(q.keys.job(job.id)) == 0  # retention removed it
        release.set()
        assert await run_until(lambda: _gone_for(q, job.id, 0.3))

    assert await q.redis.exists(q.keys.job(job.id)) == 0


async def _gone_for(q, job_id: str, seconds: float) -> bool:
    """True once the job's hash has stayed absent for `seconds`."""
    await asyncio.sleep(seconds)  # the late failure lands inside this window
    return await q.redis.exists(q.keys.job(job_id)) == 0
