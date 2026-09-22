"""Resource hygiene: a job (or a whole flow) that runs its course must leave
NOTHING behind - no hash, lock, logs, or flow aux keys, and no stray entry in the
children index or the roots scratch - and a worker that stops must leak no
background tasks. The fuzzer checks orphans mid-run; this pins the end state.
"""

import asyncio
import contextlib

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
}  # fmt: skip


async def _noop(job):
    return None


async def _leaked_keys(q: Queue) -> list[str]:
    """Per-job/aux keys left under the queue's namespace (infra keys excluded)."""
    base = q.keys.base
    leaked = []
    for key in await q.redis.keys(base + "*"):
        suffix = key[len(base) :]
        if suffix in _INFRA or suffix.split(":")[0] in ("metrics", "worker", "repeat"):
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


# ---- a stopped worker leaks no background tasks ------------------------------------


async def test_stopped_worker_leaks_no_tasks(q, run_until):
    done = []

    async def proc(job):
        done.append(job.id)

    worker = Worker(q.name, proc, prefix=PREFIX, connection=q.redis, heartbeat_interval=50)
    task = asyncio.create_task(worker.run())
    await q.add("j", {})
    assert await run_until(lambda: len(done) >= 1, timeout=10)
    await worker.stop()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # every background loop (stalled / heartbeat / promote) is done, none lingering
    assert all(t.done() for t in worker._tasks), [t for t in worker._tasks if not t.done()]
    await worker.stop()  # idempotent: a second stop must not raise


async def _connected(watcher: Queue) -> int:
    return int((await watcher.redis.info("clients"))["connected_clients"])


async def test_closing_a_queue_gives_its_sockets_back(q):
    """`close()` has to release the connections it opened, not just the one the client
    holds: the rest stay open until the garbage collector reaches them, which on a
    closed event loop is a traceback at exit."""
    watcher = Queue(q.name, prefix=PREFIX)
    try:
        base = await _connected(watcher)
        other = Queue(q.name, prefix=PREFIX)
        await asyncio.gather(*(other.counts() for _ in range(5)))  # open a few
        assert await _connected(watcher) > base

        await other.close()

        assert await _connected(watcher) == base
    finally:
        await watcher.close()


async def test_stopping_a_worker_gives_its_sockets_back(q):
    """The same for a worker, which parks one connection per process loop."""
    watcher = Queue(q.name, prefix=PREFIX)
    try:
        base = await _connected(watcher)
        w = Worker(q.name, _noop, prefix=PREFIX, concurrency=4, stalled_interval=0)
        task = asyncio.create_task(w.run())
        await asyncio.sleep(0.3)
        assert await _connected(watcher) > base

        await w.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert await _connected(watcher) == base
    finally:
        await watcher.close()


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
