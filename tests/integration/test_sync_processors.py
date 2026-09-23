"""Integration: a plain `def` processor (docs/specs/sync-and-scale.md).

People arriving from the sync queues bring sync handlers. Awaited, one raises a
TypeError after it has already run inside the event loop, which is the worst of both
answers: it ran, it blocked, and the job failed. It runs in a thread instead.
"""

import asyncio
import itertools
import threading
import time

import pytest

from toro import Worker

PREFIX = "torotest"


def _loop_gaps(ticks: list[float]) -> float:
    """The longest the event loop went without running our ticker."""
    return max((b - a for a, b in itertools.pairwise(ticks)), default=0.0)


async def _ticker(ticks: list[float]) -> None:
    while True:
        ticks.append(time.monotonic())
        await asyncio.sleep(0.02)


async def test_a_sync_processor_does_not_block_the_loop(q, run_worker):
    """SS-001: the return value is the job's result, and the loop keeps running
    throughout, which is what lock renewal and the heartbeat need."""
    ticks: list[float] = []

    def proc(job):
        time.sleep(0.4)
        return {"slept": job.name}

    ticking = asyncio.create_task(_ticker(ticks))
    try:
        async with run_worker(q, proc):
            job = await q.add("slow", {})
            assert await q.result(job.id, timeout=15) == {"slept": "slow"}
    finally:
        ticking.cancel()

    assert _loop_gaps(ticks) < 0.15, f"the loop stalled for {_loop_gaps(ticks):.2f}s"


async def test_a_sync_processor_is_a_processor(q, run_worker, run_until):
    """SS-002: everything else about a job is the same. A sync processor that raises
    is a failure like any other, and retries like any other."""
    attempts: list[int] = []

    def proc(job):
        attempts.append(job.attempts_made)
        raise RuntimeError("boom")

    async with run_worker(q, proc) as w:
        w.on("failed", lambda *a, **k: None)
        job = await q.add("breaks", {}, attempts=2, backoff=0)
        assert await run_until(lambda: len(attempts) >= 2, timeout=15)
        assert await run_until(_failed(q, 1), timeout=15)

    assert (await q.counts())["failed"] == 1
    failed = await q.get_job(job.id)
    # the processor's own error, not "object dict can't be used in 'await' expression"
    assert "boom" in (failed.failed_reason or "")
    assert "await" not in (failed.failed_reason or "")


async def test_sync_jobs_run_side_by_side(q, run_worker, run_until):
    """SS-003: the pool is sized by `concurrency`, so two sync jobs on a worker with
    two slots overlap rather than queue behind one thread."""
    running: list[str] = []
    peak = 0

    def proc(job):
        nonlocal peak
        running.append(job.id)
        peak = max(peak, len(running))
        time.sleep(0.2)
        running.remove(job.id)
        return 1

    async with run_worker(q, proc, concurrency=2):
        await q.add("a", {})
        await q.add("b", {})
        assert await run_until(_completed(q, 2), timeout=15)

    assert peak == 2, f"the two jobs did not overlap (peak {peak})"


async def test_an_async_worker_starts_no_threads(q, run_worker, run_until):
    """SS-003: the pool is created on the first sync job, so an all-async worker
    pays nothing for a feature it does not use."""

    async def proc(job):
        return 1

    async with run_worker(q, proc) as w:
        await q.add("j", {})
        assert await run_until(_completed(q, 1), timeout=15)
        assert w._executor is None


def _pool_threads(worker) -> list[str]:
    """The worker's own threads, by name. Counting every thread in the process would
    also count the ones asyncio starts for itself (DNS lookups), which come and go
    for reasons that have nothing to do with this."""
    return [t.name for t in threading.enumerate() if t.name.startswith(f"toro-{worker.name}")]


async def test_the_pool_is_given_back_when_the_worker_stops(q, run_worker, run_until):
    """SS-003: a worker that stops leaves no threads behind, however many sync jobs
    it ran."""

    def proc(job):
        time.sleep(0.05)
        return 1

    async with run_worker(q, proc, concurrency=4) as w:
        for _ in range(4):
            await q.add("j", {})
        assert await run_until(_completed(q, 4), timeout=15)
        assert _pool_threads(w), "the sync jobs ran on no thread of the worker's own"

    for _ in range(100):  # asked to stop on shutdown, then wound down by the runtime
        if not _pool_threads(w):
            break
        await asyncio.sleep(0.05)
    assert _pool_threads(w) == []


@pytest.mark.parametrize("attempts", [1])
async def test_a_processor_whose_kind_was_misread_still_returns_its_value(q, run_worker, attempts):
    """SS-007: a decorator that hides a coroutine function reads as sync, and the
    thread hands back an un-started coroutine. Awaited anyway, the job gets its real
    result instead of a coroutine object that fails to serialize."""

    async def real(job):
        return {"ok": job.name}

    class Hidden:  # not a coroutine function by inspection, returns a coroutine
        def __call__(self, job):
            return real(job)

    async with run_worker(q, Hidden()):
        job = await q.add("hidden", {}, attempts=attempts)
        assert await q.result(job.id, timeout=15) == {"ok": "hidden"}


async def test_cancelling_a_sync_job_ends_it_when_its_thread_does(q, run_worker, run_until):
    """SS-004: a thread is not a task. The request lands at once and the outcome waits
    for the work, because Python cannot interrupt a thread. The job still ends
    `cancelled` rather than `completed`, whatever its processor returned: the worker
    decides the outcome by what it asked for."""
    started = threading.Event()
    finished = threading.Event()

    def proc(job):
        started.set()
        time.sleep(0.6)
        finished.set()
        return 1

    async with run_worker(q, proc):
        job = await q.add("long", {})
        assert await asyncio.to_thread(started.wait, 15)

        assert await q.cancel_job(job.id) is True

        # recorded at once, and still running: the state is what is true
        assert await q.redis.hget(q.keys.job(job.id), "cancel") == "1"
        assert not finished.is_set()
        assert (await q.get_job(job.id)).state == "active"

        assert await asyncio.to_thread(finished.wait, 15)  # it runs to its own end
        assert await run_until(_cancelled(q, 1), timeout=15)


async def test_a_cancelled_sync_job_keeps_the_slot_its_thread_still_holds(q, run_worker, run_until):
    """SS-004: a slot and a thread are the same capacity counted twice, so freeing one
    while the other is still busy hands the next job to a pool with no thread for it:
    claimed, locked, renewed, and not running. At `concurrency` cancellations the
    worker stops running anything at all and nothing says so."""
    started = threading.Event()
    done: list[str] = []

    def proc(job):
        if job.name == "long":
            started.set()
            time.sleep(1.0)
        done.append(job.name)
        return 1

    async with run_worker(q, proc, concurrency=1):
        long_job = await q.add("long", {})
        assert await asyncio.to_thread(started.wait, 15)
        assert await q.cancel_job(long_job.id) is True

        after = await q.add("after", {})
        await asyncio.sleep(0.3)  # ample time for a free slot to claim it
        state = (await q.get_job(after.id)).state
        assert state == "wait", f"claimed with no thread to run it: {state}"

        assert await run_until(lambda: "after" in done, timeout=20)


async def test_cancelling_a_sync_job_does_not_hand_its_key_on_early(q, run_worker, run_until):
    """SS-004, CN-006: `concurrency_key` means one at a time, and a terminal state
    hands the key to the next job. A thread that is still running has not finished
    with the key, whatever the job's state says."""
    lock = threading.Lock()
    running = 0
    peak = 0
    started = threading.Event()

    def proc(job):
        nonlocal running, peak
        with lock:
            running += 1
            peak = max(peak, running)
        started.set()
        time.sleep(0.8 if job.name == "first" else 0.05)
        with lock:
            running -= 1
        return 1

    async with run_worker(q, proc, concurrency=2):
        first = await q.add("first", {}, concurrency_key="acct-7")
        assert await asyncio.to_thread(started.wait, 15)
        await q.add("second", {}, concurrency_key="acct-7")
        assert await q.cancel_job(first.id) is True
        assert await run_until(_completed(q, 1), timeout=20)

    assert peak == 1, f"two jobs ran at once on one key (peak {peak})"


def _cancelled(q, n: int):
    async def check() -> bool:
        return (await q.counts())["cancelled"] >= n

    return check


def _completed(q, n: int):
    async def check() -> bool:
        return (await q.counts())["completed"] >= n

    return check


def _failed(q, n: int):
    async def check() -> bool:
        return (await q.counts())["failed"] >= n

    return check


async def test_a_sync_worker_reports_its_concurrency(q):
    """The pool is the worker's own, sized by `concurrency`: the loop's default
    executor is shared process-wide and sized for something else entirely, so a job
    waiting for one of its threads would wait holding a lock."""
    w = Worker(q.name, lambda job: 1, prefix=PREFIX, concurrency=7, connection=q.redis)
    try:
        assert w._pool()._max_workers == 7
    finally:
        w._pool().shutdown(wait=False)
