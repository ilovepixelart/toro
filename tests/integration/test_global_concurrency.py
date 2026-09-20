"""Tests for the global concurrency cap: one limit on jobs active at once,
across every worker on the queue.

Needs a Redis on localhost:6379. Uses an isolated prefix and cleans up after
itself, so it won't touch other data.
"""

import asyncio

import pytest

from toro import Queue, Worker

PREFIX = "torotest"
QUEUE = "globalcap"


async def _clear(queue: Queue) -> None:
    keys = await queue.redis.keys(queue.keys.base + "*")
    if keys:
        await queue.redis.delete(*keys)


@pytest.fixture
async def q():
    queue = Queue(QUEUE, prefix=PREFIX)
    await _clear(queue)
    yield queue
    await _clear(queue)
    await queue.close()


async def _noop(job):
    return None


class _HighWater:
    """In-process gauge of how many processors run at once, and the peak."""

    def __init__(self) -> None:
        self.now = 0
        self.peak = 0

    def enter(self) -> None:
        self.now += 1
        self.peak = max(self.peak, self.now)

    def leave(self) -> None:
        self.now -= 1


async def _run_until_drained(
    q: Queue, workers: list[Worker], total: int, timeout: float = 15.0
) -> int:
    """Run the workers until `total` jobs completed; returns the peak LLEN of
    `active` sampled along the way (the list the cap is enforced on)."""
    tasks = [asyncio.create_task(w.run()) for w in workers]
    peak_active = 0
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        peak_active = max(peak_active, await q.redis.llen(q.keys.active))
        if (await q.counts())["completed"] == total:
            break
        await asyncio.sleep(0.005)
    for w in workers:
        await w.stop()
    for t in tasks:
        t.cancel()
    return peak_active


async def test_cap_holds_across_workers(q):
    """3 workers x concurrency 4 could run 12 at once; the cap holds them to 2.
    Flaky jobs fail once and retry, so the claim after a failure is covered as
    well as the claim after a completion and the initial claim."""
    total = 24
    for i in range(total):
        await q.add("job", {"flaky": i % 3 == 0}, attempts=2)
    gauge = _HighWater()

    async def proc(job):
        gauge.enter()
        try:
            await asyncio.sleep(0.03)
            if job.data["flaky"] and job.attempts_made == 1:
                raise RuntimeError("first attempt fails")
        finally:
            gauge.leave()

    workers = [
        Worker(QUEUE, proc, prefix=PREFIX, concurrency=4, global_concurrency=2, stalled_interval=0)
        for _ in range(3)
    ]
    peak_active = await _run_until_drained(q, workers, total)

    assert (await q.counts())["completed"] == total
    assert gauge.peak == 2  # reached the cap, never passed it
    assert peak_active <= 2


async def test_capped_claim_touches_nothing(q):
    """At the cap a claim is a no-op: the next job keeps its score, no attempt is
    consumed, no rate-limit token is spent. Holds for a second worker too."""
    for i in range(3):
        await q.add("job", {"i": i})
    limit = {"max": 5, "duration": 60_000}
    w1 = Worker(QUEUE, _noop, prefix=PREFIX, global_concurrency=1, rate_limit=limit)
    w2 = Worker(QUEUE, _noop, prefix=PREFIX, global_concurrency=1, rate_limit=limit)

    assert await w1._acquire() is not None  # takes the only slot
    waiting = await q.redis.zrange(q.keys.prioritized, 0, -1, withscores=True)
    tokens = await q.redis.hgetall(q.keys.limiter)

    assert await w1._acquire() is None
    assert await w2._acquire() is None

    assert await q.redis.zrange(q.keys.prioritized, 0, -1, withscores=True) == waiting
    assert await q.redis.hgetall(q.keys.limiter) == tokens
    assert await q.redis.llen(q.keys.active) == 1
    for jid, _score in waiting:
        assert await q.redis.hget(q.keys.job(jid), "attemptsMade") in (None, "0")
    await w1.redis.aclose()
    await w2.redis.aclose()


async def test_unset_cap_is_unbounded(q):
    """No cap (the default): workers run up to their summed concurrency."""
    total = 12
    for i in range(total):
        await q.add("job", {"i": i})
    gauge = _HighWater()

    async def proc(job):
        gauge.enter()
        try:
            await asyncio.sleep(0.05)
        finally:
            gauge.leave()

    workers = [
        Worker(QUEUE, proc, prefix=PREFIX, concurrency=3, stalled_interval=0) for _ in range(2)
    ]
    await _run_until_drained(q, workers, total)

    assert (await q.counts())["completed"] == total
    assert gauge.peak >= 4  # well past any small cap; 6 when every loop is busy
