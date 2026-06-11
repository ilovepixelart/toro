"""Integration: per-minute metrics counters and queue latency.

Counters live in per-minute hash buckets written atomically inside the finish
scripts: completed count, terminal-failed count, and summed processing
duration. Retries are not failures; stall-failures are.
"""

import asyncio
import itertools
import time

from toro import Queue, Worker
from toro.scripts import METRICS_RETENTION_MS

PREFIX = "torotest"
QUEUE = "torotest"


async def _noop(job):
    return "ok"


def _minute(ms: float) -> int:
    return int(ms // 60_000) * 60_000


async def _count(q: Queue, state: str, want: int) -> bool:
    return (await q.counts())[state] >= want


async def test_completed_jobs_count_in_current_minute(q, run_worker, run_until):
    async with run_worker(q, _noop):
        for _ in range(3):
            await q.add("m", {})
        assert await run_until(lambda: _count(q, "completed", 3))

    points = await q.metrics(minutes=2)
    assert len(points) == 2
    assert points[-1]["timestamp"] == _minute(time.time() * 1000)
    assert sum(p["completed"] for p in points) == 3
    assert all(p["failed"] == 0 for p in points)
    assert all(p["ms"] >= 0 for p in points)


async def test_terminal_failure_counts_once_retries_do_not(q, run_worker, run_until):
    async def boom(job):
        raise RuntimeError("boom")

    async with run_worker(q, boom):
        await q.add("m", {}, attempts=3)  # 2 retries + 1 terminal failure
        assert await run_until(lambda: _count(q, "failed", 1))

    points = await q.metrics(minutes=2)
    assert sum(p["failed"] for p in points) == 1  # retries are not failures
    assert sum(p["completed"] for p in points) == 0


async def test_success_after_retry_is_one_completion_zero_failures(q, run_worker, run_until):
    attempt = 0

    async def flaky(job):
        nonlocal attempt
        attempt += 1
        if attempt == 1:
            raise RuntimeError("transient")
        return "ok"

    async with run_worker(q, flaky):
        await q.add("m", {}, attempts=2)
        assert await run_until(lambda: _count(q, "completed", 1))

    points = await q.metrics(minutes=2)
    assert sum(p["completed"] for p in points) == 1
    assert sum(p["failed"] for p in points) == 0


async def test_duration_ms_is_summed(q, run_worker, run_until):
    async def slow(job):
        await asyncio.sleep(0.05)

    async with run_worker(q, slow):
        await q.add("m", {})
        await q.add("m", {})
        assert await run_until(lambda: _count(q, "completed", 2))

    points = await q.metrics(minutes=2)
    # two ~50ms jobs; generous lower bound for CI timing noise
    assert sum(p["ms"] for p in points) >= 80


async def test_zero_fill_returns_continuous_minutes(q):
    points = await q.metrics(minutes=5)
    assert len(points) == 5
    stamps = [p["timestamp"] for p in points]
    assert stamps == sorted(stamps)
    assert all(b - a == 60_000 for a, b in itertools.pairwise(stamps))
    assert all(p["completed"] == 0 and p["failed"] == 0 and p["ms"] == 0 for p in points)


async def test_buckets_expire(q, run_worker, run_until):
    async with run_worker(q, _noop):
        await q.add("m", {})
        assert await run_until(lambda: _count(q, "completed", 1))

    bucket = q.keys.metrics_bucket(_minute(time.time() * 1000))
    ttl = await q.redis.pttl(bucket)
    assert 0 < ttl <= METRICS_RETENTION_MS


async def test_stall_failure_counts_as_failed(q):
    job = await q.add("m", {})
    jid = job.id
    w = Worker(QUEUE, _noop, prefix=PREFIX, max_stalled_count=0, connection=q.redis)

    # A worker grabbed the job and died: on `active`, no lock.
    await q.redis.zrem(q.keys.prioritized, jid)
    await q.redis.rpush(q.keys.active, jid)

    await w.check_stalled(throttle_ms=0)  # mark
    failed, _ = await w.check_stalled(throttle_ms=0)  # escalate past the limit
    assert failed == [jid]

    points = await q.metrics(minutes=2)
    assert sum(p["failed"] for p in points) == 1


async def test_latency_is_age_of_next_waiting_job(q, run_worker, run_until):
    assert await q.latency() == 0  # nothing waiting

    await q.add("m", {})
    await asyncio.sleep(0.1)
    lat = await q.latency()
    assert lat >= 80  # the waiting job is ~100ms old

    async with run_worker(q, _noop):
        assert await run_until(lambda: _count(q, "completed", 1))
    assert await q.latency() == 0  # drained
