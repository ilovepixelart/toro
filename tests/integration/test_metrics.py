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
    assert all(
        p["added"] == 0 and p["completed"] == 0 and p["failed"] == 0 and p["ms"] == 0
        for p in points
    )


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


async def test_added_jobs_count_in_current_minute(q):
    for _ in range(3):
        await q.add("m", {})
    points = await q.metrics(minutes=2)
    assert sum(p["added"] for p in points) == 3
    assert sum(p["completed"] for p in points) == 0  # added, not yet processed


async def test_dedup_suppressed_adds_do_not_count(q):
    await q.add("m", {}, deduplication={"id": "once", "ttl": 60_000})
    await q.add("m", {}, deduplication={"id": "once", "ttl": 60_000})  # suppressed
    await q.add("m", {}, job_id="fixed-id")
    await q.add("m", {}, job_id="fixed-id")  # idempotent replay, not a new job
    points = await q.metrics(minutes=2)
    assert sum(p["added"] for p in points) == 2  # one per real insert


async def test_per_name_counts(q, run_worker, run_until):
    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        return "ok"

    async with run_worker(q, proc):
        await q.add("good", {})
        await q.add("good", {})
        await q.add("bad", {}, attempts=1)
        assert await run_until(lambda: _count(q, "completed", 2))
        assert await run_until(lambda: _count(q, "failed", 1))

    names = await q.metrics_by_name(minutes=2)
    by = {n["name"]: n for n in names}
    assert by["good"]["completed"] == 2
    assert by["good"]["failed"] == 0
    assert by["bad"]["failed"] == 1
    assert by["bad"]["completed"] == 0
    assert by["good"]["ms"] >= 0


async def test_metrics_by_name_sorts_failures_first(q, run_worker, run_until):
    async def proc(job):
        if job.name == "flaky":
            raise RuntimeError("boom")
        return "ok"

    async with run_worker(q, proc):
        for _ in range(5):
            await q.add("busy", {})
        await q.add("flaky", {}, attempts=1)
        assert await run_until(lambda: _count(q, "completed", 5))
        assert await run_until(lambda: _count(q, "failed", 1))

    names = await q.metrics_by_name(minutes=2)
    assert names[0]["name"] == "flaky"  # failure-first: triage order, not volume


async def test_duration_recorded_even_when_hash_is_removed_on_complete(q, run_worker, run_until):
    # recordMetrics must read processedOn BEFORE recordFinished deletes the
    # hash, or remove_on_complete=True silently zeroes every duration
    async def slow(job):
        await asyncio.sleep(0.05)

    async with run_worker(q, slow):
        j = await q.add("m", {}, remove_on_complete=True)
        assert await run_until(lambda: _bucket_total(q, "completed", 1))

    assert await q.get_job(j.id) is None  # hash really gone
    points = await q.metrics(minutes=2)
    assert sum(p["ms"] for p in points) >= 40  # duration still captured


async def test_added_counts_delayed_jobs(q):
    await q.add("m", {}, delay=60_000)  # parks in delayed, not wait
    points = await q.metrics(minutes=2)
    assert sum(p["added"] for p in points) == 1


async def test_per_name_survives_colons_in_the_name(q, run_worker, run_until):
    async with run_worker(q, _noop):
        await q.add("user:sync", {})
        assert await run_until(lambda: _count(q, "completed", 1))

    names = await q.metrics_by_name(minutes=2)
    assert names[0]["name"] == "user:sync"  # split on FIRST colon only
    assert names[0]["completed"] == 1


async def test_stall_failure_records_the_job_name(q):
    job = await q.add("stally", {})
    w = Worker(QUEUE, _noop, prefix=PREFIX, max_stalled_count=0, connection=q.redis)
    await q.redis.zrem(q.keys.prioritized, job.id)
    await q.redis.rpush(q.keys.active, job.id)
    await w.check_stalled(throttle_ms=0)
    failed, _ = await w.check_stalled(throttle_ms=0)
    assert failed == [job.id]

    names = await q.metrics_by_name(minutes=2)
    by = {n["name"]: n for n in names}
    assert by["stally"]["failed"] == 1


async def _bucket_total(q: Queue, field: str, want: int) -> bool:
    points = await q.metrics(minutes=2)
    return sum(p[field] for p in points) >= want
