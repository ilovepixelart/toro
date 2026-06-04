"""Tests for the reliability core: locks, token guard, stalled recovery.

Needs a Redis on localhost:6379. Uses an isolated prefix and cleans up after
itself, so it won't touch other data.
"""
import asyncio
import time

import pytest

from toro import Queue, Worker

PREFIX = "torotest"
QUEUE = "reliability"


def _now_ms() -> int:
    return int(time.time() * 1000)


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


async def test_mark_and_sweep_recovers_then_fails(q):
    """Two passes recover a dead job; exceeding maxStalledCount fails it."""
    job = await q.add("x", {"n": 1}, attempts=5)
    jid = job.id
    w = Worker(QUEUE, _noop, prefix=PREFIX, max_stalled_count=1, connection=q.redis)

    # Simulate a worker that grabbed the job and died: on `active`, no lock.
    await q.redis.lrem(q.keys.wait, 0, jid)
    await q.redis.rpush(q.keys.active, jid)

    # Pass 1 only marks — a job stalled for less than one interval is not touched.
    failed, recovered = await w.check_stalled(throttle_ms=0)
    assert (failed, recovered) == ([], [])
    assert await q.redis.sismember(q.keys.stalled, jid)

    # Pass 2: still no lock -> recovered back to wait, counter = 1.
    failed, recovered = await w.check_stalled(throttle_ms=0)
    assert recovered == [jid] and failed == []
    assert jid in await q.redis.lrange(q.keys.wait, 0, -1)
    assert await q.redis.hget(q.keys.job(jid), "stalledCounter") == "1"

    # It dies again -> next recovery would make counter 2 > maxStalledCount 1.
    await q.redis.lrem(q.keys.wait, 0, jid)
    await q.redis.rpush(q.keys.active, jid)
    await w.check_stalled(throttle_ms=0)  # mark
    failed, recovered = await w.check_stalled(throttle_ms=0)  # escalate
    assert failed == [jid] and recovered == []
    assert await q.redis.zscore(q.keys.failed, jid) is not None
    assert await q.redis.hget(q.keys.job(jid), "state") == "failed"


async def test_live_lock_is_not_recovered(q):
    """A job whose lock is alive must survive sweeps untouched."""
    job = await q.add("x", {})
    jid = job.id
    w = Worker(QUEUE, _noop, prefix=PREFIX, connection=q.redis)
    await q.redis.lrem(q.keys.wait, 0, jid)
    await q.redis.rpush(q.keys.active, jid)
    await w._lock_job(
        keys=[q.keys.job(jid), q.keys.lock(jid), q.keys.stalled],
        args=[w.token, 30000, _now_ms(), jid],
    )
    await w.check_stalled(throttle_ms=0)  # mark
    failed, recovered = await w.check_stalled(throttle_ms=0)  # lock alive -> skip
    assert (failed, recovered) == ([], [])
    assert jid in await q.redis.lrange(q.keys.active, 0, -1)


async def test_lost_lock_cannot_commit(q):
    """A worker that lost its lock can neither complete nor fail the job."""
    job = await q.add("x", {})
    jid = job.id
    w = Worker(QUEUE, _noop, prefix=PREFIX, connection=q.redis)
    await q.redis.lrem(q.keys.wait, 0, jid)
    await q.redis.rpush(q.keys.active, jid)
    await w._lock_job(
        keys=[q.keys.job(jid), q.keys.lock(jid), q.keys.stalled],
        args=[w.token, 30000, _now_ms(), jid],
    )
    # Someone else steals the lock.
    await q.redis.set(q.keys.lock(jid), "another-worker-token")

    res = await w._move_to_completed(
        keys=[q.keys.active, q.keys.completed, q.keys.job(jid), q.keys.lock(jid)],
        args=[jid, "{}", _now_ms(), w.token],
    )
    assert res == -2  # lock lost
    assert await q.redis.zcard(q.keys.completed) == 0
    assert jid in await q.redis.lrange(q.keys.active, 0, -1)  # nothing committed


async def test_zombie_worker_recovered_and_completed_once(q):
    """End to end: a hung worker's job is recovered and finished by another,
    and the zombie's late finish is rejected — exactly one completion."""
    await q.add("job", {"v": 1})
    seen: list[str] = []

    async def slow(job):
        await asyncio.sleep(3)  # zombie hangs past its lock duration
        seen.append("A")
        return {"by": "A"}

    async def fast(job):
        seen.append("B")
        return {"by": "B"}

    zombie = Worker(
        QUEUE, slow, prefix=PREFIX,
        lock_duration=300, renew_locks=False, stalled_interval=0,
    )
    healthy = Worker(
        QUEUE, fast, prefix=PREFIX,
        lock_duration=30000, stalled_interval=300, max_stalled_count=5,
    )
    completed: list = []
    lost: list = []
    healthy.on("completed", lambda j, r: completed.append(j.id))
    zombie.on("lock-lost", lambda jid: lost.append(jid))

    zt = asyncio.create_task(zombie.run())
    await asyncio.sleep(0.4)          # zombie grabs the job, then "hangs"
    ht = asyncio.create_task(healthy.run())
    await asyncio.sleep(2.0)          # healthy recovers + completes it

    assert seen.count("B") == 1
    assert completed == [next(iter(completed), None)] and len(completed) == 1
    assert await q.redis.zcard(q.keys.completed) == 1

    await asyncio.sleep(1.5)          # zombie wakes (~3.4s), tries to commit
    assert lost                       # its finish was rejected by the token guard
    assert await q.redis.zcard(q.keys.completed) == 1  # still exactly one

    await zombie.stop()
    await healthy.stop()
    zt.cancel()
    ht.cancel()
