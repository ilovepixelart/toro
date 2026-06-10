"""A worker must not lose concurrency slots to anything but cancellation:
not a raising user event callback, not a corrupt job hash, not a transient
Redis error between the blocking pop and the finish.
"""

import asyncio
import time


async def test_raising_event_callback_does_not_kill_the_slot(q, run_worker, run_until):
    done = []

    async def proc(job):
        done.append(job.id)

    async with run_worker(q, proc, concurrency=1, stalled_interval=0) as w:
        w.on("completed", lambda job, res: 1 / 0)  # a user callback that always raises
        await q.add("a", {})
        await q.add("b", {})
        # The first job's emit raises; with one slot, the second job only runs
        # if the loop survived the callback.
        assert await run_until(lambda: len(done) >= 2, timeout=10.0), f"slot died: {done}"
    assert (await q.counts())["completed"] == 2


async def test_transient_error_in_the_loop_does_not_kill_the_slot(q, run_worker, run_until):
    done = []

    async def proc(job):
        done.append(job.id)

    # block_timeout=1: after the blip eats the wakeup marker, the loop re-parks
    # for one short beat instead of the default 5s — keeps the test fast AND
    # proves recovery doesn't depend on a fresh marker arriving.
    async with run_worker(q, proc, concurrency=1, stalled_interval=0, block_timeout=1.0) as w:
        original = w._acquire
        calls = {"n": 0}

        async def flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionError("redis blip")  # first claim attempt fails
            return await original()

        w._acquire = flaky
        await q.add("a", {})
        assert await run_until(lambda: len(done) >= 1, timeout=10.0), "slot died on a blip"
        assert calls["n"] >= 2  # the loop really came back for another claim


async def test_corrupt_job_hash_does_not_kill_the_slot(q, run_worker, run_until):
    # A hash with invalid JSON in `data` makes Job.from_hash raise after the
    # claim. The slot must survive; the job is recovered by the stalled sweep
    # and bounded by max_stalled_count (at-least-once machinery, not a crash).
    now = int(time.time() * 1000)
    await q.redis.hset(
        q.keys.job("corrupt1"),
        mapping={
            "id": "corrupt1",
            "name": "bad",
            "data": "{not json",
            "opts": "{}",
            "timestamp": now,
            "attemptsMade": 0,
            "priority": 0,
            "state": "wait",
        },
    )
    pc = await q.redis.incr(q.keys.pc)
    await q.redis.zadd(q.keys.prioritized, {"corrupt1": (2**20 - 0) * (2**32) + pc})
    await q.redis.zadd(q.keys.marker, {"0": 0})

    done = []

    async def proc(job):
        done.append(job.name)

    async with run_worker(q, proc, concurrency=1, stalled_interval=0):
        await asyncio.sleep(0)  # let the loop claim the poisoned job first
        await q.add("good", {})
        assert await run_until(lambda: len(done) >= 1, timeout=10.0), "slot died on corrupt data"
        assert done == ["good"]
