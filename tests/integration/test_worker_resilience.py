"""A worker must not lose concurrency slots to anything but cancellation:
not a raising user event callback, not a corrupt job hash, not a transient
Redis error between the blocking pop and the finish.
"""

import asyncio
import logging
import time

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError


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
    # for one short beat instead of the default 5s - keeps the test fast AND
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


async def test_idle_repoll_survives_a_read_timeout_shorter_than_the_pop(q, run_worker, run_until):
    """The blocking pop must come back before the connection's read timeout, or it
    raises instead of timing out quietly and the loop never reaches the claim: a
    job whose wake was missed is then stranded for good. A caller-provided
    connection can carry any read timeout, so the worker has to stay under it."""
    await q.add("stranded", {})
    await q.redis.delete(q.keys.marker)  # the missed wake: work waiting, no marker
    done = []

    async def proc(job):
        done.append(job.id)

    conn = aioredis.from_url("redis://localhost:6379", socket_timeout=0.4, decode_responses=True)
    async with run_worker(q, proc, connection=conn, block_timeout=1.0, stalled_interval=0):
        assert await run_until(lambda: done, timeout=3.0), "the idle re-poll never claimed it"


async def test_a_finish_is_re_sent_through_a_blip(q, run_worker, caplog):
    """A finish that failed on a connection error was dropped, and the job re-ran
    after the sweep. A finish is idempotent (the token and claim guards refuse a
    replay of one that ran), so it is re-sent while the lease holds: a blip becomes a
    late commit. The retries never fetch the next job."""
    seen_fetch: list[str] = []
    blips = 2

    async with run_worker(q, lambda job: "done", stalled_interval=0) as w:
        real = w._move_to_completed

        async def flaky(*, keys, args):
            nonlocal blips
            seen_fetch.append(args[4])  # ARGV[5]: fetch the next job
            if blips:
                blips -= 1
                raise RedisConnectionError("blip")
            return await real(keys=keys, args=args)

        w._move_to_completed = flaky
        with caplog.at_level(logging.INFO, logger="toro.worker"):
            job = await q.add("once", {})
            assert await q.result(job.id, timeout=10) == "done"

    assert seen_fetch == ["1", "0", "0"]
    assert (await q.get_job(job.id)).attempts_made == 1  # committed once, never re-run
    warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "a finish" in warnings[0]
    assert [r.message for r in caplog.records if r.levelno == logging.INFO] == [
        "a finish recovered"
    ]


async def test_a_redis_outage_logs_once_and_backs_off(q, run_worker, run_until, caplog):
    """With Redis down every slot logged a full traceback ten times a second. One
    warning opens the outage, the pause doubles up to the block timeout, and one
    line closes it."""
    calls = 0
    down = True

    async with run_worker(
        q, lambda job: None, concurrency=3, stalled_interval=0, block_timeout=1.0
    ) as w:
        real = w.redis.bzpopmin

        async def flaky(*args, **kwargs):
            nonlocal calls
            calls += 1
            if down:
                raise RedisConnectionError("down")
            return await real(*args, **kwargs)

        with caplog.at_level(logging.INFO, logger="toro.worker"):
            w.redis.bzpopmin = flaky
            await asyncio.sleep(2.0)
            during = calls
            down = False
            assert await run_until(lambda: "a claim recovered" in caplog.text, timeout=5)

    assert during <= 3 * 8, during  # three slots backing off, not thirty attempts a second
    warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "a claim" in warnings[0]
