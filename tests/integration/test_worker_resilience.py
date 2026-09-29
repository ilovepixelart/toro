"""A worker must not lose concurrency slots to anything but cancellation:
not a raising user event callback, not a corrupt job hash, not a transient
Redis error between the blocking pop and the finish.
"""

import asyncio
import hashlib
import logging
import time

import pytest
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError

from toro import scripts
from toro.connection import connect


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


async def _count(q, state: str, want: int) -> bool:
    return (await q.counts())[state] >= want


@pytest.mark.parametrize("succeed", [True, False], ids=["completed", "failed"])
async def test_a_finish_whose_reply_was_lost_hands_over_the_job_it_fetched(
    q, run_worker, run_until, swallow_first_reply, succeed
):
    """The first send committed the job and fetched the next one, then its reply was
    lost. The re-send was refused (the lock is gone), the worker reported the finish
    lost, and the fetched job sat locked in `active` with nobody running it until the
    sweep. A finish records what it answered, and the re-send is answered the same."""
    script = scripts.MOVE_TO_COMPLETED if succeed else scripts.MOVE_TO_FAILED
    sha = hashlib.sha1(script.encode()).hexdigest()  # noqa: S324 - Redis names scripts by SHA1
    await q.redis.script_load(script)  # cached: the swallowed reply must be the finish's own
    proxy = await swallow_first_reply(sha)
    port = proxy.sockets[0].getsockname()[1]
    conn = aioredis.from_url(f"redis://localhost:{port}", socket_timeout=0.5, decode_responses=True)
    ran: list[str] = []
    lost: list[str] = []

    async def proc(job):
        ran.append(job.id)
        if not succeed:
            raise RuntimeError("boom")

    await q.add("a", {}, attempts=1)
    await q.add("b", {}, attempts=1)
    try:
        async with run_worker(q, proc, connection=conn, block_timeout=0.2, stalled_interval=0) as w:
            w.on("lock-lost", lost.append)
            assert await run_until(lambda: len(ran) == 2, timeout=5), f"ran {ran}"
            state = "completed" if succeed else "failed"
            assert await run_until(lambda: _count(q, state, 2), timeout=5)
    finally:
        await conn.aclose()
        proxy.close()
    assert lost == []


@pytest.mark.parametrize("succeed", [True, False], ids=["completed", "failed"])
async def test_a_finish_re_sent_by_the_client_is_answered_from_the_memo(
    q, run_worker, run_until, drop_link_after_first, succeed
):
    """The finish ran, the connection dropped with its reply on the way, and the client
    re-sent the same call on a new connection by itself: the worker's own retry never
    saw an error. The re-send was refused (the lock is gone), the finish was reported
    lost, and the job the first send fetched sat locked in `active` with nobody
    running it. The memo answers a re-send whoever sent it."""
    script = scripts.MOVE_TO_COMPLETED if succeed else scripts.MOVE_TO_FAILED
    sha = hashlib.sha1(script.encode()).hexdigest()  # noqa: S324 - Redis names scripts by SHA1
    await q.redis.script_load(script)  # cached: the cut link must be the finish's own
    proxy = await drop_link_after_first(sha)
    port = proxy.sockets[0].getsockname()[1]
    conn = connect(f"redis://localhost:{port}", blocking_timeout=0.2)
    ran: list[str] = []
    lost: list[str] = []

    async def proc(job):
        ran.append(job.id)
        if not succeed:
            raise RuntimeError("boom")

    await q.add("a", {}, attempts=1)
    await q.add("b", {}, attempts=1)
    try:
        async with run_worker(q, proc, connection=conn, block_timeout=0.2, stalled_interval=0) as w:
            w.on("lock-lost", lost.append)
            assert await run_until(lambda: len(ran) == 2, timeout=5), f"ran {ran}"
            state = "completed" if succeed else "failed"
            assert await run_until(lambda: _count(q, state, 2), timeout=5)
    finally:
        await conn.aclose()
        proxy.close()
    assert lost == []


async def test_a_cancel_whose_reply_was_lost_is_not_reported_lost(
    q, run_worker, run_until, swallow_first_reply
):
    """A cancel commit fetches nothing, so nothing was stranded, but its lost reply
    was reported as a lost lock on a job that had just been cancelled."""
    sha = hashlib.sha1(scripts.MOVE_TO_CANCELLED.encode()).hexdigest()  # noqa: S324
    await q.redis.script_load(scripts.MOVE_TO_CANCELLED)
    proxy = await swallow_first_reply(sha)
    port = proxy.sockets[0].getsockname()[1]
    conn = aioredis.from_url(f"redis://localhost:{port}", socket_timeout=0.5, decode_responses=True)
    started = asyncio.Event()
    lost: list[str] = []
    ended: list[str] = []

    async def proc(job):
        started.set()
        await asyncio.sleep(30)

    job = await q.add("long", {})
    try:
        async with run_worker(q, proc, connection=conn, block_timeout=0.2, stalled_interval=0) as w:
            w.on("lock-lost", lost.append)
            w.on("cancelled", lambda job: ended.append(job.id))
            await asyncio.wait_for(started.wait(), 5)
            await q.cancel_job(job.id)
            assert await run_until(lambda: ended, timeout=5)
    finally:
        await conn.aclose()
        proxy.close()
    assert lost == []
    assert (await q.get_job(job.id)).state == "cancelled"
