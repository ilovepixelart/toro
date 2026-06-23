"""Fault injection / chaos: drive toro through faults the happy-path tests never
see and assert the at-least-once machinery still holds - a dropped Redis call
mid-commit, and two workers racing the same recovery or promotion.

These target the gaps left by test_reliability/test_worker_resilience: those cover
a worker that *dies*; here the worker *survives* a transient failure mid-finish
(the lock renewer is cancelled, the lock lapses, the stalled sweep recovers the
job) and two workers hit the same atomic guard at once.
"""

import asyncio

from toro import Queue, Worker

PREFIX = "torotest"


async def _noop(job):
    return None


def _completed(q: Queue, n: int = 1):
    async def check():
        return (await q.counts())["completed"] >= n

    return check


def _failed(q: Queue, n: int = 1):
    async def check():
        return (await q.counts())["failed"] >= n

    return check


# ---- a dropped Redis call mid-commit recovers, exactly one terminal state ----------


async def test_dropped_commit_recovers_and_completes_once(q, run_worker, run_until):
    runs: list[str] = []

    async def proc(job):
        runs.append(job.id)

    # short lock so it lapses fast once the renewer is cancelled; sweep is driven
    # by hand for determinism (stalled_interval=0 disables the worker's own loop).
    async with run_worker(
        q, proc, concurrency=1, stalled_interval=0, lock_duration=150, block_timeout=0.2
    ) as w:
        orig = w._finish_completed
        hits = {"n": 0}

        async def flaky(job, result):
            hits["n"] += 1
            if hits["n"] == 1:
                raise ConnectionError("redis dropped mid-commit")  # the first commit never lands
            return await orig(job, result)

        w._finish_completed = flaky

        await q.add("j", {})
        # claim + process happen, then the commit is dropped: the job is stranded
        # on `active`, the renewer was cancelled, so the lock will lapse.
        assert await run_until(lambda: hits["n"] >= 1, timeout=10), "commit never attempted"
        await asyncio.sleep(0.3)  # > lock_duration: the lock is now dead
        await w.check_stalled(throttle_ms=0)  # pass 1: mark
        await w.check_stalled(throttle_ms=0)  # pass 2: recover -> wait + wakeup marker
        # the freed slot re-claims and this time the commit lands
        assert await run_until(_completed(q, 1), timeout=10), "never recovered"

    assert (await q.counts())["completed"] == 1  # exactly one terminal completion
    assert hits["n"] >= 2  # the commit was genuinely retried after recovery
    assert len(runs) >= 1  # at-least-once: the processor may have run twice


async def test_dropped_fail_commit_recovers_and_fails_once(q, run_worker, run_until):
    async def proc(job):
        raise RuntimeError("boom")  # always fails

    async with run_worker(
        q, proc, concurrency=1, stalled_interval=0, lock_duration=150, block_timeout=0.2
    ) as w:
        orig = w._finish_failed
        hits = {"n": 0}

        async def flaky(job, exc):
            hits["n"] += 1
            if hits["n"] == 1:
                raise ConnectionError("redis dropped mid-fail-commit")
            return await orig(job, exc)

        w._finish_failed = flaky

        await q.add("j", {}, attempts=1)
        assert await run_until(lambda: hits["n"] >= 1, timeout=10), "fail-commit never attempted"
        await asyncio.sleep(0.3)
        await w.check_stalled(throttle_ms=0)
        await w.check_stalled(throttle_ms=0)
        assert await run_until(_failed(q, 1), timeout=10), "never recovered to failed"

    assert (await q.counts())["failed"] == 1  # exactly one terminal failure
    assert hits["n"] >= 2


# ---- two workers hit the same atomic guard at once ---------------------------------


async def test_concurrent_stalled_sweeps_recover_exactly_once(q):
    # A dead job swept by two workers at the same instant: the atomic LREM-from-active
    # guard inside MOVE_STALLED means exactly one recovers it - no double re-enqueue,
    # no double counter bump.
    job = await q.add("x", {}, attempts=5)
    jid = job.id
    w1 = Worker(q.name, _noop, prefix=PREFIX, max_stalled_count=3, connection=q.redis)
    w2 = Worker(q.name, _noop, prefix=PREFIX, max_stalled_count=3, connection=q.redis)

    # a worker grabbed it and died: on `active`, no lock
    await q.redis.zrem(q.keys.prioritized, jid)
    await q.redis.rpush(q.keys.active, jid)

    await w1.check_stalled(throttle_ms=0)  # pass 1: mark (single, sequential)
    assert await q.redis.sismember(q.keys.stalled, jid)
    # pass 2: BOTH workers try to recover the now-marked job at the same instant
    results = await asyncio.gather(w1.check_stalled(throttle_ms=0), w2.check_stalled(throttle_ms=0))

    recovered = [r for _failed_ids, rec in results for r in rec]
    assert recovered.count(jid) == 1, results  # exactly one sweep won
    assert await q.redis.zscore(q.keys.prioritized, jid) is not None  # back in the queue once
    assert jid not in await q.redis.lrange(q.keys.active, 0, -1)  # off active
    assert await q.redis.hget(q.keys.job(jid), "stalledCounter") == "1"  # bumped once, not twice


async def test_concurrent_delayed_promotion_promotes_each_once(q):
    # Two workers promote the same due batch at once. Lua runs serially, so the
    # first promotes the whole batch and the second finds an empty range: every
    # due job lands in `prioritized` exactly once, none lost or duplicated.
    from toro import scripts

    now = await _server_now_ms(q)
    ids = [f"d{i}" for i in range(5)]
    for jid in ids:
        await q.redis.hset(
            q.keys.job(jid),
            mapping={"id": jid, "name": jid, "data": "{}", "opts": "{}", "state": "delayed"},
        )
        await q.redis.zadd(q.keys.delayed, {jid: now - 1000})  # already due

    w1 = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    w2 = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    keys = [q.keys.delayed, q.keys.prioritized, q.keys.marker, q.keys.base, q.keys.pc]
    await asyncio.gather(
        w1._promote_delayed(keys=keys, args=[now, scripts.PROMOTE_BATCH]),
        w2._promote_delayed(keys=keys, args=[now, scripts.PROMOTE_BATCH]),
    )

    promoted = sorted(await q.redis.zrange(q.keys.prioritized, 0, -1))
    assert promoted == sorted(ids)  # each due job promoted exactly once, none duplicated
    assert await q.redis.zcard(q.keys.delayed) == 0  # nothing left behind


async def _server_now_ms(q: Queue) -> int:
    secs, micros = await q.redis.time()
    return int(secs) * 1000 + int(micros) // 1000
