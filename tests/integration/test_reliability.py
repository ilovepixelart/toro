"""Tests for the reliability core: locks, token guard, stalled recovery.

Needs a Redis on localhost:6379. Uses an isolated prefix and cleans up after
itself, so it won't touch other data.
"""

import asyncio
import time

import pytest

from toro import JobFailedError, Queue, Worker

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


async def _claim(q: Queue, jid: str, token: str) -> None:
    """Test-only: move a specific job to `active` and lock it (bypasses ordering)."""
    await q.redis.zrem(q.keys.prioritized, jid)
    await q.redis.rpush(q.keys.active, jid)
    await q.redis.set(q.keys.lock(jid), token)


async def test_mark_and_sweep_recovers_then_fails(q):
    """Two passes recover a dead job; exceeding maxStalledCount fails it."""
    job = await q.add("x", {"n": 1}, attempts=5)
    jid = job.id
    w = Worker(QUEUE, _noop, prefix=PREFIX, max_stalled_count=1, connection=q.redis)

    # Simulate a worker that grabbed the job and died: on `active`, no lock.
    await q.redis.zrem(q.keys.prioritized, jid)
    await q.redis.rpush(q.keys.active, jid)

    # Pass 1 only marks - a job stalled for less than one interval is not touched.
    failed, recovered = await w.check_stalled(throttle_ms=0)
    assert (failed, recovered) == ([], [])
    assert await q.redis.sismember(q.keys.stalled, jid)

    # Pass 2: still no lock -> recovered back to wait, counter = 1.
    failed, recovered = await w.check_stalled(throttle_ms=0)
    assert recovered == [jid] and failed == []
    assert await q.redis.zscore(q.keys.prioritized, jid) is not None
    assert await q.redis.hget(q.keys.job(jid), "stalledCounter") == "1"

    # It dies again -> next recovery would make counter 2 > maxStalledCount 1.
    await q.redis.zrem(q.keys.prioritized, jid)
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
    await _claim(q, jid, w.token)
    await w.check_stalled(throttle_ms=0)  # mark
    failed, recovered = await w.check_stalled(throttle_ms=0)  # lock alive -> skip
    assert (failed, recovered) == ([], [])
    assert jid in await q.redis.lrange(q.keys.active, 0, -1)


async def test_lost_lock_cannot_commit(q):
    """A worker that lost its lock can neither complete nor fail the job."""
    job = await q.add("x", {})
    jid = job.id
    w = Worker(QUEUE, _noop, prefix=PREFIX, connection=q.redis)
    await _claim(q, jid, w.token)
    # Someone else steals the lock.
    await q.redis.set(q.keys.lock(jid), "another-worker-token")

    res = await w._move_to_completed(
        keys=[
            q.keys.active,
            q.keys.completed,
            q.keys.job(jid),
            q.keys.lock(jid),
            q.keys.prioritized,
            q.keys.marker,
            q.keys.stalled,
            q.keys.base,
            q.keys.pc,
            q.keys.events,
            q.keys.meta_paused,
        ],
        args=[jid, "{}", _now_ms(), w.token, "0", 30000, -1, -1],
    )
    assert res == -2  # lock lost
    assert await q.redis.zcard(q.keys.completed) == 0
    assert jid in await q.redis.lrange(q.keys.active, 0, -1)  # nothing committed


async def test_fetch_next_in_finish(q):
    """Completing a job with fetch=1 commits it AND hands back the next waiting
    job, already moved to active and locked to us - no extra round trip."""
    cur = await q.add("cur", {})
    nxt = await q.add("nxt", {})
    w = Worker(QUEUE, _noop, prefix=PREFIX, connection=q.redis)

    await _claim(q, cur.id, w.token)  # cur active+locked; nxt stays in prioritized
    res = await w._move_to_completed(
        keys=[
            q.keys.active,
            q.keys.completed,
            q.keys.job(cur.id),
            q.keys.lock(cur.id),
            q.keys.prioritized,
            q.keys.marker,
            q.keys.stalled,
            q.keys.base,
            q.keys.pc,
            q.keys.events,
            q.keys.meta_paused,
            q.keys.limiter,
        ],
        args=[cur.id, "{}", _now_ms(), w.token, "1", 30000, -1, -1, 0, 0, 60_000],
    )
    assert isinstance(res, list) and res[0] == 1
    assert len(res) == 3 and res[2] == nxt.id  # next handed back
    assert nxt.id in await q.redis.lrange(q.keys.active, 0, -1)
    assert await q.redis.get(q.keys.lock(nxt.id)) == w.token  # locked to us
    assert cur.id not in await q.redis.lrange(q.keys.active, 0, -1)
    assert await q.redis.zscore(q.keys.completed, cur.id) is not None


async def test_drains_many_via_fetch_next(q):
    """A single worker drains a backlog by looping through fetch-next."""
    n = 50
    for i in range(n):
        await q.add("j", {"i": i})
    w = Worker(QUEUE, _noop, prefix=PREFIX, concurrency=1)
    t = asyncio.create_task(w.run())
    for _ in range(100):
        if await q.redis.zcard(q.keys.completed) >= n:
            break
        await asyncio.sleep(0.05)
    assert await q.redis.zcard(q.keys.completed) == n
    assert await q.redis.llen(q.keys.active) == 0
    await w.stop()
    t.cancel()


async def test_global_priority_ordering(q):
    """Jobs are processed in one global order: higher priority first, FIFO within
    a level - regardless of enqueue order."""
    await q.add("a", {"p": 0}, priority=0)  # least urgent
    await q.add("b", {"p": 0}, priority=0)
    await q.add("c", {"p": 5}, priority=5)  # most urgent, added last
    await q.add("d", {"p": 2}, priority=2)

    order: list = []

    async def record(job):
        order.append(job.data["p"])

    w = Worker(QUEUE, record, prefix=PREFIX, concurrency=1)
    t = asyncio.create_task(w.run())
    for _ in range(100):
        if len(order) >= 4:
            break
        await asyncio.sleep(0.05)
    await w.stop()
    t.cancel()

    # 5 first, then 2, then the two 0s in FIFO (enqueue) order.
    assert order == [5, 2, 0, 0]


async def test_remove_on_complete_keeps_last_n(q):
    """remove_on_complete=N keeps only the newest N completed jobs."""
    for i in range(5):
        await q.add("k", {"i": i}, remove_on_complete=2)
    processed: list = []

    async def proc(job):
        processed.append(job.id)

    w = Worker(QUEUE, proc, prefix=PREFIX, concurrency=1)
    t = asyncio.create_task(w.run())
    for _ in range(100):
        if len(processed) >= 5:
            break
        await asyncio.sleep(0.05)
    await asyncio.sleep(0.1)
    assert len(processed) == 5
    assert await q.redis.zcard(q.keys.completed) == 2
    await w.stop()
    t.cancel()


async def test_remove_on_complete_true_deletes_job(q):
    """remove_on_complete=True records nothing and drops the job hash."""
    job = await q.add("k", {}, remove_on_complete=True)
    done: list = []

    async def proc(j):
        done.append(1)

    w = Worker(QUEUE, proc, prefix=PREFIX)
    t = asyncio.create_task(w.run())
    for _ in range(100):
        if done:
            break
        await asyncio.sleep(0.05)
    await asyncio.sleep(0.1)
    assert await q.redis.zcard(q.keys.completed) == 0
    assert await q.get_job(job.id) is None
    await w.stop()
    t.cancel()


async def test_await_result_success(q):
    """A producer can await a job's return value across the worker boundary."""

    async def proc(job):
        await asyncio.sleep(0.1)
        return {"doubled": job.data["n"] * 2}

    w = Worker(QUEUE, proc, prefix=PREFIX)
    t = asyncio.create_task(w.run())
    job = await q.add("calc", {"n": 21})
    result = await job.result(timeout=5)
    assert result == {"doubled": 42}
    await w.stop()
    t.cancel()


async def test_await_result_failure_raises(q):
    """A failed job surfaces as JobFailedError from result()."""

    async def proc(job):
        await asyncio.sleep(0.1)
        raise RuntimeError("kaboom")

    w = Worker(QUEUE, proc, prefix=PREFIX)
    t = asyncio.create_task(w.run())
    job = await q.add("boom", {}, attempts=1)
    with pytest.raises(JobFailedError, match="kaboom"):
        await job.result(timeout=5)
    await w.stop()
    t.cancel()


async def test_result_works_with_remove_on_complete(q):
    """result() still delivers the value even when the job hash is auto-removed,
    because the outcome is published before removal."""

    async def proc(job):
        await asyncio.sleep(0.1)
        return "ok"

    w = Worker(QUEUE, proc, prefix=PREFIX)
    t = asyncio.create_task(w.run())
    job = await q.add("ephemeral", {}, remove_on_complete=True)
    result = await job.result(timeout=5)
    assert result == "ok"
    assert await q.get_job(job.id) is None  # hash was removed
    await w.stop()
    t.cancel()


async def test_pause_and_resume(q):
    """A paused queue stops claiming new jobs; resume picks them up again."""
    processed: list = []

    async def proc(job):
        processed.append(job.id)

    w = Worker(QUEUE, proc, prefix=PREFIX)
    t = asyncio.create_task(w.run())

    await q.pause()
    assert await q.is_paused()
    await q.add("a", {})
    await q.add("b", {})
    await asyncio.sleep(0.6)
    assert processed == []  # nothing claimed while paused
    assert (await q.counts())["wait"] == 2

    await q.resume()
    assert not await q.is_paused()
    for _ in range(60):
        if len(processed) >= 2:
            break
        await asyncio.sleep(0.05)
    assert len(processed) == 2  # both ran after resume

    await w.stop()
    t.cancel()


async def test_custom_job_id_is_idempotent(q):
    """A custom job_id dedupes: re-adding the same id is ignored, not duplicated."""
    j1 = await q.add("welcome", {"to": "ada"}, job_id="order-42")
    assert j1.id == "order-42"

    j2 = await q.add("welcome", {"to": "someone-else"}, job_id="order-42")
    assert j2.id == "order-42"
    assert (await q.counts())["wait"] == 1  # not duplicated
    assert (await q.get_job("order-42")).data == {"to": "ada"}  # original kept

    # Once removed, the id is free to reuse.
    await q.remove_job("order-42")
    await q.add("welcome", {"to": "new"}, job_id="order-42")
    assert (await q.get_job("order-42")).data == {"to": "new"}


async def test_custom_job_id_rejects_all_digits(q):
    with pytest.raises(ValueError):
        await q.add("x", {}, job_id="123")


async def test_deduplication_throttles_within_ttl(q):
    """A dedup id with a ttl ignores repeats within the window."""
    j1 = await q.add("notify", {"u": 1}, deduplication={"id": "user-1", "ttl": 5000})
    j2 = await q.add("notify", {"u": 1}, deduplication={"id": "user-1", "ttl": 5000})
    assert j2.id == j1.id  # deduped → same id
    assert (await q.counts())["wait"] == 1  # not duplicated
    # A different dedup id is unaffected.
    await q.add("notify", {"u": 2}, deduplication={"id": "user-2", "ttl": 5000})
    assert (await q.counts())["wait"] == 2


async def test_rate_limit_throttles_throughput(q):
    """A queue-wide limiter caps throughput across the worker; jobs aren't dropped."""
    for i in range(12):
        await q.add("job", {"i": i})
    done: list[str] = []

    async def proc(job):
        done.append(job.id)

    w = Worker(
        QUEUE, proc, prefix=PREFIX, rate_limit={"max": 2, "duration": 1000}, stalled_interval=0
    )
    task = asyncio.create_task(w.run())
    await asyncio.sleep(0.9)
    await w.stop()
    task.cancel()

    # max=2/sec: a burst of 2 plus ~1-2 refilled in under a second - never the lot.
    assert 2 <= len(done) <= 6
    assert len(done) < 12
    # The rest stay queued (rate limiting never fails or drops a job).
    assert (await q.counts())["wait"] == 12 - len(done)


async def test_rate_limit_disabled_runs_everything(q):
    """No limiter → all jobs flow through promptly (guards the rlMax=0 fast path)."""
    for i in range(8):
        await q.add("job", {"i": i})
    done: list[str] = []

    async def proc(job):
        done.append(job.id)

    w = Worker(QUEUE, proc, prefix=PREFIX, stalled_interval=0)
    task = asyncio.create_task(w.run())
    for _ in range(50):
        if len(done) == 8:
            break
        await asyncio.sleep(0.05)
    await w.stop()
    task.cancel()
    assert len(done) == 8


async def test_default_job_options_merge(q):
    """Queue-level default_job_options apply to every add; per-call options win."""
    dq = Queue(
        QUEUE,
        prefix=PREFIX,
        connection=q.redis,
        default_job_options={"remove_on_complete": 1000, "priority": 3},
    )
    j1 = await dq.add("a", {})
    assert j1.opts.remove_on_complete == 1000
    assert j1.opts.priority == 3
    j2 = await dq.add("b", {}, priority=7, remove_on_complete=True)
    assert j2.opts.priority == 7  # per-call overrides the default
    assert j2.opts.remove_on_complete is True


async def test_graceful_shutdown_finishes_inflight(q):
    """stop() lets an in-flight job finish instead of killing it mid-run."""
    job = await q.add("slow", {})
    done: list = []

    async def slow(j):
        await asyncio.sleep(0.5)
        done.append(j.id)

    w = Worker(QUEUE, slow, prefix=PREFIX)
    t = asyncio.create_task(w.run())
    await asyncio.sleep(0.2)  # job is now in-flight
    await w.stop(grace_period=5)  # should wait for the 0.5s handler to finish

    assert done == [job.id]  # ran to completion
    assert await q.redis.zcard(q.keys.completed) == 1
    assert await q.redis.llen(q.keys.active) == 0
    t.cancel()


async def test_zombie_worker_recovered_and_completed_once(q):
    """End to end: a hung worker's job is recovered and finished by another,
    and the zombie's late finish is rejected - exactly one completion."""
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
        QUEUE,
        slow,
        prefix=PREFIX,
        lock_duration=300,
        renew_locks=False,
        stalled_interval=0,
    )
    healthy = Worker(
        QUEUE,
        fast,
        prefix=PREFIX,
        lock_duration=30000,
        stalled_interval=300,
        max_stalled_count=5,
    )
    completed: list = []
    lost: list = []
    healthy.on("completed", lambda j, r: completed.append(j.id))
    zombie.on("lock-lost", lambda jid: lost.append(jid))

    zt = asyncio.create_task(zombie.run())
    await asyncio.sleep(0.4)  # zombie grabs the job, then "hangs"
    ht = asyncio.create_task(healthy.run())
    await asyncio.sleep(2.0)  # healthy recovers + completes it

    assert seen.count("B") == 1
    assert completed == [next(iter(completed), None)] and len(completed) == 1
    assert await q.redis.zcard(q.keys.completed) == 1

    await asyncio.sleep(1.5)  # zombie wakes (~3.4s), tries to commit
    assert lost  # its finish was rejected by the token guard
    assert await q.redis.zcard(q.keys.completed) == 1  # still exactly one

    await zombie.stop()
    await healthy.stop()
    zt.cancel()
    ht.cancel()


async def test_dedup_id_rejects_key_unsafe_characters(q):
    # the dedup id becomes a Redis key segment ("de:<id>"); a ':' or control
    # char lets two logically different ids collide and silently drop jobs
    for bad in ("user:123", "a\x00b", "x\x1fy"):
        with pytest.raises(ValueError, match="deduplication id"):
            await q.add("m", {}, deduplication={"id": bad, "ttl": 60_000})


async def test_result_event_roundtrips_hostile_payloads(q, run_worker):
    # completed events are published as JSON built in Lua; a return value full
    # of JSON metacharacters must come back byte-identical through result()
    nasty = {"s": 'he said "}{," \\ and \n newline', "n": [1, {"deep": '"]}'}]}

    async def proc(job):
        return nasty

    async with run_worker(q, proc):
        j = await q.add("m", {})
        assert await q.result(j.id, timeout=10) == nasty
