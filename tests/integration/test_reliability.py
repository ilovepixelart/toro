"""Tests for the reliability core: locks, token guard, stalled recovery.

Needs a Redis on localhost:6379. Uses an isolated prefix and cleans up after
itself, so it won't touch other data.
"""

import asyncio
import time

import pytest

from toro import JobFailedError, Queue, Worker, scripts

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
        args=scripts.completed_args(
            job_id=cur.id,
            returnvalue="{}",
            now=_now_ms(),
            token=w.token,
            fetch="1",
            lock_duration=30000,
            rl_max=0,
            rl_duration=0,
            global_concurrency=0,
        ),
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


async def test_a_job_claimed_before_a_pause_completes_while_paused(q, run_worker, run_until):
    """pause() stops new claims, not the work in hand: the running job's finish commits
    and its fetch of the next job comes back empty. A paused fetch that answered as if
    it had claimed something breaks the finish script after its commit, and the worker
    never reports the completion."""
    release = asyncio.Event()
    started: list[str] = []
    completed: list[str] = []

    async def proc(job):
        started.append(job.id)
        await release.wait()

    job = await q.add("running", {})
    await q.add("next", {})
    async with run_worker(q, proc, concurrency=1, stalled_interval=0) as w:
        w.on("completed", lambda j, r: completed.append(j.id))
        assert await run_until(lambda: started)
        await q.pause()
        release.set()
        assert await run_until(lambda: completed, timeout=2)

    assert completed == [job.id]
    counts = await q.counts()
    assert (counts["completed"], counts["wait"]) == (1, 1), counts


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


@pytest.mark.parametrize(
    "job_id",
    [
        "completed",  # the job hash WOULD BE the completed set: WRONGTYPE for the queue
        "active",
        "prioritized",  # add() found the queue's key and returned as if the job existed
        "marker",
        "id",
        "repeat:nightly",  # a scheduler's template
        "de:sync-user-42",  # a deduplication window
        "de",  # its own lock, `de:lock`, is the window of the deduplication id `lock`
        "metrics:1700000000000",
        "7:lock",  # job 7's lock: claiming job 7 would overwrite this job
        "order-123:results",
    ],
)
async def test_custom_job_id_cannot_land_on_another_key(q, run_worker, run_until, job_id):
    await q.add("first", {})  # the queue's own keys exist
    with pytest.raises(ValueError, match="reserved"):
        await q.add("victim", {}, job_id=job_id)

    # nothing was written, and the queue still works end to end
    ran = []

    async def proc(job):
        ran.append(job.name)

    async with run_worker(q, proc):
        assert await run_until(lambda: ran == ["first"])
    assert (await q.counts())["completed"] == 1


async def test_custom_job_id_may_use_colons(q, run_worker, run_until):
    # `order:123` is how ids are written; only the queue's own namespaces are taken
    async def proc(job):
        return job.id

    job = await q.add("welcome", {}, job_id="order:123")
    async with run_worker(q, proc):
        assert await job.result(timeout=10) == "order:123"
    assert (await q.get_job("order:123")).state == "completed"


async def test_deduplication_throttles_within_ttl(q):
    """A dedup id with a ttl ignores repeats within the window."""
    j1 = await q.add("notify", {"u": 1}, deduplication={"id": "user-1", "ttl": 5000})
    j2 = await q.add("notify", {"u": 1}, deduplication={"id": "user-1", "ttl": 5000})
    assert j2.id == j1.id  # deduped → same id
    assert (await q.counts())["wait"] == 1  # not duplicated
    # A different dedup id is unaffected.
    await q.add("notify", {"u": 2}, deduplication={"id": "user-2", "ttl": 5000})
    assert (await q.counts())["wait"] == 2


@pytest.mark.parametrize("bad", ["", "a:b", "ctrl\x01", "\n", 7])
async def test_a_concurrency_key_must_be_a_key_segment(q, bad):
    """It becomes a Redis key segment, like a scheduler or deduplication id: two
    logically distinct keys must not collide into one."""
    with pytest.raises(ValueError, match="concurrency_key"):
        await q.add("job", {}, concurrency_key=bad)


async def test_a_concurrency_key_is_visible_on_the_job(q):
    job = await q.add("job", {}, concurrency_key="order-42")
    assert (await q.get_job(job.id)).opts.concurrency_key == "order-42"


async def test_the_events_dispatcher_is_live_before_a_result_waits(q):
    """`result()` reads a job's outcome from its terminal event, and a job whose own
    finish removed it has nothing else to read. redis-py's `subscribe()` returns once
    the command is WRITTEN, not once Redis has acted on it: a dispatcher that reported
    itself ready in between would miss every event published in that window."""
    for _ in range(20):
        waiter = Queue(q.name, prefix=PREFIX)
        try:
            await waiter._ensure_dispatcher()
            assert int((await q.redis.pubsub_numsub(q.keys.events))[0][1]) == 1
        finally:
            await waiter.close()


class _Subscription:
    """A subscription whose replies are scripted, and silent once they run out."""

    def __init__(self, *replies: dict) -> None:
        self.left = list(replies)
        self.closed = False

    async def subscribe(self, *_channels: str) -> None:
        return None

    async def get_message(self, timeout: float) -> dict | None:
        if self.left:
            return self.left.pop(0)
        await asyncio.sleep(timeout)
        return None

    async def aclose(self) -> None:
        self.closed = True


async def test_an_unconfirmed_subscription_gives_up_and_closes(q, monkeypatch):
    """Redis never answers the SUBSCRIBE: the wait says so rather than hanging, and the
    half-made subscription is closed instead of holding a pool connection."""
    monkeypatch.setattr("toro.connection.SUBSCRIBE_TIMEOUT", 0.05)
    mute = _Subscription()
    monkeypatch.setattr(q.redis, "pubsub", lambda *a, **kw: mute)

    with pytest.raises(TimeoutError, match="did not confirm"):
        await q.result("1", timeout=5)

    assert mute.closed
    assert q._events_task is None


async def test_a_job_event_before_the_confirmation_is_not_taken_for_it(q, monkeypatch):
    """Another client's job event can arrive on the channel before Redis confirms the
    subscription. Only the confirmation itself may end the wait."""
    replies = _Subscription({"type": "message"}, {"type": "subscribe"})
    monkeypatch.setattr(q.redis, "pubsub", lambda *a, **kw: replies)

    await q._ensure_dispatcher()

    assert replies.left == []  # the event was read, and did not end the wait
    assert not replies.closed
    q._events_task.cancel()


async def test_result_reads_a_job_that_removed_itself(q, run_worker):
    """`remove_on_complete=True` deletes the job inside the finish script, so its value
    can only come from the event. Repeated: the window is a race with Redis."""

    async def proc(job):
        return job.data["i"]

    async with run_worker(q, proc):
        for i in range(10):
            waiter = Queue(q.name, prefix=PREFIX)
            try:
                job = await waiter.add("gone", {"i": i}, remove_on_complete=True)
                assert await job.result(timeout=5) == i
            finally:
                await waiter.close()


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


@pytest.mark.parametrize("outcome", ["completed", "failed"])
async def test_a_finish_that_fetches_the_next_job_pays_the_limiter(q, outcome):
    """Each finish script fetches the next job under the same limiter, reading the
    limit from its own argument slots. A slot off by one is a limiter silently off, or
    silently wrong, on that path alone. The bucket in Redis tells: the next fetch
    spends one token of a bucket sized exactly `max`, and refills at `max/duration`."""
    for i in range(3):
        await q.add("job", {"i": i})

    async def proc(job):
        if outcome == "failed":
            raise RuntimeError("boom")

    w = Worker(
        QUEUE, proc, prefix=PREFIX, rate_limit={"max": 7, "duration": 60_000}, stalled_interval=0
    )
    w.on("failed", lambda *a, **k: None)
    task = asyncio.create_task(w.run())
    try:
        await asyncio.sleep(0.5)  # three claims: the first pop, then two finish-fetches
    finally:
        await w.stop()
        task.cancel()

    bucket = await q.redis.hgetall(q.keys.limiter)
    assert (await q.counts())[outcome] == 3
    # 7 tokens, one per claim, refilled by at most 0.5 s at 7 per minute
    assert 4.0 <= float(bucket["tokens"]) < 4.1, bucket
    assert 60_000 < await q.redis.pttl(q.keys.limiter) <= 61_000  # duration + 1 s


@pytest.mark.parametrize("outcome", ["completed", "failed"])
async def test_a_rate_limited_fetch_next_rearms_the_marker(q, outcome):
    """A finish whose fetch of the next job is turned away by the limiter leaves that
    job waiting and arms the marker, so a worker wakes to claim it once a token frees.
    Without the marker the job waits for an unrelated add to wake anyone."""
    cur = await q.add("cur", {})
    nxt = await q.add("nxt", {})
    w = Worker(
        QUEUE, _noop, prefix=PREFIX, rate_limit={"max": 1, "duration": 60_000}, connection=q.redis
    )
    await _claim(q, cur.id, w.token)
    now = _now_ms()
    await q.redis.hset(q.keys.limiter, mapping={"tokens": 0, "ts": now})  # the bucket is spent
    await q.redis.delete(q.keys.marker)

    limit = {"fetch": "1", "lock_duration": 30000, "rl_max": 1, "rl_duration": 60_000}
    if outcome == "completed":
        await w._move_to_completed(
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
            args=scripts.completed_args(
                job_id=cur.id,
                returnvalue="{}",
                now=now,
                token=w.token,
                global_concurrency=0,
                **limit,
            ),
        )
    else:
        await w._move_to_failed(
            keys=[
                q.keys.active,
                q.keys.prioritized,
                q.keys.delayed,
                q.keys.failed,
                q.keys.job(cur.id),
                q.keys.lock(cur.id),
                q.keys.marker,
                q.keys.stalled,
                q.keys.base,
                q.keys.pc,
                q.keys.events,
                q.keys.meta_paused,
                q.keys.limiter,
            ],
            args=scripts.failed_args(
                job_id=cur.id,
                reason="boom",
                now=now,
                attempts_made=1,
                max_attempts=1,
                backoff=0,
                token=w.token,
                global_concurrency=0,
                **limit,
            ),
        )

    assert (await q.counts())[outcome] == 1
    assert await q.redis.zscore(q.keys.prioritized, nxt.id) is not None  # not claimed
    assert await q.redis.zcard(q.keys.marker) == 1


async def test_a_rate_limit_of_one_lets_exactly_one_job_through(q, run_worker, run_until):
    """A bucket of max=1 starts full: its single token is spendable at once, and
    spending it leaves the rest waiting out the duration."""
    for i in range(3):
        await q.add("job", {"i": i})
    done: list[str] = []

    async def proc(job):
        done.append(job.id)

    async with run_worker(q, proc, rate_limit={"max": 1, "duration": 60_000}, stalled_interval=0):
        assert await run_until(lambda: done, timeout=2)
        await asyncio.sleep(0.3)  # the other two must NOT run: nothing to wait for

    counts = await q.counts()
    assert (counts["completed"], counts["wait"]) == (1, 2), counts


async def test_a_rate_limited_claim_waits_only_for_the_missing_part_of_a_token(q, monkeypatch):
    """The retry is the time until the bucket holds one whole token again: with half a
    token refilled, half the per-token interval is left, not one and a half."""
    for i in range(3):
        await q.add("job", {"i": i})
    clock = {"now": 1_700_000_000_000}
    monkeypatch.setattr("toro.worker._now_ms", lambda: clock["now"])
    w = Worker(
        QUEUE, _noop, prefix=PREFIX, rate_limit={"max": 2, "duration": 1000}, connection=q.redis
    )
    waits: list[int] = []
    w.on("rate-limited", waits.append)

    assert await w._acquire() is not None
    assert await w._acquire() is not None  # the bucket of 2 is empty
    clock["now"] += 250  # half a token back at 2 per second
    assert await w._acquire() is None

    assert waits == [250]


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


async def test_zombie_worker_recovered_and_completed_once(q, run_until):
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

    async def zombie_holds_it() -> bool:
        return await q.redis.llen(q.keys.active) == 1

    zt = asyncio.create_task(zombie.run())
    assert await run_until(zombie_holds_it)  # zombie grabs the job, then "hangs"
    ht = asyncio.create_task(healthy.run())
    assert await run_until(lambda: completed, timeout=10), "healthy never recovered the job"

    assert seen.count("B") == 1
    assert len(completed) == 1
    assert await q.redis.zcard(q.keys.completed) == 1

    # the zombie wakes (~3s in) and tries to commit
    assert await run_until(lambda: lost, timeout=10), "the zombie's late finish was not rejected"
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


async def test_a_job_that_finishes_between_sweep_passes_is_not_recovered(q, run_until):
    """The sweep marks every active job on one pass and recovers the marked ones
    that have lost their lock on the next. A job that finished in between has lost
    its lock too, but it has also left `active`: recovering it would put a completed
    job back in the queue and run it a second time."""
    job = await q.add("once", {})
    release = asyncio.Event()
    runs: list[str] = []

    async def proc(j):
        runs.append(j.id)
        await release.wait()

    w = Worker(QUEUE, proc, prefix=PREFIX, stalled_interval=0)
    task = asyncio.create_task(w.run())
    try:
        assert await run_until(lambda: runs)
        await w.check_stalled(throttle_ms=0)  # pass 1: marks the running job
        release.set()

        async def completed() -> bool:
            return (await q.counts())["completed"] == 1

        assert await run_until(completed)
        failed, recovered = await w.check_stalled(throttle_ms=0)  # pass 2
    finally:
        release.set()
        await w.stop()
        task.cancel()

    assert (failed, recovered) == ([], [])
    counts = await q.counts()
    assert (counts["completed"], counts["wait"]) == (1, 0), counts
    assert runs == [job.id]


async def test_a_renewal_clears_the_stalled_mark(q):
    """A renewal proves the worker alive, so it takes the job off the sweep's marked
    set. A lock lost after it is a fresh stall, marked on the next pass rather than
    recovered at once on a mark the renewal already answered."""
    job = await q.add("x", {})
    jid = job.id
    w = Worker(QUEUE, _noop, prefix=PREFIX, connection=q.redis)
    await _claim(q, jid, w.token)
    await w.check_stalled(throttle_ms=0)  # pass 1: marks the running job

    renewed = await w._extend_lock(
        keys=[q.keys.lock(jid), q.keys.stalled, q.keys.job(jid)], args=[w.token, 30000, jid]
    )
    assert renewed == 1
    await q.redis.delete(q.keys.lock(jid))  # the worker dies right after renewing

    failed, recovered = await w.check_stalled(throttle_ms=0)  # pass 2: only marks again
    assert (failed, recovered) == ([], [])
    assert jid in await q.redis.lrange(q.keys.active, 0, -1)


async def test_a_sweep_inside_the_throttle_window_does_nothing(q):
    """Workers sharing a queue sweep at most once per stalled_interval between them:
    the first pass takes the throttle key, and a second pass inside the window does
    nothing, even with a lockless job it could otherwise recover."""
    job = await q.add("x", {})
    jid = job.id
    w = Worker(QUEUE, _noop, prefix=PREFIX, connection=q.redis)
    await q.redis.zrem(q.keys.prioritized, jid)
    await q.redis.rpush(q.keys.active, jid)  # a dead worker's job: on `active`, no lock

    assert await w.check_stalled() == ([], [])  # marks it and starts the window
    assert await w.check_stalled() == ([], [])  # inside the window: no recovery
    assert jid in await q.redis.lrange(q.keys.active, 0, -1)


async def test_a_recovered_job_waits_again_at_its_own_priority(q):
    """Recovery puts a job back exactly as it was queued: state `wait`, and its stored
    priority, so an urgent job does not fall behind a less urgent one by stalling."""
    urgent = await q.add("urgent", {}, priority=10)
    other = await q.add("other", {}, priority=5)
    w = Worker(QUEUE, _noop, prefix=PREFIX, connection=q.redis)
    # a worker claimed the urgent job and died: active, no lock
    await q.redis.zrem(q.keys.prioritized, urgent.id)
    await q.redis.hset(q.keys.job(urgent.id), "state", "active")
    await q.redis.rpush(q.keys.active, urgent.id)

    await w.check_stalled(throttle_ms=0)  # mark
    failed, recovered = await w.check_stalled(throttle_ms=0)  # recover
    assert (failed, recovered) == ([], [urgent.id])

    assert (await q.get_job(urgent.id)).state == "wait"
    assert await q.redis.zrange(q.keys.prioritized, 0, -1) == [urgent.id, other.id]


async def test_a_renewal_that_finds_another_workers_lock_reports_lock_lost(
    q, run_worker, run_until
):
    """A takeover (another worker re-claimed the job after a stall) leaves the hash
    and replaces the lock token. The renewal emits `lock-lost` once and stops
    renewing, and the processor is not cancelled: it runs on, and only its finish is
    dropped."""
    release = asyncio.Event()
    ran_to_the_end: list[str] = []

    async def proc(job):
        await release.wait()
        ran_to_the_end.append(job.id)

    job = await q.add("long", {})
    lost: list[str] = []
    async with run_worker(q, proc, lock_duration=30_000, lock_renew_time=50) as worker:
        worker.on("lock-lost", lost.append)
        assert await run_until(lambda: q.redis.exists(q.keys.lock(job.id)))
        await q.redis.set(q.keys.lock(job.id), "another-worker", px=30_000)

        assert await run_until(lambda: lost, timeout=2)
        await asyncio.sleep(0.2)  # four more renew intervals: none may renew or re-emit
        assert lost == [job.id]
        assert await q.redis.get(q.keys.lock(job.id)) == "another-worker"
        release.set()
        assert await run_until(lambda: ran_to_the_end, timeout=2)


async def test_a_job_outliving_its_lock_duration_keeps_the_lock_by_renewing(
    q, run_worker, run_until
):
    """lock_duration bounds a silent worker, not a long job: renewals every
    lock_renew_time keep the lock, so a job running over three lock durations still
    commits. A renewal misread as a lost lock stops renewing, the lock lapses and the
    finish is dropped."""

    async def proc(job):
        await asyncio.sleep(1)  # the job's own work, over three lock durations
        return "done"

    completed: list[str] = []
    lost: list[str] = []
    async with run_worker(
        q, proc, lock_duration=300, lock_renew_time=100, stalled_interval=0
    ) as worker:
        worker.on("completed", lambda j, r: completed.append(j.id))
        worker.on("lock-lost", lost.append)
        job = await q.add("long", {})
        assert await run_until(lambda: completed, timeout=3)

    assert completed == [job.id]
    assert lost == []
