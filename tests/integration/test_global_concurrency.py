"""Integration: the global concurrency cap - one limit on jobs active at once,
across every worker on the queue.
"""

import asyncio
import contextlib
import enum

import pytest
from redis.exceptions import ResponseError

from toro import Queue, Worker, scripts
from toro.job import Job

PREFIX = "torotest"


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
        Worker(q.name, proc, prefix=PREFIX, concurrency=4, global_concurrency=2, stalled_interval=0)
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
    w1 = Worker(q.name, _noop, prefix=PREFIX, global_concurrency=1, rate_limit=limit)
    w2 = Worker(q.name, _noop, prefix=PREFIX, global_concurrency=1, rate_limit=limit)

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
        Worker(q.name, proc, prefix=PREFIX, concurrency=3, stalled_interval=0) for _ in range(2)
    ]
    await _run_until_drained(q, workers, total)

    assert (await q.counts())["completed"] == total
    assert gauge.peak >= 4  # well past any small cap; 6 when every loop is busy


async def test_crashed_worker_slots_are_recovered(q):
    """A worker that dies holding every slot must not wedge the queue: there is no
    slot counter to leak, so the stalled sweep taking its jobs off `active` is what
    frees them. The healthy worker is refused first, then drains everything."""
    total = 5
    for i in range(total):
        await q.add("job", {"i": i})

    dead = Worker(q.name, _noop, prefix=PREFIX, global_concurrency=2, lock_duration=200)
    assert await dead._acquire() is not None
    assert await dead._acquire() is not None  # both slots held, never finished or renewed

    healthy = Worker(
        q.name,
        _noop,
        prefix=PREFIX,
        concurrency=2,
        global_concurrency=2,
        stalled_interval=200,
        max_stalled_count=5,
    )
    assert await healthy._acquire() is None  # wedged for as long as the dead slots stand

    await _run_until_drained(q, [healthy], total, timeout=8.0)

    counts = await q.counts()
    assert counts["completed"] == total
    assert counts["active"] == 0
    await dead.redis.aclose()


async def _release_by_drain_finish(holder: Worker, job_id: str, fields: dict[str, str]) -> None:
    """A draining worker finishes with fetch=0: it frees the slot, claims nothing."""
    await holder._finish_completed(Job.from_hash(job_id, fields), {"ok": 1})


async def _release_by_drain_failure(holder: Worker, job_id: str, fields: dict[str, str]) -> None:
    """The same drain, but the job fails for good. attempts=1, so there is no retry:
    a retry would re-arm the marker through enqueue and hide a missing wake."""
    await holder._finish_failed(Job.from_hash(job_id, fields), RuntimeError("boom"))


async def _release_by_stalled_failure(holder: Worker, job_id: str, fields: dict[str, str]) -> None:
    """The holder is dead. Its lock runs out and the parked worker's own sweep fails
    the job for good (max_stalled_count=0), which frees the slot with no claim."""


@pytest.mark.parametrize(
    ("release", "holder_lock_ms", "sweep_ms"),
    [
        pytest.param(_release_by_drain_finish, 30_000, 0, id="drain_finish"),
        pytest.param(_release_by_drain_failure, 30_000, 0, id="drain_failure"),
        pytest.param(_release_by_stalled_failure, 200, 200, id="stalled_failure"),
    ],
)
async def test_freed_slot_wakes_parked_worker(q, release, holder_lock_ms, sweep_ms):
    """A slot freed WITHOUT a claim must wake a worker parked on the cap. With a
    30s block_timeout, a missed wake leaves the waiting job untouched far past the
    deadline here."""
    await q.add("held", {})
    waiting = await q.add("waiting", {})
    holder = Worker(
        q.name, _noop, prefix=PREFIX, global_concurrency=1, lock_duration=holder_lock_ms
    )
    held = await holder._acquire()
    assert held is not None

    done: list[str] = []
    started = asyncio.Event()

    async def proc(job):
        done.append(job.id)
        started.set()

    parked = Worker(
        q.name,
        proc,
        prefix=PREFIX,
        global_concurrency=1,
        block_timeout=30.0,
        stalled_interval=sweep_ms,
        max_stalled_count=0,
    )
    task = asyncio.create_task(parked.run())
    try:
        await asyncio.sleep(0.3)  # woken once by the leftover marker, refused, parked again
        assert done == []

        await release(holder, *held)

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(started.wait(), timeout=3.0)
        assert done == [waiting.id]
    finally:
        await parked.stop()
        task.cancel()
        await holder.redis.aclose()


class _Limits(enum.IntEnum):
    DB_POOL = 1


async def test_int_subclass_cap_is_enforced(q):
    """An IntEnum is a valid int to Python. It must cap like one, not silently
    turn the cap off."""
    for i in range(3):
        await q.add("job", {"i": i})
    w = Worker(q.name, _noop, prefix=PREFIX, global_concurrency=_Limits.DB_POOL)
    assert await w._acquire() is not None
    assert await w._acquire() is None
    assert await q.redis.llen(q.keys.active) == 1
    await w.redis.aclose()


async def test_draining_worker_passes_the_wake_on(q):
    """A draining worker still has spare loops parked on the marker. When its last
    job finishes (fetch=0) the freed slot's wake can be popped by one of THOSE
    loops, since Redis serves the longest-blocked client first. It must hand the
    wake on, or the slot sits idle until another worker's block_timeout."""
    release = asyncio.Event()
    ran_on_b = asyncio.Event()

    async def proc_a(job):
        await release.wait()

    async def proc_b(job):
        ran_on_b.set()

    opts = {"prefix": PREFIX, "global_concurrency": 1, "block_timeout": 3.0, "stalled_interval": 0}
    a = Worker(q.name, proc_a, concurrency=2, **opts)
    b = Worker(q.name, proc_b, concurrency=1, **opts)
    ta = asyncio.create_task(a.run())
    await asyncio.sleep(0.2)  # both of A's loops are parked
    await q.add("long", {})
    await asyncio.sleep(0.2)  # A's first loop runs it; the second stays parked
    tb = asyncio.create_task(b.run())
    await asyncio.sleep(0.2)  # B parks: newer than A's spare loop
    await q.add("waiting", {})
    await asyncio.sleep(0.2)  # A's spare loop pops, is refused, re-parks BEHIND B
    stop_a = asyncio.create_task(a.stop())
    await asyncio.sleep(0.2)  # stop()'s wake goes to B, which is refused and re-parks
    try:
        release.set()  # `long` finishes with fetch=0 and arms the marker
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(ran_on_b.wait(), timeout=1.0)
        assert ran_on_b.is_set(), "the freed slot's wake was swallowed by the draining worker"
    finally:
        release.set()
        await stop_a
        await b.stop()
        ta.cancel()
        tb.cancel()


async def test_full_cap_does_not_wake_a_parked_worker(q):
    """A claim that fills the last slot must not arm the marker: a worker woken then
    can only be turned away, one wasted script call per job. While a slot is still
    free and jobs wait, the marker must be armed as ever."""
    for i in range(5):
        await q.add("job", {"i": i}, attempts=1)
    w = Worker(q.name, _noop, prefix=PREFIX, global_concurrency=2)
    w._running = True  # finish with fetch-next, as a running worker does

    await q.redis.delete(q.keys.marker)
    first = await w._acquire()  # 1 of 2 slots taken: one free, jobs waiting
    assert await q.redis.zcard(q.keys.marker) == 1

    await q.redis.delete(q.keys.marker)
    second = await w._acquire()  # the cap is full now
    assert await q.redis.zcard(q.keys.marker) == 0

    # a finish swaps the slot: still full, still no wake (completed and failed paths)
    assert await w._finish_completed(Job.from_hash(*first), {"ok": 1}) is not None
    assert await q.redis.zcard(q.keys.marker) == 0
    assert await w._finish_failed(Job.from_hash(*second), RuntimeError("boom")) is not None
    assert await q.redis.zcard(q.keys.marker) == 0
    assert await q.redis.zcard(q.keys.prioritized) == 1  # a job really was still waiting
    await w.redis.aclose()


async def test_missing_cap_argument_is_an_error(q):
    """A limit must never fail open. A caller that omits the cap gets a script
    error, not a claim that quietly ignores the limit."""
    await q.add("job", {})
    claim = q.redis.register_script(scripts.MOVE_TO_ACTIVE)
    keys = [
        q.keys.prioritized,
        q.keys.active,
        q.keys.marker,
        q.keys.stalled,
        q.keys.base,
        q.keys.pc,
        q.keys.meta_paused,
        q.keys.limiter,
    ]
    with pytest.raises(ResponseError):
        await claim(keys=keys, args=["token", 30_000, 0, 0, 0])
    assert await q.redis.llen(q.keys.active) == 0


async def test_finish_with_a_missing_cap_commits_nothing(q):
    """Fail-closed has to be fail-BEFORE-commit. Redis does not roll a script back:
    a finish that errors after its writes leaves the job committed, the next job
    unclaimed, and no wake armed. Both finish scripts read the cap only when they
    fetch, so that is where an omitted cap must be caught up front."""
    await q.add("first", {})
    await q.add("second", {})
    w = Worker(q.name, _noop, prefix=PREFIX)
    job_id, _fields = await w._acquire()
    k = q.keys
    now = 1_000

    completed_keys = [
        k.active, k.completed, k.job(job_id), k.lock(job_id), k.prioritized, k.marker,
        k.stalled, k.base, k.pc, k.events, k.meta_paused, k.limiter,
    ]  # fmt: skip
    with pytest.raises(ResponseError, match="global concurrency"):
        await w._move_to_completed(
            keys=completed_keys,
            args=scripts.completed_args(
                job_id=job_id,
                returnvalue="{}",
                now=now,
                token=w.token,
                fetch="1",
                lock_duration=30_000,
                rl_max=0,
                rl_duration=0,
                global_concurrency=0,
            )[:-1],  # the cap is the last argument: a caller that never sends it
        )

    failed_keys = [
        k.active, k.prioritized, k.delayed, k.failed, k.job(job_id), k.lock(job_id), k.marker,
        k.stalled, k.base, k.pc, k.events, k.meta_paused, k.limiter,
    ]  # fmt: skip
    with pytest.raises(ResponseError, match="global concurrency"):
        await w._move_to_failed(
            keys=failed_keys,
            args=scripts.failed_args(
                job_id=job_id,
                reason="boom",
                now=now,
                attempts_made=1,
                max_attempts=1,
                backoff=0,
                token=w.token,
                fetch="1",
                lock_duration=30_000,
                rl_max=0,
                rl_duration=0,
                global_concurrency=0,
            )[:-1],
        )

    counts = await q.counts()
    assert (counts["completed"], counts["failed"]) == (0, 0)
    assert await q.redis.lrange(k.active, 0, -1) == [job_id]  # still held, lock intact
    assert await q.redis.get(k.lock(job_id)) == w.token
    await w.redis.aclose()


async def test_slot_of_a_removed_job_is_reused_once_its_processor_ends(q):
    """Removing an active job frees its slot in `active` but deliberately wakes no
    one: the processor may still be running. When it ends, its finish comes back
    lock-lost, and at that moment the worker KNOWS the slot is really free. It must
    say so, or the waiting job sits out a full idle re-poll."""
    release = asyncio.Event()
    started: dict[str, asyncio.Event] = {"held": asyncio.Event(), "waiting": asyncio.Event()}

    async def proc(job):
        started[job.name].set()
        if job.name == "held":
            await release.wait()

    held = await q.add("held", {})
    await q.add("waiting", {})
    w = Worker(
        q.name,
        proc,
        prefix=PREFIX,
        concurrency=2,
        global_concurrency=1,
        block_timeout=4.0,
        stalled_interval=0,
    )
    task = asyncio.create_task(w.run())
    try:
        await asyncio.wait_for(started["held"].wait(), timeout=2.0)
        await asyncio.sleep(0.2)  # the spare loop is parked: the cap is full
        assert await q.remove_job(held.id)
        await asyncio.sleep(0.2)
        assert not started["waiting"].is_set()  # no eager wake: `held` is still running

        release.set()  # the processor ends; its finish returns lock-lost
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(started["waiting"].wait(), timeout=1.0)
        assert started["waiting"].is_set(), "the freed slot sat idle until the re-poll"
    finally:
        release.set()
        await w.stop(grace_period=1)
        task.cancel()
