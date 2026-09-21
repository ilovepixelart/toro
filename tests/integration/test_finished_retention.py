"""Retention of finished jobs: the defaults an unset option keeps, `False` as the
way out, the per-script trim budget, the one place an option is read, and scheduled
jobs. (Also scheduler input validation.)
"""

import json
import time
import uuid

import pytest

from toro import FlowChild, Worker, scripts
from toro.queue import Queue

PREFIX = "torotest"


async def test_add_scheduler_rejects_non_positive_every(q):
    with pytest.raises(ValueError, match="positive"):
        await q.add_scheduler("bad", every=0)
    with pytest.raises(ValueError, match="positive"):
        await q.add_scheduler("bad", every=-5000)


async def _seed_finished(q: Queue, state: str, n: int) -> None:
    """`n` jobs that finished a day ago, oldest first: `<state>0` .. `<state>{n-1}`,
    each with its hash and a log line (an aux key a trim must take with it)."""
    set_key = q.keys.completed if state == "completed" else q.keys.failed
    old = int(time.time() * 1000) - 86_400_000
    pipe = q.redis.pipeline(transaction=False)
    for i in range(n):
        jid = f"{state}{i}"
        pipe.hset(q.keys.job(jid), mapping={"id": jid, "name": "bench", "state": state})
        pipe.rpush(q.keys.logs(jid), "a log line")
        pipe.zadd(set_key, {jid: old + i})
        if i % 2000 == 1999:
            await pipe.execute()
            pipe = q.redis.pipeline(transaction=False)
    await pipe.execute()


async def _finish_one(q: Queue, **opts) -> None:
    """One real claim and completion, straight through the scripts, of a job added
    with the given options - one finish at a time, so each trim can be counted."""
    token = uuid.uuid4().hex
    acquire = q.redis.register_script(scripts.MOVE_TO_ACTIVE)
    complete = q.redis.register_script(scripts.MOVE_TO_COMPLETED)
    job = await q.add("bench", {}, **opts)
    now = int(time.time() * 1000)
    res = await acquire(
        keys=[
            q.keys.prioritized,
            q.keys.active,
            q.keys.marker,
            q.keys.stalled,
            q.keys.base,
            q.keys.pc,
            q.keys.meta_paused,
            q.keys.limiter,
        ],
        args=[token, 30_000, now, 0, 0, 0],
    )
    assert res and res[1] == job.id
    out = await complete(
        keys=[
            q.keys.active,
            q.keys.completed,
            q.keys.job(job.id),
            q.keys.lock(job.id),
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
            job_id=job.id,
            returnvalue="null",
            now=now,
            token=token,
            fetch="0",
            lock_duration=30_000,
            rl_max=0,
            rl_duration=0,
            global_concurrency=0,
        ),
    )
    assert out == [1]


async def test_age_trim_is_bounded_per_finish(q):
    n = 2500
    await _seed_finished(q, "completed", n)

    # keepAge=1h: every seeded entry is expired, but a single finish may only
    # trim a bounded slice of them.
    await _finish_one(q, remove_on_complete={"age": 3600})

    # Exactly one bounded slice trimmed (oldest first), the rest left for the
    # next finishes to amortize - plus the job that just completed.
    remaining = await q.redis.zcard(q.keys.completed)
    assert remaining == n - 1000 + 1, f"trim not bounded: {remaining} left of {n}"
    # The trimmed slice was the oldest - its hashes are gone, newer ones remain.
    assert not await q.redis.exists(q.keys.job("completed0"))
    assert await q.redis.exists(q.keys.job(f"completed{n - 1}"))


async def test_count_trim_is_bounded_per_finish(q):
    """BR-005: a count bound met by a deep backlog drains it a slice per finish."""
    n, keep = 2500, 10
    await _seed_finished(q, "completed", n)

    await _finish_one(q, remove_on_complete=keep)

    # One slice, oldest first: the 1000 oldest are gone with their hashes, and
    # everything newer is untouched, the job that just finished included.
    assert await q.redis.zcard(q.keys.completed) == n - 1000 + 1
    assert not await q.redis.exists(q.keys.job("completed0"))
    assert not await q.redis.exists(q.keys.job("completed999"))
    assert await q.redis.exists(q.keys.job("completed1000"))

    # The backlog drains over the following finishes and settles AT the bound.
    sizes = []
    for _ in range(4):
        await _finish_one(q, remove_on_complete=keep)
        sizes.append(await q.redis.zcard(q.keys.completed))
    assert sizes == [502, keep, keep, keep]
    # What is left is the newest by finish time: the five jobs finished here and
    # the five newest of the backlog, hashes intact; the next oldest is gone.
    kept = await q.redis.zrange(q.keys.completed, 0, -1)
    assert kept[:5] == [f"completed{i}" for i in range(n - 5, n)]
    assert all(not jid.startswith("completed") for jid in kept[5:])
    assert all([await q.redis.exists(q.keys.job(jid)) for jid in kept])
    assert not await q.redis.exists(q.keys.job(f"completed{n - 6}"))


async def test_count_and_age_trims_share_one_budget(q):
    """Both bounds on one job: the age trim and the count trim together still delete
    at most one batch in a finish."""
    n = 3000
    await _seed_finished(q, "completed", n)  # all a day old, all past a bound of 10

    await _finish_one(q, remove_on_complete={"count": 10, "age": 3600})

    assert await q.redis.zcard(q.keys.completed) == n + 1 - 1000


async def _flaky(job):
    if job.name == "boom":
        raise RuntimeError(job.name)


async def _gone(q: Queue, jid: str) -> bool:
    return not await q.redis.exists(q.keys.job(jid), q.keys.logs(jid))


async def test_unset_option_bounds_the_finished_sets(q, run_worker, run_until):
    """BR-001: on defaults, completed keeps the newest 1000 and failed the newest 5000."""
    await _seed_finished(q, "completed", 1000)
    await _seed_finished(q, "failed", 5000)

    async with run_worker(q, _flaky, concurrency=1):
        for name in ("ok", "ok", "ok", "boom", "boom"):
            await q.add(name, {})
        assert await run_until(
            lambda: _settled(q, completed=1000, failed=5000, newest=(3, 2)), timeout=10
        )

    # each finish pushed the oldest of its own set out, hash and aux keys with it
    assert [await _gone(q, f"completed{i}") for i in range(4)] == [True, True, True, False]
    assert [await _gone(q, f"failed{i}") for i in range(3)] == [True, True, False]


async def _settled(q: Queue, *, completed: int, failed: int, newest: tuple[int, int]) -> bool:
    """Both sets at the given size AND the jobs just run are the newest members."""
    done = await q.redis.zrange(q.keys.completed, -newest[0], -1)
    dead = await q.redis.zrange(q.keys.failed, -newest[1], -1)
    return (
        await q.redis.zcard(q.keys.completed) == completed
        and await q.redis.zcard(q.keys.failed) == failed
        and not any(j.startswith("completed") for j in done)
        and not any(j.startswith("failed") for j in dead)
    )


@pytest.mark.parametrize("how", ["per job", "per queue"])
async def test_false_keeps_everything(q, run_worker, run_until, how):
    """BR-002: `False` opts out of the default, given per job or for the queue."""
    await _seed_finished(q, "completed", 1000)
    await _seed_finished(q, "failed", 5000)
    keep = {"remove_on_complete": False, "remove_on_fail": False}
    producer = Queue(
        q.name, prefix=PREFIX, default_job_options=keep if how == "per queue" else None
    )

    try:
        async with run_worker(q, _flaky, concurrency=1):
            for name in ("ok", "ok", "ok", "boom", "boom"):
                await producer.add(name, {}, **(keep if how == "per job" else {}))
            assert await run_until(
                lambda: _settled(q, completed=1003, failed=5002, newest=(3, 2)), timeout=10
            )
    finally:
        await producer.close()

    assert not await _gone(q, "completed0")
    assert not await _gone(q, "failed0")


# keepFor is local to the shared Lua, so it is run the way the scripts reach it:
# with the library prepended.
_KEEP_FOR = scripts._LIB + "local c, a = keepFor(ARGV[1], ARGV[2]) return {c, a}"
_UNSET = {"completed": (1000, -1), "failed": (5000, -1)}  # the documented defaults


@pytest.mark.parametrize("state", ["completed", "failed"])
@pytest.mark.parametrize(
    ("value", "kept"),
    [
        ("absent", None),  # the option was never given: the default for the set
        (None, None),
        (False, (-1, -1)),  # keep everything: the way out of the default
        (True, (0, -1)),  # remove at once
        (1, (1, -1)),  # not True: the newest one
        (1000, (1000, -1)),
        (2.9, (2, -1)),  # a count is whole (the finish below proves it: this
        ({"count": 2.9}, (2, -1)),  # reply truncates, a rank command does not)
        ({"count": 500}, (500, -1)),
        ({"age": 3600}, (-1, 3600)),
        ({"age": 3600, "count": 500}, (500, 3600)),
        ({}, (-1, -1)),  # a bound that names neither keeps everything
        ("nonsense", (-1, -1)),
        ({"count": "abc"}, (-1, -1)),  # not a number: no bound, not an error
        ({"count": 1e400}, (-1, -1)),  # infinity would compare as no bound anyway
        ({"count": -5}, (-1, -1)),  # any negative means no bound, as -1 does
        ({"age": 2.5}, (-1, 2)),
    ],
    ids=str,
)
async def test_the_one_place_a_remove_option_is_read(q, state, value, kept):
    """BR-003, BR-004: every way a job finishes reads its option HERE, in the script
    that records it - a worker's finish, a parent failed with its child, a job the
    stalled sweep gives up on. There is no second copy of this table to drift."""
    option = "removeOnComplete" if state == "completed" else "removeOnFail"
    other = "removeOnFail" if state == "completed" else "removeOnComplete"
    opts = {other: True}  # the other set's option must not leak into this one
    if value != "absent":
        opts[option] = value
    got = await q.redis.eval(_KEEP_FOR, 0, json.dumps(opts), state)
    assert tuple(got) == (kept or _UNSET[state])


@pytest.mark.parametrize("state", ["completed", "failed"])
@pytest.mark.parametrize("stored", ["not json", '"a string"', "[1, 2"])
async def test_unreadable_opts_count_as_unset(q, state, stored):
    assert tuple(await q.redis.eval(_KEEP_FOR, 0, stored, state)) == _UNSET[state]


async def test_eagerly_failed_parent_is_kept_under_the_default(q, run_worker, run_until):
    """BR-004: no worker ever finishes this parent: the script that fails it bounds it."""
    await _seed_finished(q, "failed", 5000)

    async with run_worker(q, _flaky, concurrency=1):
        parent = await q.add_flow("report", {}, children=[FlowChild("boom", {})])
        assert await run_until(lambda: _state_is(q, parent.id, "failed"), timeout=10)

    # the child's failure and the parent's each pushed one old job out
    assert await q.redis.zcard(q.keys.failed) == 5000
    assert [await _gone(q, f"failed{i}") for i in range(3)] == [True, True, False]


async def _state_is(q: Queue, job_id: str, state: str) -> bool:
    return await q.redis.hget(q.keys.job(job_id), "state") == state


async def test_failing_a_chain_of_ancestors_shares_one_budget(q, run_worker, run_until):
    """A failing leaf fails its ancestors in the SAME script, one recordFinished each.
    The batch bounds the script, not each of them: a deep flow must not multiply it."""
    n = 9000
    await _seed_finished(q, "failed", n)  # 4000 past the default bound

    async with run_worker(q, _flaky, concurrency=1):
        mid = FlowChild("mid", {}, children=[FlowChild("boom", {})])
        root = await q.add_flow("root", {}, children=[mid])
        assert await run_until(lambda: _state_is(q, root.id, "failed"), timeout=10)

    # leaf, mid and root were recorded by one script: three jobs in, one batch out
    assert await q.redis.zcard(q.keys.failed) == n + 3 - 1000


async def test_scheduled_jobs_honor_a_queue_that_keeps_everything(q, run_worker, run_until):
    """End to end: the first occurrence comes from the producer, every later one is
    minted by the WORKER, and none of them may trim a queue that opted out."""
    await _seed_finished(q, "completed", 1000)
    keep = {"remove_on_complete": False, "remove_on_fail": False}
    producer = Queue(q.name, prefix=PREFIX, default_job_options=keep)

    async def grown() -> bool:
        return await q.redis.zcard(q.keys.completed) >= 1003

    try:
        async with run_worker(q, _flaky, concurrency=1):
            await producer.add_scheduler("tick", every=50)
            assert await run_until(grown, timeout=10), "scheduled jobs were trimmed"
    finally:
        await producer.remove_scheduler("tick")
        await producer.close()
    assert not await _gone(q, "completed0")


async def _stall_out(q: Queue, w: Worker, **opts) -> str:
    """A job whose worker died holding it (on `active`, no lock), swept past the limit."""
    job = await q.add("crashy", {}, **opts)
    await q.redis.zrem(q.keys.prioritized, job.id)
    await q.redis.rpush(q.keys.active, job.id)
    await w.check_stalled(throttle_ms=0)  # mark
    failed, _ = await w.check_stalled(throttle_ms=0)  # escalate past the limit
    assert failed == [job.id]
    return job.id


@pytest.mark.parametrize("option", [2.9, {"count": 2.9}, {"count": 2.9, "age": 86_400 * 2}])
async def test_a_fractional_count_is_a_whole_rank(q, option):
    """A rank command takes integers only. A count that reached ZREMRANGEBYRANK as
    2.9 raised inside the finish script, after the finish had committed: no event,
    no metrics, no trim, and a flow child would never settle its parent."""
    await _seed_finished(q, "completed", 5)

    await _finish_one(q, remove_on_complete=option)

    assert await q.redis.zcard(q.keys.completed) == 2


async def test_a_crash_loop_cannot_outgrow_the_failed_bound(q):
    """A job that kills its worker never reaches the failure script: the stalled sweep
    fails it. A queue whose only failures are those has to stay bounded too."""
    await _seed_finished(q, "failed", 5000)
    w = Worker(q.name, _flaky, prefix=PREFIX, max_stalled_count=0, connection=q.redis)

    for _ in range(3):
        await _stall_out(q, w)

    assert await q.redis.zcard(q.keys.failed) == 5000
    assert [await _gone(q, f"failed{i}") for i in range(4)] == [True, True, True, False]


@pytest.mark.parametrize(
    ("option", "size", "kept"),
    [(True, 10, False), (False, 11, True), (2, 2, True)],
)
async def test_a_stalled_out_job_honors_its_remove_on_fail(q, option, size, kept):
    await _seed_finished(q, "failed", 10)
    w = Worker(q.name, _flaky, prefix=PREFIX, max_stalled_count=0, connection=q.redis)

    jid = await _stall_out(q, w, remove_on_fail=option)

    assert await q.redis.zcard(q.keys.failed) == size
    assert bool(await q.redis.exists(q.keys.job(jid))) is kept
