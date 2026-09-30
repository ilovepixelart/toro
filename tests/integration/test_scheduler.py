"""Integration: repeatable schedules (every / cron) firing through a real worker.

(The pure next_run math is covered in tests/unit/test_scheduler.py.)
"""

import asyncio
import json

import pytest
from redis.asyncio.client import Pipeline

from toro import Queue, Worker

PREFIX = "torotest"


async def test_scheduler_every_fires_repeatedly(q, run_worker, run_until):
    runs: list = []

    async def proc(job):
        runs.append(job.name)

    async with run_worker(q, proc):
        await q.add_scheduler("tick", every=400, name="tick")
        assert await run_until(lambda: len(runs) >= 3, timeout=8.0)

    assert all(n == "tick" for n in runs)
    scheds = await q.schedulers()
    assert len(scheds) == 1 and scheds[0]["id"] == "tick"


async def test_remove_scheduler_stops_it(q, run_worker, run_until):
    runs: list = []

    async def proc(job):
        runs.append(1)

    async with run_worker(q, proc):
        await q.add_scheduler("tick", every=400)
        assert await run_until(lambda: len(runs) >= 1, timeout=8.0)

        await q.remove_scheduler("tick")
        assert await q.schedulers() == []

        fired = len(runs)
        await asyncio.sleep(2.5)  # well past interval + delayed poll
        assert len(runs) - fired <= 1  # at most one in-flight occurrence


async def test_scheduler_template_carries_the_queue_defaults(q):
    """A worker mints every occurrence from the STORED template and never sees the
    producer's `default_job_options`, so they have to be in the template."""
    keep = {"remove_on_complete": False, "remove_on_fail": False}
    producer = Queue(q.name, prefix=PREFIX, default_job_options={**keep, "attempts": 4})
    try:
        await producer.add_scheduler("tick", every=60_000, backoff=250)
        stored = json.loads(await q.redis.hget(q.keys.scheduler("tick"), "opts"))
        first = (await q.get_jobs("delayed", 0, 0))[0]
    finally:
        await producer.close()

    assert (stored["removeOnComplete"], stored["removeOnFail"]) == (False, False)
    assert (stored["attempts"], stored["backoff"]) == (4, 250)  # merged, not replaced
    assert first.opts.remove_on_complete is False  # and stamped on the occurrence


async def test_scheduler_options_win_over_the_queue_defaults(q):
    producer = Queue(q.name, prefix=PREFIX, default_job_options={"attempts": 4})
    try:
        await producer.add_scheduler("tick", every=60_000, attempts=2)
        stored = json.loads(await q.redis.hget(q.keys.scheduler("tick"), "opts"))
    finally:
        await producer.close()
    assert stored["attempts"] == 2


async def test_a_cron_that_never_comes_round_leaves_nothing_behind(q):
    """`0 0 30 2 *` parses: February the 30th. croniter accepts it and then refuses to
    name a next run, so the scheduler hash was written and the call raised after it:
    a template invisible to `schedulers()` (which reads the repeat set) and impossible
    to remove by name. Anything that can refuse has to refuse before the first write.
    """
    with pytest.raises(ValueError, match="cron"):
        await q.add_scheduler("impossible", cron="0 0 30 2 *", name="never")

    assert await q.schedulers() == []
    assert await q.redis.exists(q.keys.scheduler("impossible")) == 0


async def test_changing_a_schedule_replaces_its_pending_occurrence(q):
    """Re-registering an id updates the schedule, so the occurrence it had pending
    goes: left in place it would still run at the old time, schedule its own
    successor, and outlive remove_scheduler, which only knows the new one."""
    await q.add_scheduler("nightly", every=60_000)
    # Two cadences whose next slots differ. Were they ever to coincide, both would be
    # one occurrence id and this run would prove nothing, but it could not fail.
    await q.add_scheduler("nightly", every=37_000)

    pending = await q.redis.zrange(q.keys.delayed, 0, -1)
    assert len(pending) == 1, pending

    await q.remove_scheduler("nightly")
    assert await q.redis.zrange(q.keys.delayed, 0, -1) == []


async def test_changing_a_schedule_leaves_a_running_occurrence_alone(q):
    """Only an occurrence that has not started is dropped: one a worker is already
    running finishes as it would have."""
    await q.add_scheduler("nightly", every=60_000)
    (old_id,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    await q.redis.hset(q.keys.job(old_id), "state", "active")  # as a claim leaves it

    await q.add_scheduler("nightly", every=37_000)

    assert await q.redis.hget(q.keys.job(old_id), "state") == "active"


async def test_an_occurrence_claimed_while_it_is_being_dropped_survives(q, monkeypatch):
    """The state is read, then removed in a transaction watching the job's hash. A
    worker claiming the occurrence between those two steps writes that hash, so the
    removal aborts and the claimed run is left alone, with no error for the caller."""
    await q.add_scheduler("nightly", every=60_000)
    (old_id,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    read_state = Pipeline.hget

    async def claim_after_the_read(pipe, *args):
        state = await read_state(pipe, *args)
        await q.redis.hset(q.keys.job(old_id), "state", "active")  # the worker's claim
        return state

    monkeypatch.setattr(Pipeline, "hget", claim_after_the_read)

    await q.add_scheduler("nightly", every=37_000)

    assert await q.redis.hget(q.keys.job(old_id), "state") == "active"


@pytest.mark.parametrize("cadence", [{}, {"every": 60_000, "cron": "0 3 * * *"}])
async def test_a_schedule_takes_exactly_one_cadence(q, cadence):
    with pytest.raises(ValueError, match="exactly one of `every` or `cron`"):
        await q.add_scheduler("nightly", **cadence)
    assert await q.schedulers() == []


async def test_an_occurrence_run_early_still_schedules_the_next(q, run_worker, run_until):
    """Promoting an occurrence runs it before its slot. The next one follows its slot,
    not the clock: counted from `now` it lands on the running occurrence's own id,
    nothing new is enqueued, and the schedule never runs again."""
    await q.add_scheduler("tick", every=60_000)
    (first,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    slot = int(first.rsplit(":", 1)[1])
    assert await q.promote_job(first) is True
    runs: list[str] = []

    async with run_worker(q, lambda job: runs.append(job.id)):
        assert await run_until(lambda: runs)

    assert runs == [first]
    assert await q.redis.zrange(q.keys.delayed, 0, -1) == [f"repeat:tick:{slot + 60_000}"]
    assert (await q.schedulers())[0]["next"] == slot + 60_000


async def test_an_occurrence_run_late_skips_to_the_next_slot_after_now(
    q, run_worker, run_until, monkeypatch
):
    """A late pickup (workers were down) schedules the next slot after the clock, so
    the missed slots are skipped rather than fired one after another as a backlog."""
    await q.add_scheduler("tick", every=60_000)
    (first,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    slot = int(first.rsplit(":", 1)[1])
    assert await q.promote_job(first) is True
    monkeypatch.setattr("toro.worker._now_ms", lambda: slot + 150_000)  # 2.5 slots late
    runs: list[str] = []

    async with run_worker(q, lambda job: runs.append(job.id)):
        assert await run_until(lambda: runs)

    assert await q.redis.zrange(q.keys.delayed, 0, -1) == [f"repeat:tick:{slot + 180_000}"]


async def test_an_occurrence_recovered_after_a_crash_still_schedules_the_next(
    q, run_worker, run_until
):
    """A worker that dies between claiming an occurrence and enqueuing the next leaves
    the chain to the run that recovers it. That run is a second attempt, so enqueuing
    only on a first attempt ended the schedule for good."""
    await q.add_scheduler("tick", every=60_000)
    (first,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    slot = int(first.rsplit(":", 1)[1])
    assert await q.promote_job(first) is True
    doomed = Worker(q.name, lambda job: None, prefix=PREFIX, connection=q.redis)
    assert await doomed._acquire() is not None  # claimed, and the worker dies here
    await q.redis.delete(q.keys.lock(first))
    await doomed.check_stalled(throttle_ms=0)  # mark
    assert await doomed.check_stalled(throttle_ms=0) == ([], [first])  # recover
    runs: list[str] = []

    async with run_worker(q, lambda job: runs.append(job.id)):
        assert await run_until(lambda: runs)

    assert runs == [first]
    assert await q.redis.zrange(q.keys.delayed, 0, -1) == [f"repeat:tick:{slot + 60_000}"]


async def test_a_retried_occurrence_does_not_start_a_second_chain(
    q, run_worker, run_until, monkeypatch
):
    """The first attempt enqueued the next occurrence; a retry after the clock moved
    past it must not enqueue another at a later slot, which would run the schedule
    twice from then on. The next occurrence is due by then, so the retry's own claim
    promotes it and it runs right after: the chain is read while the retry runs."""
    await q.add_scheduler("tick", every=60_000, attempts=2)
    (first,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    slot = int(first.rsplit(":", 1)[1])
    assert await q.promote_job(first) is True
    clock = {"now": slot + 10}
    monkeypatch.setattr("toro.worker._now_ms", lambda: clock["now"])
    runs = 0
    chain: list[tuple[float | None, list[str]]] = []

    async def proc(job):
        nonlocal runs
        runs += 1
        if runs == 1:
            clock["now"] = slot + 70_000  # past the next slot before the retry
            raise RuntimeError("first attempt fails")
        if job.id == first:  # the retry: what did its pickup leave of the chain?
            chain.append(
                (
                    await q.redis.zscore(q.keys.repeat, "tick"),
                    await q.redis.zrange(q.keys.delayed, 0, -1),
                )
            )

    async with run_worker(q, proc):
        assert await run_until(lambda: chain)

    # the chain stands where the first attempt left it, and the retry minted nothing
    assert chain == [(slot + 60_000, [])]


async def test_a_whole_float_interval_runs_like_the_int(q, run_worker, run_until):
    """An interval that came through arithmetic (`30 * 60 * 1000 / 2`) is a float with
    nothing after the point. Stored as "60000.0", the worker could not read it back:
    the first pickup failed before the processor ran, the chain ended, and
    schedulers() raised for the whole queue."""
    await q.add_scheduler("tick", every=60_000.0)
    (first,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    slot = int(first.rsplit(":", 1)[1])
    assert await q.promote_job(first) is True
    runs: list[str] = []

    async with run_worker(q, lambda job: runs.append(job.id)):
        assert await run_until(lambda: runs)

    assert (await q.schedulers())[0]["every"] == 60_000
    assert await q.redis.zrange(q.keys.delayed, 0, -1) == [f"repeat:tick:{slot + 60_000}"]


@pytest.mark.parametrize("every", [1000.5, True], ids=["fractional", "bool"])
async def test_an_interval_that_is_not_whole_is_refused(q, every):
    """A fraction of a millisecond or a bool is no interval; accepted, it killed the
    schedule at its first run. It is refused where it is given, and nothing is kept."""
    with pytest.raises(ValueError, match="positive whole number"):
        await q.add_scheduler("tick", every=every)
    assert await q.schedulers() == []
    assert await q.redis.zrange(q.keys.delayed, 0, -1) == []


async def test_a_numeric_string_interval_still_works(q):
    """A string of digits has always worked end to end, so it keeps working."""
    await q.add_scheduler("tick", every="60000")
    assert (await q.schedulers())[0]["every"] == 60_000


async def test_updating_a_schedule_updates_its_pending_occurrence(q):
    """Re-registering an id with the same cadence but new data updates the schedule
    in place, the occurrence already queued included. Left as it was, it ran once
    more with the old name, data and options, a whole interval after the update."""
    await q.add_scheduler("report", every=3_600_000, name="v1", data={"v": 1}, priority=1)
    await q.add_scheduler("report", every=3_600_000, name="v2", data={"v": 2}, priority=5)

    pending = await q.get_jobs("delayed", 0, -1)
    assert [(j.name, j.data, j.opts.priority) for j in pending] == [("v2", {"v": 2}, 5)]


@pytest.mark.parametrize("drop", ["cancel_job", "remove_job", "clean"])
async def test_a_schedule_outlives_its_pending_occurrence_being_dropped(
    q, run_worker, run_until, monkeypatch, drop
):
    """Only remove_scheduler ends a schedule. Its queued occurrence is the only thing
    that enqueues the next one, so dropping it (cancelled, removed, or cleaned out
    of `delayed` from a dashboard) used to end the schedule silently while
    schedulers() still listed it. The worker's sweep enqueues the next slot."""
    await q.add_scheduler("tick", every=60_000)
    (first,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    slot = int(first.rsplit(":", 1)[1])
    if drop == "clean":
        assert await q.clean("delayed") == 1
    else:
        assert await getattr(q, drop)(first) is True
    monkeypatch.setattr("toro.worker._now_ms", lambda: slot + 1)  # the slot has passed

    async with run_worker(q, lambda job: None):
        assert await run_until(lambda: q.redis.zcard(q.keys.delayed), timeout=5)

    assert await q.redis.zrange(q.keys.delayed, 0, -1) == [f"repeat:tick:{slot + 60_000}"]
    assert (await q.schedulers())[0]["next"] == slot + 60_000


async def test_the_sweep_leaves_a_schedule_whose_occurrence_is_still_queued(q, monkeypatch):
    """A slot that has passed with its occurrence still queued (not yet promoted or
    claimed) is a healthy chain: the occurrence enqueues the next when it runs, and
    the sweep adding one too would run the schedule twice."""
    await q.add_scheduler("tick", every=60_000)
    (first,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    slot = int(first.rsplit(":", 1)[1])
    monkeypatch.setattr("toro.worker._now_ms", lambda: slot + 1)
    worker = Worker(q.name, lambda job: None, prefix=PREFIX, connection=q.redis)

    await worker._resume_orphaned_schedules()

    assert await q.redis.zrange(q.keys.delayed, 0, -1) == [first]
    assert (await q.schedulers())[0]["next"] == slot


async def test_removing_a_schedule_as_its_next_occurrence_is_minted_leaves_nothing(
    q, run_worker, run_until
):
    """remove_scheduler() landing between _schedule_next's reads and its ZADD: the id
    went back into the repeat set with the next slot and one more occurrence was
    enqueued from the template in hand, so schedulers() listed a schedule with no
    template, and it ran once more, until someone removed it again."""
    await q.add_scheduler("tick", every=200)
    removed = asyncio.Event()

    async with run_worker(q, lambda job: None, stalled_interval=0) as w:
        real_zadd = w.redis.zadd

        async def zadd_after_removal(key, mapping, **kw):
            if key == q.keys.repeat and not removed.is_set():
                await q.remove_scheduler("tick")  # lands between the reads and the write
                removed.set()
            return await real_zadd(key, mapping, **kw)

        w.redis.zadd = zadd_after_removal  # this worker's client only
        assert await run_until(removed.is_set, timeout=5)
        await asyncio.sleep(0.3)  # whatever the worker still minted has landed by now

    assert await q.schedulers() == []
    assert await q.redis.zcard(q.keys.repeat) == 0
    assert (await q.counts())["delayed"] == 0


@pytest.mark.parametrize(
    "priority", [1.7, True, -1, "5"], ids=["fraction", "bool", "negative", "text"]
)
async def test_a_scheduler_priority_is_validated_like_a_job_priority(q, priority):
    """`add()` refuses these; `add_scheduler()` clamped them into the template, so a
    bool ran every occurrence at priority 1 and a fraction was truncated."""
    with pytest.raises(ValueError, match="priority"):
        await q.add_scheduler("nightly", every=3_600_000, priority=priority)


async def test_a_scheduler_takes_the_queue_default_priority(q):
    """The docs say the queue's defaults merge under the scheduler's options as they
    do for `add()`; the priority parameter's own default of 0 won over them."""
    producer = Queue(q.name, prefix=PREFIX, default_job_options={"priority": 5})
    try:
        await producer.add_scheduler("nightly", every=3_600_000)
        assert await producer.add_scheduler("urgent", every=3_600_000, priority=1)
    finally:
        await producer.close()
    pending = {j.name: j.opts.priority for j in await q.get_jobs("delayed")}
    assert pending == {"nightly": 5, "urgent": 1}


async def test_a_scheduler_name_is_validated_like_a_job_name(q):
    """The stored name goes into every occurrence and into `trigger_scheduler()`'s
    add(): a name add() refuses ran on schedule and raised on "run now"."""
    with pytest.raises(ValueError, match="job name"):
        await q.add_scheduler("nightly", every=3_600_000, name="x" * 129)
    with pytest.raises(ValueError, match="job name"):
        await q.add_scheduler("nightly", every=3_600_000, name="tab\there")


async def test_removing_a_schedule_between_the_move_and_the_enqueue_leaves_nothing(
    q, run_worker, run_until
):
    """The other gap of the mint above: the schedule has moved to its next slot and
    the occurrence is not enqueued yet. remove_scheduler() landing there read the new
    slot, found no occurrence at it and removed nothing, so the occurrence enqueued
    right after ran once more with no schedule behind it."""
    minting = asyncio.Event()
    release = asyncio.Event()

    async def proc(job):
        pass

    async with run_worker(q, proc) as worker:
        add_occurrence = worker._add_scheduled

        async def held(**kw):
            minting.set()
            await release.wait()  # remove_scheduler() lands between the two writes
            return await add_occurrence(**kw)

        worker._add_scheduled = held
        await q.add_scheduler("tick", every=1000)
        await asyncio.wait_for(minting.wait(), 5)

        await q.remove_scheduler("tick")
        release.set()
        assert await run_until(lambda: not worker._current, timeout=5.0)

        assert await q.schedulers() == []
        assert await q.redis.zcard(q.keys.delayed) == 0
