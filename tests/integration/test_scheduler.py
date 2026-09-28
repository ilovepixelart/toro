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
    twice from then on."""
    await q.add_scheduler("tick", every=60_000, attempts=2)
    (first,) = await q.redis.zrange(q.keys.delayed, 0, -1)
    slot = int(first.rsplit(":", 1)[1])
    assert await q.promote_job(first) is True
    clock = {"now": slot + 10}
    monkeypatch.setattr("toro.worker._now_ms", lambda: clock["now"])
    attempts: list[int] = []

    async def proc(job):
        attempts.append(job.attempts_made)
        if len(attempts) == 1:
            clock["now"] = slot + 70_000  # past the next slot before the retry
            raise RuntimeError("first attempt fails")

    async with run_worker(q, proc):
        assert await run_until(lambda: len(attempts) == 2)

    assert await q.redis.zrange(q.keys.delayed, 0, -1) == [f"repeat:tick:{slot + 60_000}"]


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
