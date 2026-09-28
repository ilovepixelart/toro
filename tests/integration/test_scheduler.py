"""Integration: repeatable schedules (every / cron) firing through a real worker.

(The pure next_run math is covered in tests/unit/test_scheduler.py.)
"""

import asyncio
import json

import pytest
from redis.asyncio.client import Pipeline

from toro import Queue

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
