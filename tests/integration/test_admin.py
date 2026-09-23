"""Integration: admin/dashboard actions - remove, promote, retry, clean, trigger.

Each asserts the resulting state AND the negative case (acting on a missing job
returns False rather than silently succeeding).
"""

import uuid

import pytest

from toro import Queue

PREFIX = "torotest"


async def _count(q, state):
    return (await q.counts())[state]


@pytest.mark.parametrize("bad", ["", "a:b", "repeat:x", "ctrl\x01", "\n"])
async def test_add_scheduler_rejects_unsafe_id(q, bad):
    # scheduler_id is a Redis key segment - ':'/control chars enable key collisions
    with pytest.raises(ValueError, match="scheduler_id"):
        await q.add_scheduler(bad, cron="0 0 * * *")


@pytest.mark.parametrize("bad", ["not a cron", "* * *", "99 * * * *"])
async def test_add_scheduler_rejects_invalid_cron(q, bad):
    # bad cron must fail at enqueue, not silently inside a worker later
    with pytest.raises(ValueError, match="cron"):
        await q.add_scheduler("sched", cron=bad)


async def test_remove_job_deletes_it_everywhere(q):
    j = await q.add("x", {})
    assert await q.remove_job(j.id) is True
    assert await q.get_job(j.id) is None
    assert await _count(q, "wait") == 0


async def test_remove_missing_job_returns_false(q):
    assert await q.remove_job("nope") is False


async def test_promote_moves_delayed_to_wait(q):
    j = await q.add("x", {}, delay=60_000)
    assert await _count(q, "delayed") == 1

    assert await q.promote_job(j.id) is True
    counts = await q.counts()
    assert counts["delayed"] == 0 and counts["wait"] == 1


async def test_promote_missing_job_returns_false(q):
    assert await q.promote_job("nope") is False


async def test_retry_job_moves_failed_back_to_wait_and_clears_reason(q, run_worker, run_until):
    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        j = await q.add("x", {}, attempts=1)  # one shot → straight to failed
        assert await run_until(lambda: _count(q, "failed"))

    assert await q.retry_job(j.id) is True
    counts = await q.counts()
    assert counts["failed"] == 0 and counts["wait"] == 1
    assert (await q.get_job(j.id)).failed_reason is None  # the prior failure is cleared


async def test_retry_all_failed_requeues_every_one(q, run_worker, run_until):
    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        for i in range(3):
            await q.add(f"x{i}", {}, attempts=1)
        assert await run_until(lambda: _count(q, "failed"), timeout=8.0)
        # wait for all three to settle into failed before requeuing
        assert await run_until(lambda: _all_failed(q, 3), timeout=8.0)

    assert await q.retry_all_failed() == 3
    counts = await q.counts()
    assert counts["failed"] == 0 and counts["wait"] == 3


async def test_clean_removes_a_whole_state(q):
    for i in range(4):
        await q.add(f"x{i}", {})
    assert await _count(q, "wait") == 4

    assert await q.clean("wait") == 4
    assert await _count(q, "wait") == 0


async def test_trigger_scheduler_enqueues_one_immediately(q):
    await q.add_scheduler("nightly", cron="0 0 * * *", name="rollup")
    before = await _count(q, "wait")  # the scheduled run is delayed

    assert await q.trigger_scheduler("nightly") is True
    assert await _count(q, "wait") == before + 1  # a manual run is enqueued now


async def test_trigger_missing_scheduler_returns_false(q):
    assert await q.trigger_scheduler("nope") is False


async def test_trigger_scheduler_carries_configured_opts(q):
    # a manual "run now" must match a scheduled occurrence's options, not defaults
    await q.add_scheduler(
        "nightly",
        cron="0 0 * * *",
        name="rollup",
        priority=7,
        attempts=5,
        remove_on_complete=25,
        remove_on_fail=False,
    )
    assert await q.trigger_scheduler("nightly") is True
    job = (await q.get_jobs("wait", 0, 0))[0]
    assert job.name == "rollup"
    assert job.opts.priority == 7
    assert job.opts.attempts == 5
    assert (job.opts.remove_on_complete, job.opts.remove_on_fail) == (25, False)


async def test_trigger_scheduler_leaves_unset_retention_to_the_queue(q):
    """A template that stores retention unset (every scheduler registered by an
    earlier release does) must not beat the queue's default with an explicit None."""
    await q.add_scheduler("nightly", cron="0 0 * * *")  # no defaults: stored unset
    keep = {"remove_on_complete": False, "remove_on_fail": False}
    producer = Queue(q.name, prefix=PREFIX, default_job_options=keep)
    try:
        assert await producer.trigger_scheduler("nightly") is True
    finally:
        await producer.close()
    job = (await q.get_jobs("wait", 0, 0))[0]
    assert (job.opts.remove_on_complete, job.opts.remove_on_fail) == (False, False)


async def _all_failed(q, n):
    return (await q.counts())["failed"] == n


async def test_trigger_scheduler_carries_the_concurrency_key(q):
    """A manual run that drops the key runs beside the scheduled occurrence, which is
    exactly what the key was asked to prevent."""
    await q.add_scheduler("sync", every=60_000, concurrency_key="tenant-1")
    assert await q.trigger_scheduler("sync") is True

    # the occurrence holds the key, so the manual run has to queue behind it
    assert (await q.counts())["held"] == 1
    assert (await q.get_jobs("held", 0, -1))[0].opts.concurrency_key == "tenant-1"


async def test_removing_something_that_is_not_a_job_touches_nothing(q):
    """A job id arrives from a URL, and a job hash lives beside the queue's own keys:
    `remove_job("totals")` used to delete the lifetime counters, `remove_job("meta")`
    the data-model stamp, and `remove_job("worker:<token>")` a live worker's presence
    record, which then read as a crashed worker. A job hash is one with options on it,
    and nothing else is a job whatever its name.
    """
    await q.add("real", {})  # so the totals and the marker exist
    worker_key = f"worker:{uuid.uuid4().hex}"
    await q.redis.hset(q.keys.base + worker_key, mapping={"id": "w1", "host": "h"})

    for name in ("totals", "meta", worker_key):
        assert await q.remove_job(name) is False, name

    assert await q.redis.hget(q.keys.totals, "added") == "1"
    assert await q.redis.hget(q.keys.meta, "model") is not None
    assert await q.redis.exists(q.keys.base + worker_key) == 1
    await q.redis.delete(q.keys.base + worker_key)


async def test_a_scheduled_occurrence_is_still_removable(q):
    """The guard is about what a key IS, not what it is called: an occurrence's id
    looks like an internal key (`repeat:<scheduler>:<millis>`) and is a real job."""
    await q.add_scheduler("nightly", every=60_000, name="rollup")
    [occurrence] = await q.get_jobs("delayed", 0, 10)
    assert occurrence.id.startswith("repeat:")

    assert await q.remove_job(occurrence.id) is True

    assert await q.get_job(occurrence.id) is None
