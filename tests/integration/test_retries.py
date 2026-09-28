"""Integration: retry lifecycle - exhaustion, recovery, backoff routing, events."""

import time

from toro import Worker
from toro.job import Job

PREFIX = "torotest"


async def _count(q, state):
    return (await q.counts())[state]


async def test_retries_until_max_then_fails(q, run_worker, run_until):
    retrying, failed = [], []

    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc) as w:
        # attach handlers BEFORE the job exists, so no event can slip past
        w.on("retrying", lambda job, exc: retrying.append(job.id))
        w.on("failed", lambda job, exc: failed.append(job.id))
        j = await q.add("flaky", {}, attempts=3)  # backoff 0 → immediate retries
        assert await run_until(lambda: _count(q, "failed"))

    job = await q.get_job(j.id)
    assert job.state == "failed"
    assert job.attempts_made == 3  # tried exactly `attempts` times
    assert job.failed_reason == "boom"
    assert job.stacktrace and "RuntimeError" in job.stacktrace
    assert retrying.count(j.id) == 2  # attempts - 1 retries ...
    assert failed.count(j.id) == 1  # ... then one terminal failure
    assert await _count(q, "failed") == 1


async def test_succeeds_on_a_later_attempt(q, run_worker, run_until):
    attempt = 0

    async def proc(job):
        nonlocal attempt
        attempt += 1
        if attempt == 1:
            raise RuntimeError("transient")
        return "recovered"

    async with run_worker(q, proc):
        j = await q.add("flaky", {}, attempts=3)
        assert await run_until(lambda: _count(q, "completed"))

    job = await q.get_job(j.id)
    assert job.state == "completed"
    assert job.attempts_made == 2  # failed once, then succeeded
    assert job.returnvalue == "recovered"
    assert await _count(q, "failed") == 0


async def test_backoff_routes_failed_retry_to_delayed(q, run_worker, run_until):
    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        j = await q.add("flaky", {}, attempts=3, backoff=5000)
        # after the first failure it waits out the backoff in `delayed`, not retried yet
        assert await run_until(lambda: _count(q, "delayed"), timeout=3.0)

        counts = await q.counts()
        assert counts["delayed"] == 1
        assert counts["failed"] == 0 and counts["completed"] == 0  # attempts remain
        assert (await q.get_job(j.id)).attempts_made == 1


async def test_a_retry_waiting_out_its_backoff_keeps_its_failed_reason(q, run_worker, run_until):
    """The reason is recorded on every failure, not only the terminal one: a job
    sitting in `delayed` has to say why it is there."""

    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        j = await q.add("flaky", {}, attempts=3, backoff=5000)
        assert await run_until(lambda: _count(q, "delayed"), timeout=3.0)

    assert (await q.get_job(j.id)).failed_reason == "boom"


async def test_a_backoff_retry_is_due_backoff_ms_after_the_failure(q, run_worker, run_until):
    """The retry is scheduled `backoff` ms into the future, not into the past, where
    the promoter would hand it straight back."""

    async def proc(job):
        raise RuntimeError("boom")

    before = int(time.time() * 1000)
    async with run_worker(q, proc):
        j = await q.add("flaky", {}, attempts=3, backoff=5000)
        assert await run_until(lambda: _count(q, "delayed"), timeout=3.0)

    assert await q.redis.zscore(q.keys.delayed, j.id) >= before + 5000


# A worker that is not running finishes with fetch=0, as one shutting down does, so
# the retry stays where the script put it instead of being claimed straight back.


async def _claim_and_fail(w: Worker) -> None:
    job_id, fields = await w._acquire()
    await w._finish_failed(Job.from_hash(job_id, fields), RuntimeError("boom"))


async def test_an_immediate_retry_goes_back_to_wait(q):
    """No backoff means straight back to `wait` with its state saying so, not a trip
    through `delayed` and not a hash still claiming `active`."""
    w = Worker(q.name, lambda job: None, prefix=PREFIX, connection=q.redis)
    j = await q.add("flaky", {}, attempts=2)

    await _claim_and_fail(w)

    assert (await q.get_job(j.id)).state == "wait"
    counts = await q.counts()
    assert (counts["wait"], counts["delayed"], counts["active"]) == (1, 0, 0)


async def test_an_immediate_retry_keeps_its_priority(q):
    """A retry re-enters `wait` at the job's own priority, ahead of less urgent work
    queued while it ran."""
    w = Worker(q.name, lambda job: None, prefix=PREFIX, connection=q.redis)
    urgent = await q.add("urgent", {}, attempts=2, priority=5)
    job_id, fields = await w._acquire()
    later = await q.add("later", {}, priority=1)

    await w._finish_failed(Job.from_hash(job_id, fields), RuntimeError("boom"))

    assert [j.id for j in await q.get_jobs("wait", 0, -1)] == [urgent.id, later.id]
