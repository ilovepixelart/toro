"""Integration: retry lifecycle - exhaustion, recovery, backoff routing, events."""

import asyncio
import time

import toro.worker as worker_module
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


async def test_retries_claimed_within_one_millisecond_still_spend_attempts(
    q, run_worker, run_until, monkeypatch
):
    """A retry with no backoff is fetched straight back by the finish that failed it,
    so on a fast machine both runs were claimed in the same millisecond and carried the
    same processedOn, which is how a run is named: the second finish read the first
    one's memo as its own re-send and was answered "retry, here it is again" with no
    attempt spent, for ever. A re-claim inside that millisecond is stamped one later."""
    frozen = int(time.time() * 1000)
    monkeypatch.setattr(worker_module, "_now_ms", lambda: frozen)  # every claim in one ms
    failed: list[str] = []

    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc) as w:
        w.on("failed", lambda job, exc: failed.append(job.id))
        j = await q.add("flaky", {}, attempts=3)
        assert await run_until(lambda: _count(q, "failed"), timeout=5)

    job = await q.get_job(j.id)
    assert (job.state, job.attempts_made, failed) == ("failed", 3, [j.id])


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


async def test_a_stalled_run_spends_no_attempt(q, run_worker, run_until):
    """`attempts` bounds the runs that finished by failing. A run cut short by a stall
    (its worker died) counted as one of them, so with attempts=2 a job whose first run
    stalled and whose second raised was failed for good after one real try."""
    seen: list[int] = []

    async def proc(job):
        seen.append(job.attempts_made)
        if len(seen) == 1:
            await asyncio.sleep(2)  # outlives the lock without renewing it: a stall
        elif len(seen) == 2:
            raise RuntimeError("the second run fails")
        return "the third run succeeds"

    job = await q.add("stall-then-fail", {}, attempts=2, backoff=0)
    async with run_worker(
        q, proc, concurrency=2, renew_locks=False, lock_duration=300, stalled_interval=100
    ):
        assert await run_until(lambda: len(seen) >= 3, timeout=15)

        async def completed_once() -> bool:
            return await _count(q, "completed") == 1

        assert await run_until(completed_once, timeout=15)

    assert seen == [0, 0, 1]  # nothing finished before runs 1 and 2; one failure before run 3
    assert (await q.get_job(job.id)).attempts_made == 2


async def test_the_first_retry_waits_the_configured_delay(q, run_worker, run_until):
    """Exponential backoff doubles per attempt from `delay`: the first retry waits
    `delay`, not half of it. The ordinal is the run that just failed, one more than the
    runs that finished before it."""
    raised_at: list[int] = []

    async def proc(job):
        raised_at.append(int(time.time() * 1000))
        raise RuntimeError("boom")

    job = await q.add("slow-retry", {}, attempts=2, backoff={"type": "exponential", "delay": 600})
    async with run_worker(q, proc):
        assert await run_until(lambda: raised_at, timeout=5)

        async def delayed_once() -> bool:
            return await q.redis.zscore(q.keys.delayed, job.id) is not None

        assert await run_until(delayed_once, timeout=5)
        due = await q.redis.zscore(q.keys.delayed, job.id)
    assert 550 <= due - raised_at[0] <= 800  # `delay` after the failure, plus the commit
