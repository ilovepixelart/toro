"""Integration: retry lifecycle - exhaustion, recovery, backoff routing, events."""


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
