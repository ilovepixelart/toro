"""Integration: a worker actually processing jobs - result, progress, logs, concurrency."""

import asyncio


async def test_add_publishes_a_change_event(q):
    # Enqueuing must emit a signal on the events channel (like completion/failure does),
    # so a live dashboard refreshes when a job is *added*, not only when one finishes.
    ps = q.redis.pubsub()
    await ps.subscribe(q.keys.events)
    await ps.get_message(timeout=1)  # drain the subscribe ack
    await q.add("newjob", {"x": 1})
    msg = await ps.get_message(ignore_subscribe_messages=True, timeout=2)
    assert msg is not None  # enqueue published a change signal
    await ps.aclose()


async def test_processes_job_and_records_full_outcome(q, run_worker, run_until):
    async def proc(job):
        return {"echo": job.data["n"] * 2}

    await q.add("double", {"n": 21})
    async with run_worker(q, proc):
        assert await run_until(lambda: _completed(q))

    job = await q.get_job((await q.get_jobs("completed"))[0].id)
    assert job.state == "completed"
    assert job.returnvalue == {"echo": 42}  # the actual result, not just "done"
    assert job.attempts_made == 1
    assert job.processed_on is not None and job.finished_on is not None
    assert job.finished_on >= job.processed_on


async def test_progress_and_logs_are_persisted(q, run_worker, run_until):
    async def proc(job):
        await job.log("starting")
        await job.update_progress(50)
        await job.update_progress(100)
        await job.log("done")

    j = await q.add("work", {})
    async with run_worker(q, proc):
        assert await run_until(lambda: _completed(q))

    stored = await q.get_job(j.id)
    assert stored.progress == 100  # last reported value persisted
    assert await q.get_logs(j.id) == ["starting", "done"]  # exact log stream


async def test_concurrency_runs_jobs_in_parallel(q, run_worker, run_until):
    inflight = 0
    peak = 0

    async def proc(job):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        await asyncio.sleep(0.15)  # hold the slot so overlap is observable
        inflight -= 1

    for i in range(6):
        await q.add("slow", {"i": i})
    async with run_worker(q, proc, concurrency=4):
        assert await run_until(lambda: _completed(q, n=6), timeout=8.0)

    # With concurrency=4 the slots genuinely overlap - serial execution would peak at 1.
    assert peak >= 2, f"expected parallel execution, peak in-flight was {peak}"


async def _completed(q, n: int = 1) -> bool:
    return (await q.counts())["completed"] >= n


async def test_a_result_that_will_not_encode_fails_the_job(q, run_worker, run_until):
    """A return value that is not JSON is a bug in the processor, and it has to end
    the job like any other. Left to escape the commit, the job stayed `active` with
    its lock held until the stalled sweep re-ran it, burning an attempt on work that
    would fail the same way every time, with nothing but a "hiccup" in the log.
    """

    async def proc(job):
        return object()  # a result the queue cannot store

    async with run_worker(q, proc, stalled_interval=0) as w:
        w.on("failed", lambda *a, **k: None)
        job = await q.add("weird", {}, attempts=1)

        async def failed() -> bool:
            return (await q.counts())["failed"] >= 1

        assert await run_until(failed, timeout=15)

    settled = await q.get_job(job.id)
    assert settled.state == "failed"
    assert "JSON serializable" in (settled.failed_reason or "")
