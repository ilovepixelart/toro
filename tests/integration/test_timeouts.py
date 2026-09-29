"""Integration: the per-job timeout.

Without one, a processor that hung renewed its lock forever and kept its slot, its
share of the global cap and its concurrency key until someone cancelled the job.
"""

import asyncio
import threading
import time

import pytest

from toro import JobFailedError

PREFIX = "torotest"


async def test_a_processor_that_runs_past_the_timeout_fails_the_job(q, run_worker):
    """The failure names the limit, and a job with attempts left retries like any
    other failure: the second run, quicker, completes it."""
    runs = 0

    async def proc(job):
        nonlocal runs
        runs += 1
        if runs == 1:
            await asyncio.sleep(30)  # the first run hangs
        return "done on the second run"

    async with run_worker(q, proc, stalled_interval=0):
        job = await q.add("slow", {}, timeout=300, attempts=2, backoff=0)
        assert await q.result(job.id, timeout=10) == "done on the second run"
    assert runs == 2


async def test_the_timeout_failure_names_the_limit_and_is_terminal_at_one_attempt(q, run_worker):
    async def proc(job):
        await asyncio.sleep(30)

    async with run_worker(q, proc, stalled_interval=0):
        job = await q.add("slow", {}, timeout=200)
        with pytest.raises(JobFailedError, match="timeout of 200 ms"):
            await q.result(job.id, timeout=10)
    ended = await q.get_job(job.id)
    assert ended.state == "failed"
    # Nothing raised in the processor, so there is no traceback to keep: the one of
    # the cancellation that stopped it is the worker's, not the job's.
    assert ended.stacktrace is None


async def _ended(q, job_id: str) -> bool:
    job = await q.get_job(job_id)
    return job is not None and job.state in ("cancelled", "failed")


async def test_a_processor_that_swallows_the_timeout_still_fails(q, run_worker):
    """The timer's cancellation is the job's failure whatever the processor does with
    it: one that caught it and returned was committed completed, with its partial
    result, and never retried."""

    async def proc(job):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            return "partial"

    async with run_worker(q, proc, stalled_interval=0):
        job = await q.add("swallows", {}, timeout=200)
        with pytest.raises(JobFailedError, match="timeout of 200 ms"):
            await q.result(job.id, timeout=10)
    ended = await q.get_job(job.id)
    assert ended.state == "failed"
    assert ended.stacktrace is None  # no exception was being handled: not "NoneType: None"


async def test_a_cancel_followed_by_the_timeout_is_one_signal(q, run_worker, run_until):
    """A cancel request already unwinding the processor is not interrupted again when
    the timer fires during its cleanup, and the job ends cancelled, as the request
    said: the first signal decides."""
    signals: list[str] = []
    started = asyncio.Event()

    async def proc(job):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            signals.append("cancelled")
            await asyncio.sleep(0.4)  # a cleanup the timer must not cut short
            signals.append("cleaned up")
            raise

    async with run_worker(q, proc, stalled_interval=0):
        job = await q.add("cleanup", {}, timeout=300)
        await asyncio.wait_for(started.wait(), 5)
        await q.cancel_job(job.id)  # now; the timer fires at 300 ms, mid-cleanup
        assert await run_until(lambda: _ended(q, job.id), timeout=5)

    assert signals == ["cancelled", "cleaned up"]
    assert (await q.get_job(job.id)).state == "cancelled"


async def test_the_timeout_followed_by_a_cancel_is_one_signal(q, run_worker, run_until):
    """The timer's cancellation already unwinding the processor is not interrupted
    again by a cancel request that lands during its cleanup; the first signal
    decides, so the job fails on its timeout."""
    signals: list[str] = []

    async def proc(job):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            signals.append("cancelled")
            await asyncio.sleep(0.4)  # a cleanup the request must not cut short
            signals.append("cleaned up")
            raise

    async with run_worker(q, proc, stalled_interval=0):
        job = await q.add("cleanup", {}, timeout=200)
        assert await run_until(lambda: len(signals) == 1, timeout=5)  # the timer fired
        await q.cancel_job(job.id)  # lands during the cleanup
        assert await run_until(lambda: _ended(q, job.id), timeout=5)

    assert signals == ["cancelled", "cleaned up"]
    failed = await q.get_job(job.id)
    assert failed.state == "failed"
    assert "timeout of 200 ms" in failed.failed_reason


async def test_a_timeout_error_the_processor_raises_itself_keeps_its_own_message(q, run_worker):
    """The worker tells its timer's cancellation from every other one, so an upstream
    call's TimeoutError is reported as the processor's failure, not the job's limit."""

    async def proc(job):
        raise TimeoutError("upstream took too long")

    async with run_worker(q, proc, stalled_interval=0):
        job = await q.add("upstream", {}, timeout=5_000)
        with pytest.raises(JobFailedError, match="upstream took too long") as failure:
            await q.result(job.id, timeout=10)
    assert "job's timeout" not in str(failure.value)


async def test_a_sync_processor_is_not_subject_to_the_timeout(q, run_worker):
    """A thread cannot be taken back, so the job runs to its end and completes."""
    started = threading.Event()

    def proc(job):
        started.set()
        time.sleep(0.6)
        return "ran to the end"

    async with run_worker(q, proc, stalled_interval=0):
        job = await q.add("blocking", {}, timeout=100)
        assert await q.result(job.id, timeout=10) == "ran to the end"
    assert started.is_set()
