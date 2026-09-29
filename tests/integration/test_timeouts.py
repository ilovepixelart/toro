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
    assert (await q.get_job(job.id)).state == "failed"


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
