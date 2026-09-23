"""Integration: the blocked-loop detector (docs/specs/sync-and-scale.md).

An async processor that calls a blocking library starves the loop. Lock renewal is a
coroutine, so a starved loop stops renewing, the stalled sweep takes the job back,
and a queue that looks healthy runs its work twice. Nothing else in toro notices,
because nothing else is in a position to: the thing that would notice is the thing
that is not running.
"""

import asyncio
import time

import pytest

from toro import Worker

PREFIX = "torotest"
BLOCKED = "event loop was blocked"


def _completed(q, n: int):
    async def check() -> bool:
        return (await q.counts())["completed"] >= n

    return check


async def test_a_blocking_processor_is_named(q, run_worker, run_until, caplog):
    """SS-005: the warning names the jobs in flight, because the processor that
    blocked the loop is one of them, and fires once for the episode rather than once
    per tick."""

    async def proc(job):
        time.sleep(0.5)  # noqa: ASYNC251 - the defect under test, on purpose
        return 1

    with caplog.at_level("WARNING"):
        async with run_worker(q, proc, blocked_warning=0.2):
            job = await q.add("blocks", {})
            assert await run_until(_completed(q, 1), timeout=15)

    assert BLOCKED in caplog.text
    assert job.id in caplog.text, "the warning did not say which job to look at"
    assert caplog.text.count(BLOCKED) == 1, "one episode, one warning"


async def test_a_run_of_blocking_jobs_still_warns_once(q, run_worker, run_until, caplog):
    """SS-005: one episode, one warning. A processor that blocks on every job would
    otherwise repeat the same sentence per job, and a log that repeats itself is a
    log nobody reads, which is the same as not warning at all."""

    async def proc(job):
        time.sleep(0.4)  # noqa: ASYNC251 - the defect under test, on purpose
        return 1

    with caplog.at_level("WARNING"):
        async with run_worker(q, proc, blocked_warning=0.2, concurrency=1):
            for _ in range(4):
                await q.add("blocks", {})
            assert await run_until(_completed(q, 4), timeout=30)

    assert caplog.text.count(BLOCKED) == 1, "the loop never stopped being blocked"


async def test_a_busy_loop_is_not_a_blocked_one(q, run_worker, run_until, caplog):
    """SS-005: a loop with plenty to do is not a stalled one. A detector that cannot
    tell them apart is noise, and noise is ignored."""

    async def proc(job):
        for _ in range(40):
            await asyncio.sleep(0.005)
        return 1

    with caplog.at_level("WARNING"):
        async with run_worker(q, proc, blocked_warning=0.2, concurrency=4):
            for _ in range(20):
                await q.add("busy", {})
            assert await run_until(_completed(q, 20), timeout=30)

    assert BLOCKED not in caplog.text


async def test_the_detector_can_be_turned_off(q, run_worker, run_until, caplog):
    """It is on by default because the failure it catches is silent, and off is one
    argument away for anyone who disagrees."""

    async def proc(job):
        time.sleep(0.5)  # noqa: ASYNC251 - the defect under test, on purpose
        return 1

    with caplog.at_level("WARNING"):
        async with run_worker(q, proc, blocked_warning=0):
            await q.add("blocks", {})
            assert await run_until(_completed(q, 1), timeout=15)

    assert BLOCKED not in caplog.text


async def test_a_watched_loop_reports_the_lag_to_a_handler(q, run_worker, run_until):
    """A log line is for a person; an event is for a dashboard or an alert."""
    seen: list[tuple[float, list[str]]] = []

    async def proc(job):
        time.sleep(0.5)  # noqa: ASYNC251 - the defect under test, on purpose
        return 1

    async with run_worker(q, proc, blocked_warning=0.2) as w:
        w.on("blocked", lambda lag, jobs: seen.append((lag, jobs)))
        await q.add("blocks", {})
        assert await run_until(_completed(q, 1), timeout=15)

    assert seen, "nothing was reported"
    lag, jobs = seen[0]
    assert lag >= 0.2
    assert jobs, "the jobs in flight are the shortlist of suspects"


@pytest.mark.parametrize("value", [-1, -0.5, 0.002, 0.009])
async def test_a_threshold_smaller_than_the_loops_own_jitter_is_rejected(q, value):
    """Below zero would warn on every tick. Below the loop's own jitter is the same
    thing more politely: a sleep that overshoots by a millisecond is not a blocked
    loop, and a warning that fires on an idle worker is noise with a scary name."""
    with pytest.raises(ValueError, match="blocked_warning"):
        Worker(q.name, lambda job: 1, prefix=PREFIX, connection=q.redis, blocked_warning=value)


async def test_an_idle_worker_never_warns(q, run_worker, caplog):
    """The smallest threshold a caller can ask for still has to survive doing
    nothing at all."""
    with caplog.at_level("WARNING"):
        async with run_worker(q, lambda job: 1, blocked_warning=0.01):
            await asyncio.sleep(1.0)

    assert BLOCKED not in caplog.text


async def test_the_default_threshold_follows_the_lock(q):
    """Tied to the renewal rather than to a round number: lag near a renewal interval
    means a renewal is already late, and a late renewal is how a job gets run twice."""
    w = Worker(q.name, lambda job: 1, prefix=PREFIX, connection=q.redis, lock_duration=20_000)

    assert w.blocked_warning == pytest.approx(w.lock_renew_time / 1000 / 2)
