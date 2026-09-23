"""Integration: the totals a scraper reads (docs/specs/operate.md).

The per-minute buckets self-expire after eight hours, which is right for charting
and useless for `rate()`: a counter that resets cannot answer "how many since
forever". These totals never expire, and are written in the same atomic step as the
transition they count, so a counter can never disagree with the state change.
"""

import asyncio

from toro import FlowChild, Queue

PREFIX = "torotest"


async def _totals(q: Queue) -> dict[str, int]:
    return {k: int(v) for k, v in (await q.redis.hgetall(q.keys.totals)).items()}


async def test_a_total_counts_every_way_a_job_can_end(q, run_worker, run_until):
    """OP-003: added, completed, failed and cancelled are each counted where the
    transition happens, so no path can record one without the other."""

    async def proc(job):
        if job.name == "breaks":
            raise RuntimeError("boom")
        return job.name

    async with run_worker(q, proc, concurrency=4) as w:
        w.on("failed", lambda *a, **k: None)
        await q.add("ok", {})
        await q.add("breaks", {}, attempts=1)
        stopped = await q.add("stopped", {}, delay=60_000)
        assert await q.cancel_job(stopped.id) is True
        assert await run_until(_settled(q, 3), timeout=10)

    totals = await _totals(q)
    assert totals["added"] == 3
    assert totals["completed"] == 1
    assert totals["failed"] == 1
    assert totals["cancelled"] == 1, "a cancellation was not counted as one"


async def test_the_duration_total_counts_the_time_the_work_took(q, run_worker, run_until):
    """`toro_job_duration_ms_total` is a published family: without it a scraper can
    chart how much work a queue does and not what it costs. It is written where the
    duration is already known, the same step as the outcome it belongs to."""

    async def proc(job):
        await asyncio.sleep(0.05)

    async with run_worker(q, proc):
        await q.add("slow", {})
        assert await run_until(_settled(q, 1), timeout=10)

    ms = (await _totals(q))["ms"]
    assert ms >= 50, "the processing time was not counted"
    assert f'toro_job_duration_ms_total{{queue="{q.name}"}} {ms}' in await q.metrics_text()


async def test_a_flow_counts_every_job_it_adds(q):
    """A whole tree is added in one script, and records one increment for it, so that
    increment is the size of the tree: counting the tree as one job would report a
    queue doing a fraction of the work it does."""
    await q.add_flow("report", {}, children=[FlowChild("a", {}), FlowChild("b", {})])

    assert (await _totals(q))["added"] == 3


async def test_a_scheduled_occurrence_is_counted_as_added(q, run_worker, run_until):
    """A schedule's occurrences are jobs like any other. Counted nowhere, a queue
    driven only by a schedule reports added=0 while completing work, so any panel
    reading backlog as added minus finished goes negative and stays there."""

    async def proc(job):
        return job.name

    await q.add_scheduler("tick", every=100, name="tock")  # mints the first occurrence
    assert (await _totals(q))["added"] == 1

    async with run_worker(q, proc):  # picking one up mints the next
        assert await run_until(_settled(q, 1), timeout=10)

    assert (await _totals(q))["added"] >= 2, "the occurrence a worker minted was not counted"


async def test_totals_never_expire(q):
    """OP-002: `rate()` reads a counter across restarts, so the key it reads from
    cannot be one that quietly disappears after eight hours."""
    await q.add("j", {})

    assert await q.redis.ttl(q.keys.totals) == -1  # -1 is "no expiry", -2 is "gone"


async def test_a_cancellation_is_never_counted_as_a_failure(q):
    """OP-004: the export is one more place the two must not be conflated."""
    root = await q.add_flow("report", {}, children=[FlowChild("leaf", {}, delay=60_000)])

    assert await q.cancel_job(root.id) is True

    totals = await _totals(q)
    assert totals.get("failed", 0) == 0
    assert totals["cancelled"] == 2  # the root and its leaf


def _settled(q: Queue, n: int):
    async def check() -> bool:
        c = await q.counts()
        return c["completed"] + c["failed"] + c["cancelled"] >= n

    return check


async def test_metrics_text_reports_the_queue_a_scraper_would_see(q, run_worker, run_until):
    """OP-001, OP-005: one round trip renders what is in Redis, counters and current
    depth together, with every state covered."""

    async def proc(job):
        return job.name

    async with run_worker(q, proc, concurrency=2):
        await q.add("done", {})
        assert await run_until(_settled(q, 1), timeout=10)
    await q.add("waiting", {})
    stopped = await q.add("stopped", {}, delay=60_000)
    assert await q.cancel_job(stopped.id) is True

    text = await q.metrics_text()

    assert f'toro_jobs_total{{queue="{q.name}",outcome="completed"}} 1' in text
    assert f'toro_jobs_total{{queue="{q.name}",outcome="cancelled"}} 1' in text
    assert f'toro_jobs_total{{queue="{q.name}",outcome="failed"}} 0' in text
    assert f'toro_queue_depth{{queue="{q.name}",state="wait"}} 1' in text
    # every state a job can be in, so a dashboard and a scraper agree
    for state in await q.counts():
        assert f'state="{state}"' in text
    assert text.endswith("# EOF\n")


async def test_lifetime_totals_are_readable_on_their_own(q):
    """A dashboard serving several queues needs the numbers, not one queue's rendered
    text: concatenating renders would declare every family twice. So the totals are
    readable without reaching into the queue's keys."""
    await q.add("j", {})

    totals = await q.lifetime_totals()

    assert totals["added"] == 1
    assert totals["completed"] == 0  # present at zero, like the exposition
    assert totals["failed"] == 0
    assert totals["cancelled"] == 0
