"""Integration: the totals a scraper reads (docs/specs/operate.md).

The per-minute buckets self-expire after eight hours, which is right for charting
and useless for `rate()`: a counter that resets cannot answer "how many since
forever". These totals never expire, and are written in the same atomic step as the
transition they count, so a counter can never disagree with the state change.
"""

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
