"""Integration: read-side API - counts, ordering, lookup, search, pause state."""


async def test_counts_reflect_each_state(q):
    await q.add("waiting", {})
    await q.add("later", {}, delay=60_000)

    counts = await q.counts()
    assert counts["wait"] == 1
    assert counts["delayed"] == 1
    assert counts["active"] == 0
    assert counts["completed"] == 0
    assert counts["failed"] == 0


async def test_get_jobs_returns_global_priority_order(q):
    await q.add("low", {}, priority=0)
    await q.add("high", {}, priority=10)
    await q.add("mid", {}, priority=5)

    names = [j.name for j in await q.get_jobs("wait")]
    assert names == ["high", "mid", "low"]  # most urgent first, not insertion order


async def test_get_job_returns_none_for_missing(q):
    assert await q.get_job("does-not-exist") is None


async def test_search_matches_name_and_data_and_misses_cleanly(q):
    await q.add("send-welcome", {"to": "ada@example.com"})
    await q.add("charge-card", {"customer": "cus_9"})

    assert [j.name for j in await q.search("wait", "welcome")] == ["send-welcome"]
    assert [j.name for j in await q.search("wait", "cus_9")] == ["charge-card"]
    assert await q.search("wait", "no-such-thing") == []  # a real miss, not everything


async def test_is_paused_reflects_pause_and_resume(q):
    assert await q.is_paused() is False
    await q.pause()
    assert await q.is_paused() is True
    await q.resume()
    assert await q.is_paused() is False
