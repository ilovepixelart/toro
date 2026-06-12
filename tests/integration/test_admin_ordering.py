"""Ordering consistency across the read and admin APIs.

get_jobs() pages finished states newest-first; search() and retry_all_failed()
must look at the SAME end of the set (what the user sees), while clean() prunes
oldest-first (drop old history before recent results) - each documented.
"""

import time

from toro.queue import Queue


async def _seed_finished(q: Queue, state: str, n: int) -> None:
    """n finished jobs j0..j{n-1}, j0 the OLDEST (lowest finish-time score)."""
    now = int(time.time() * 1000) - n * 1000
    zset = getattr(q.keys, state)
    pipe = q.redis.pipeline(transaction=False)
    for i in range(n):
        jid = f"j{i}"
        pipe.hset(
            q.keys.job(jid),
            mapping={
                "id": jid,
                "name": f"name-{jid}-end",
                "data": "{}",
                "opts": "{}",
                "timestamp": now + i * 1000,
                "attemptsMade": 1,
                "priority": 0,
                "state": state,
                "finishedOn": now + i * 1000,
                **({"failedReason": "x"} if state == "failed" else {}),
            },
        )
        pipe.zadd(zset, {jid: now + i * 1000})
        if i % 5000 == 4999:
            await pipe.execute()
            pipe = q.redis.pipeline(transaction=False)
    await pipe.execute()


async def test_search_scans_the_newest_finished_jobs(q):
    await _seed_finished(q, "completed", 600)
    # The newest job is inside the 500-job scan window; the oldest is not -
    # matching what get_jobs() (and the dashboard) shows as "recent".
    assert await q.search("completed", "name-j599-end", scan_limit=500)
    assert not await q.search("completed", "name-j0-end", scan_limit=500)


async def test_retry_all_failed_retries_the_newest_when_truncated(q):
    await _seed_finished(q, "failed", 1200)
    retried = await q.retry_all_failed(limit=1000)
    assert retried == 1000
    # The leftovers are the OLDEST 200 - the visible/recent failures got retried.
    left = await q.redis.zrange(q.keys.failed, 0, -1)
    assert len(left) == 200
    assert set(left) == {f"j{i}" for i in range(200)}


async def test_clean_prunes_the_oldest_first(q):
    await _seed_finished(q, "completed", 1500)
    removed = await q.clean("completed", limit=1000)
    assert removed == 1000
    # What survives is the NEWEST 500 - clean drops old history, not fresh results.
    left = await q.redis.zrange(q.keys.completed, 0, -1)
    assert set(left) == {f"j{i}" for i in range(1000, 1500)}
