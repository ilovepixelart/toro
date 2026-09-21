"""Auto-removal retention edges: scheduler input validation and the bounded
age-trim - enabling `remove_on_complete={"age": ...}` on a queue with a deep
finished backlog must not sweep it all in one Redis-blocking call.
"""

import time
import uuid

import pytest

from toro import scripts
from toro.queue import Queue


async def test_add_scheduler_rejects_non_positive_every(q):
    with pytest.raises(ValueError, match="positive"):
        await q.add_scheduler("bad", every=0)
    with pytest.raises(ValueError, match="positive"):
        await q.add_scheduler("bad", every=-5000)


async def _seed_old_completed(q: Queue, n: int) -> None:
    old = int(time.time() * 1000) - 86_400_000  # finished a day ago
    pipe = q.redis.pipeline(transaction=False)
    for i in range(n):
        jid = f"old{i}"
        pipe.hset(q.keys.job(jid), mapping={"id": jid, "name": "bench", "state": "completed"})
        pipe.zadd(q.keys.completed, {jid: old + i})
        if i % 5000 == 4999:
            await pipe.execute()
            pipe = q.redis.pipeline(transaction=False)
    await pipe.execute()


async def _finish_one(q: Queue, keep_count: int, keep_age: int) -> None:
    """One real claim and completion, straight through the scripts, under the
    given retention (the ARGV pair `JobOptions.keep_args` produces)."""
    token = uuid.uuid4().hex
    acquire = q.redis.register_script(scripts.MOVE_TO_ACTIVE)
    complete = q.redis.register_script(scripts.MOVE_TO_COMPLETED)
    job = await q.add("bench", {})
    now = int(time.time() * 1000)
    res = await acquire(
        keys=[
            q.keys.prioritized,
            q.keys.active,
            q.keys.marker,
            q.keys.stalled,
            q.keys.base,
            q.keys.pc,
            q.keys.meta_paused,
            q.keys.limiter,
        ],
        args=[token, 30_000, now, 0, 0, 0],
    )
    assert res and res[1] == job.id
    out = await complete(
        keys=[
            q.keys.active,
            q.keys.completed,
            q.keys.job(job.id),
            q.keys.lock(job.id),
            q.keys.prioritized,
            q.keys.marker,
            q.keys.stalled,
            q.keys.base,
            q.keys.pc,
            q.keys.events,
            q.keys.meta_paused,
            q.keys.limiter,
        ],
        args=[job.id, "null", now, token, "0", 30_000, keep_count, keep_age, 0, 0, 60_000],
    )
    assert out == [1]


async def test_age_trim_is_bounded_per_finish(q):
    n = 2500
    await _seed_old_completed(q, n)

    # keepAge=1h: every seeded entry is expired, but a single finish may only
    # trim a bounded slice of them.
    await _finish_one(q, keep_count=-1, keep_age=3600)

    # Exactly one bounded slice trimmed (oldest first), the rest left for the
    # next finishes to amortize - plus the job that just completed.
    remaining = await q.redis.zcard(q.keys.completed)
    assert remaining == n - 1000 + 1, f"trim not bounded: {remaining} left of {n}"
    # The trimmed slice was the oldest - its hashes are gone, newer ones remain.
    assert not await q.redis.exists(q.keys.job("old0"))
    assert await q.redis.exists(q.keys.job(f"old{n - 1}"))


async def test_count_trim_is_bounded_per_finish(q):
    """BR-005: a count bound met by a deep backlog drains it a slice per finish."""
    n, keep = 2500, 10
    await _seed_old_completed(q, n)

    await _finish_one(q, keep_count=keep, keep_age=-1)

    # One slice, oldest first: the 1000 oldest are gone with their hashes, and
    # everything newer is untouched, the job that just finished included.
    assert await q.redis.zcard(q.keys.completed) == n - 1000 + 1
    assert not await q.redis.exists(q.keys.job("old0"))
    assert not await q.redis.exists(q.keys.job("old999"))
    assert await q.redis.exists(q.keys.job("old1000"))

    # The backlog drains over the following finishes and settles AT the bound.
    sizes = []
    for _ in range(4):
        await _finish_one(q, keep_count=keep, keep_age=-1)
        sizes.append(await q.redis.zcard(q.keys.completed))
    assert sizes == [502, keep, keep, keep]
    # What is left is the newest by finish time: the five jobs finished here and
    # the five newest of the backlog, hashes intact; the next oldest is gone.
    kept = await q.redis.zrange(q.keys.completed, 0, -1)
    assert kept[:5] == [f"old{i}" for i in range(n - 5, n)]
    assert all(not jid.startswith("old") for jid in kept[5:])
    assert all([await q.redis.exists(q.keys.job(jid)) for jid in kept])
    assert not await q.redis.exists(q.keys.job(f"old{n - 6}"))
