"""@load - a claim must promote due delayed jobs in bounded chunks (LIMIT), so a
big backlog coming due at once (e.g. a backoff storm after an outage) never blocks
Redis for the whole backlog inside one claim.

Seeds M delayed jobs all due now, drains `delayed` with repeated claims, and
samples PING latency from a second connection throughout: no single claim - and
no observed stall - may approach the duration of the whole drain. Baseline
recorded on local Redis 7.4 before chunking: one call swept all M jobs and an
independent client's PING stalled for the full sweep (M=50k: 131ms, ~2.6µs/job).
"""

import asyncio
import math
import time

import redis.asyncio as aioredis

from toro import Worker, scripts
from toro.queue import Queue

PREFIX = "torotest"


async def _noop(job):
    return None


async def _wipe(q: Queue) -> None:
    keys = await q.redis.keys(q.keys.base + "*")
    if keys:
        await q.redis.delete(*keys)


async def _seed_delayed_due(q: Queue, m: int) -> None:
    """Plant m delayed jobs whose time has already come (hash + `delayed` ZSET),
    mirroring what ADD_JOB writes for a delayed add.
    """
    due = int(time.time() * 1000) - 1000
    pipe = q.redis.pipeline(transaction=False)
    for i in range(m):
        jid = f"pd{i}"
        pipe.hset(
            q.keys.job(jid),
            mapping={
                "id": jid,
                "name": "bench",
                "data": "{}",
                "opts": "{}",
                "timestamp": due,
                "attemptsMade": 0,
                "priority": 0,
                "state": "delayed",
                "delay": 1000,
            },
        )
        pipe.zadd(q.keys.delayed, {jid: due})
        if i % 5000 == 4999:  # keep pipeline buffers bounded
            await pipe.execute()
            pipe = q.redis.pipeline(transaction=False)
    await pipe.execute()


async def _ping_sampler(url: str, stop: asyncio.Event) -> list[float]:
    """Sample PING latency (ms) from an independent connection until stopped."""
    r = aioredis.from_url(url, decode_responses=True)
    stalls: list[float] = []
    try:
        while not stop.is_set():
            t0 = time.perf_counter()
            await r.ping()
            stalls.append((time.perf_counter() - t0) * 1000)
            await asyncio.sleep(0)  # stay hot - we WANT to observe any block
    finally:
        await r.aclose()
    return stalls


async def test_a_claim_promotes_due_jobs_in_bounded_chunks(q, load_scale):
    print("\n--- claim-path promotion, chunked drain (must not block Redis) ---")
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    for m in (int(10_000 * load_scale), int(50_000 * load_scale)):
        await _wipe(q)
        await _seed_delayed_due(q, m)

        stop = asyncio.Event()
        sampler = asyncio.create_task(_ping_sampler("redis://localhost:6379", stop))
        await asyncio.sleep(0.05)  # let the sampler establish a baseline
        per_call: list[float] = []
        while await q.redis.zcard(q.keys.delayed):
            t0 = time.perf_counter()
            assert await w._acquire() is not None  # promotes a batch, takes one
            per_call.append((time.perf_counter() - t0) * 1000)
        await asyncio.sleep(0.05)
        stop.set()
        stalls = await sampler

        assert len(per_call) == math.ceil(m / scripts.PROMOTE_BATCH)
        counts = await q.counts()
        assert counts["wait"] + counts["active"] == m  # every due job was promoted
        total_ms = sum(per_call)
        print(
            f" M={m:>6}: drained in {len(per_call)} claims, {total_ms:>7.1f}ms total   "
            f"max claim {max(per_call):>6.1f}ms   PING max {max(stalls):>6.1f}ms"
        )
        # Chunking: neither a single claim nor an independent client's stall may
        # approach the duration of the whole drain.
        assert max(per_call) < total_ms * 0.5, f"one claim swept ~everything: {per_call[:3]}"
        assert max(stalls) < total_ms * 0.5, f"Redis blocked ~the whole drain: {max(stalls):.1f}ms"
