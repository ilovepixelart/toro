"""@load - PROMOTE_DELAYED must promote due jobs in bounded chunks (LIMIT), so
a big backlog coming due at once (e.g. a backoff storm after an outage) never
blocks Redis for the whole sweep.

Seeds M delayed jobs all due now, drains them with repeated promote calls, and
samples PING latency from a second connection throughout: no single call - and
no observed stall - may approach the duration of the whole drain. Baseline
recorded on local Redis 7.4 before chunking: one call swept all M jobs and an
independent client's PING stalled for the full sweep (M=50k: 131ms, ~2.6µs/job).
"""

import asyncio
import time

import redis.asyncio as aioredis

from toro import scripts
from toro.queue import Queue


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


async def test_promote_delayed_drains_in_bounded_chunks(q, load_scale):
    print("\n--- PROMOTE_DELAYED chunked drain (must not block Redis) ---")
    promote = q.redis.register_script(scripts.PROMOTE_DELAYED)
    for m in (int(10_000 * load_scale), int(50_000 * load_scale)):
        await _wipe(q)
        await _seed_delayed_due(q, m)

        stop = asyncio.Event()
        sampler = asyncio.create_task(_ping_sampler("redis://localhost:6379", stop))
        await asyncio.sleep(0.05)  # let the sampler establish a baseline
        per_call: list[float] = []
        total = 0
        while True:
            t0 = time.perf_counter()
            n = await promote(
                keys=[q.keys.delayed, q.keys.prioritized, q.keys.marker, q.keys.base, q.keys.pc],
                args=[int(time.time() * 1000), scripts.PROMOTE_BATCH],
            )
            per_call.append((time.perf_counter() - t0) * 1000)
            total += n
            if n < scripts.PROMOTE_BATCH:
                break
        await asyncio.sleep(0.05)
        stop.set()
        stalls = await sampler

        assert total == m
        assert (await q.counts())["wait"] == m  # every due job landed in prioritized
        total_ms = sum(per_call)
        print(
            f" M={m:>6}: drained in {len(per_call)} calls, {total_ms:>7.1f}ms total   "
            f"max call {max(per_call):>6.1f}ms   PING max {max(stalls):>6.1f}ms"
        )
        # Chunking: neither a single promote call nor an independent client's
        # stall may approach the duration of the whole drain.
        assert max(per_call) < total_ms * 0.5, f"one call swept ~everything: {per_call[:3]}"
        assert max(stalls) < total_ms * 0.5, f"Redis blocked ~the whole drain: {max(stalls):.1f}ms"
