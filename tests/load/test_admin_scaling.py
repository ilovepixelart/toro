"""@load — retry_all_failed must batch its per-job RETRY_JOB scripts through
one pipeline (the clean() pattern), not pay one round trip per job.

Compares retry_all_failed() against the same retries issued serially via
retry_job() — the batched bulk API has to beat the per-job loop by a wide
margin, at every size. Baseline recorded on local Redis 7.4: serial ~6.5k
jobs/s (one ~0.16ms round trip each), pipelined ~110k jobs/s (~17x).
"""

import time

from toro.queue import Queue


async def _wipe(q: Queue) -> None:
    keys = await q.redis.keys(q.keys.base + "*")
    if keys:
        await q.redis.delete(*keys)


async def _seed_failed(q: Queue, n: int) -> None:
    """Plant n terminally-failed jobs directly (hash + `failed` ZSET entry),
    mirroring what MOVE_TO_FAILED writes — milliseconds instead of running
    n real jobs through a worker.
    """
    now = int(time.time() * 1000)
    pipe = q.redis.pipeline(transaction=False)
    for i in range(n):
        jid = f"pf{i}"
        pipe.hset(
            q.keys.job(jid),
            mapping={
                "id": jid,
                "name": "bench",
                "data": "{}",
                "opts": "{}",
                "timestamp": now,
                "attemptsMade": 1,
                "priority": 0,
                "state": "failed",
                "failedReason": "x",
                "finishedOn": now,
            },
        )
        pipe.zadd(q.keys.failed, {jid: now})
        if i % 5000 == 4999:  # keep pipeline buffers bounded at volume
            await pipe.execute()
            pipe = q.redis.pipeline(transaction=False)
    await pipe.execute()


async def test_retry_all_failed_beats_a_serial_per_job_loop(q, load_scale):
    sizes = [int(s * load_scale) for s in (500, 2000, 5000)]
    ratios: dict[int, float] = {}

    print("\n--- retry_all_failed (bulk) vs serial retry_job loop ---")
    for n in sizes:
        await _wipe(q)
        await _seed_failed(q, n)
        ids = await q.redis.zrange(q.keys.failed, 0, n - 1)
        t0 = time.perf_counter()
        for jid in ids:
            assert await q.retry_job(jid)
        serial = time.perf_counter() - t0
        assert (await q.counts())["wait"] == n  # all really moved to prioritized

        await _wipe(q)
        await _seed_failed(q, n)
        t0 = time.perf_counter()
        retried = await q.retry_all_failed(limit=n)
        bulk = time.perf_counter() - t0
        assert retried == n
        assert (await q.counts())["wait"] == n

        ratios[n] = serial / bulk
        print(
            f" n={n:>5}: serial loop {serial * 1000:>8.1f}ms ({n / serial:>7,.0f} jobs/s)   "
            f"bulk {bulk * 1000:>7.1f}ms ({n / bulk:>8,.0f} jobs/s)   speedup x{ratios[n]:.1f}"
        )

    # The bulk API must remove the per-job round trip — far faster than the
    # serial loop, and increasingly so as n grows.
    assert ratios[sizes[-1]] > 2.0, f"retry_all_failed is not batching: {ratios}"
