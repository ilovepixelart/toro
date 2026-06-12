"""@load - a worker's effective concurrency must not be silently capped by the
connection pool.

The concern: each parked process loop holds a pool connection inside BZPOPMIN,
and the BlockingConnectionPool defaults to 50 connections - so concurrency=100
might quietly run at 50. The counter-hypothesis: a loop only holds a connection
while PARKED or issuing a command, not while its job runs, so under real load
the pool rotates and full concurrency is reached. This measures which is true:
deep backlog, slow jobs, concurrency 2x the pool size - peak simultaneous
in-flight jobs must approach the configured concurrency.
"""

import asyncio


async def test_worker_reaches_configured_concurrency_past_the_pool_size(
    q, run_worker, run_until, load_scale
):
    n, concurrency = int(120 * load_scale), 100
    pool_max = q.redis.connection_pool.max_connections
    assert concurrency > pool_max, "test needs concurrency above the pool size to mean anything"

    for i in range(n):  # deep backlog BEFORE the worker starts
        await q.add("bench", {"i": i})

    done = 0
    peak = 0

    async def proc(job):
        nonlocal done
        await asyncio.sleep(0.5)
        done += 1

    async with run_worker(q, proc, concurrency=concurrency, stalled_interval=0) as w:
        worker_pool = w.redis.connection_pool.max_connections

        async def sample() -> bool:
            nonlocal peak
            peak = max(peak, len(w._current))
            return done >= n

        # Time budget: waves of `concurrency` 0.5s jobs, with generous slack.
        budget = max(20.0, (n / concurrency) * 0.5 * 3)
        assert await run_until(sample, timeout=budget, interval=0.02), f"only {done}/{n} ran"

    print(
        f"\n--- worker concurrency vs pool ---\n"
        f" default pool={pool_max}, worker pool sized to {worker_pool} for "
        f"concurrency={concurrency}: peak in-flight {peak}, all {n} jobs completed"
    )
    # No silent cap: in-flight work must climb well past the pool size.
    assert peak > pool_max * 1.4, f"concurrency silently capped near the pool size: peak={peak}"
