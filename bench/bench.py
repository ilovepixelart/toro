"""Measure toro's hot path: enqueue + process throughput and Redis cmds/job.

Compares against the pre-fetch-next baseline we recorded (~6,080 jobs/s,
13 cmds/job) to show the round-trip savings.

Usage:  uv run python bench/bench.py [N] [concurrency]
"""

import asyncio
import sys
import time

import redis.asyncio as aioredis

from toro import Queue, Worker

URL = "redis://localhost:6379"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
CONC = int(sys.argv[2]) if len(sys.argv) > 2 else 20


async def _clear(r, base):
    keys = await r.keys(base + "*")
    if keys:
        await r.delete(*keys)


async def _cmds(r):
    return (await r.info("stats"))["total_commands_processed"]


async def main():
    r = aioredis.from_url(URL, decode_responses=True)
    await _clear(r, "bench:b:")
    q = Queue("b", url=URL, prefix="bench")

    c0 = await _cmds(r)
    t0 = time.perf_counter()
    for i in range(N):
        await q.add("bench", {"i": i})
    enq = time.perf_counter() - t0
    enq_cmds = await _cmds(r) - c0

    done = asyncio.Event()
    count = 0

    async def handler(job):
        nonlocal count
        count += 1
        if count >= N:
            done.set()

    worker = Worker("b", handler, url=URL, prefix="bench", concurrency=CONC)
    c1 = await _cmds(r)
    t1 = time.perf_counter()
    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(done.wait(), timeout=120)
    proc = time.perf_counter() - t1
    proc_cmds = await _cmds(r) - c1

    await worker.stop()
    task.cancel()
    await q.close()
    await r.aclose()

    print(f"N={N}, concurrency={CONC}\n")
    print(f"enqueue:  {N / enq:>8,.0f} jobs/s   {enq_cmds / N:>5.1f} cmds/job")
    print(f"process:  {count / proc:>8,.0f} jobs/s   {proc_cmds / count:>5.1f} cmds/job")
    print("\nbaseline (pre fetch-next): ~6,080 jobs/s, 13.0 cmds/job")


if __name__ == "__main__":
    asyncio.run(main())
