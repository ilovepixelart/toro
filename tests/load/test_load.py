"""@load smoke test — a short OPEN-LOOP run that proves toro keeps up under steady
arrival without dropping jobs and without latency running away.

Automated guardrail (opt-in: `pytest -m load`); the full sweep lives in harness.py.
We assert behaviour, not a vanity number:
  * every enqueued job completes (no drops / losses),
  * zero errors,
  * p99 end-to-end latency stays bounded (a queue that fell behind would balloon it).

Latency is recorded straight from the in-process processor (a closure) — reliable
and zero-overhead, no pub/sub timing games.
"""

import asyncio
import time


async def test_open_loop_keeps_up_without_drops_or_runaway_latency(q, run_worker, run_until):
    N = 400
    rate = 200.0  # jobs/s — modest, well under saturation
    interval = 1.0 / rate
    e2e_ms: list[float] = []

    async def proc(job):
        e2e_ms.append(time.time() * 1000 - job.data["_enq"])  # end-to-end, in ms

    async with run_worker(q, proc, concurrency=32):
        # OPEN-LOOP producer: emit on a fixed schedule, never gated on completions.
        nxt = time.time()
        for _ in range(N):
            await q.add("bench", {"_enq": time.time() * 1000})
            nxt += interval
            await asyncio.sleep(max(0.0, nxt - time.time()))
        completed = await run_until(lambda: len(e2e_ms) >= N, timeout=10.0)

    # (a) no drops — every enqueued job ran exactly once; (b) none failed.
    assert completed, f"only {len(e2e_ms)}/{N} completed — dropped jobs?"
    assert len(e2e_ms) == N
    assert (await q.counts())["failed"] == 0

    # (c) latency stayed bounded — runaway queueing would push p99 into seconds.
    e2e_ms.sort()
    p99 = e2e_ms[min(len(e2e_ms) - 1, int(len(e2e_ms) * 0.99))]
    assert p99 < 1000, f"p99 end-to-end {p99:.0f}ms — the queue fell behind"
