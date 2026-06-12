"""@load - the cost of removing a finished job from the `active` list
(`LREM count=0`, an O(len) full-list scan on every completion).

Measured verdict (recorded 2026-06, local Redis 7.4): the asymptotic claim is
true but the constant is small - the scan costs ~4-5µs per 1k active entries,
invisible under the ~0.2ms round trip until `active` reaches tens of thousands
of in-flight jobs. The tests below amplify to 100k entries so the scan
dominates the RTT and the slope is measurable.

(a) MOVE_TO_COMPLETED latency vs active list size - characterizes the slope.
(b) LREM count 0 / 1 / -1 for a tail-positioned element (jobs are LPUSHed at
    the head, so the oldest - most likely to finish - sit near the tail).
    Decision on the data: toro KEEPS count=0. The direction trick saves only
    microseconds under the ~0.2ms round trip at realistic active sizes, while
    count=0 removes every occurrence - self-healing if a bug ever double-inserts.
"""

import statistics
import time
import uuid

from toro import scripts
from toro.queue import Queue


async def _wipe(q: Queue) -> None:
    keys = await q.redis.keys(q.keys.base + "*")
    if keys:
        await q.redis.delete(*keys)


async def _pad_active(q: Queue, pad: int) -> None:
    """Synthetic in-flight jobs from "other workers", chunked LPUSH."""
    for i in range(0, pad, 10_000):
        await q.redis.lpush(q.keys.active, *[f"ghost{j}" for j in range(i, min(i + 10_000, pad))])


async def test_finish_cost_grows_with_active_list_size(q, load_scale):
    print("\n--- MOVE_TO_COMPLETED latency vs active list size ---")
    token = uuid.uuid4().hex
    acquire = q.redis.register_script(scripts.MOVE_TO_ACTIVE)
    complete = q.redis.register_script(scripts.MOVE_TO_COMPLETED)
    medians: dict[int, float] = {}

    pads = (0, int(10_000 * load_scale), int(100_000 * load_scale))
    for pad in pads:
        await _wipe(q)
        await _pad_active(q, pad)
        samples = []
        for i in range(50):
            job = await q.add("bench", {"i": i})
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
                args=[token, 30_000, int(time.time() * 1000), 0, 0],
            )
            assert res and res[1] == job.id
            t0 = time.perf_counter()
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
                args=[
                    job.id,
                    "null",
                    int(time.time() * 1000),
                    token,
                    "0",
                    30_000,
                    -1,
                    -1,
                    0,
                    0,
                    60_000,
                ],
            )
            samples.append((time.perf_counter() - t0) * 1000)
            assert out == [1]  # committed, not lock-lost
        assert await q.redis.llen(q.keys.active) == pad  # ghosts untouched, job removed
        medians[pad] = statistics.median(samples)
        print(f" active≈{pad:>7}: finish median {medians[pad]:.3f}ms  (50 reps)")

    slope_us_per_1k = (medians[pads[-1]] - medians[0]) * 1000 / (pads[-1] / 1000)
    print(f" slope ≈ {slope_us_per_1k:.1f}µs per 1k active entries")
    # Shape: at 100k+ in-flight entries the O(A) scan must dominate the RTT.
    assert medians[pads[-1]] > medians[0] * 1.5, f"finish cost did not grow with A: {medians}"


async def test_lrem_direction_microbench(q):
    print("\n--- LREM 0 vs 1 vs -1, target near the tail (len=100k) ---")
    key = q.keys.base + "lrembench"
    n = 100_000
    results: dict[int, float] = {}
    for count in (0, 1, -1):
        samples = []
        for _ in range(15):
            await q.redis.delete(key)
            for i in range(0, n, 10_000):
                await q.redis.rpush(key, *[f"g{j}" for j in range(i, min(i + 10_000, n))])
            await q.redis.rpush(key, "target", *[f"t{j}" for j in range(10)])  # ~tail
            t0 = time.perf_counter()
            removed = await q.redis.lrem(key, count, "target")
            samples.append((time.perf_counter() - t0) * 1000)
            assert removed == 1
        results[count] = statistics.median(samples)
        print(f" LREM count={count:>2}: median {results[count]:.3f}ms")
    await q.redis.delete(key)

    # The decision datum: scanning from the tail must beat the full scan for a
    # tail-positioned (oldest, FIFO-finishing) job once the list is large.
    assert results[-1] < results[0], f"expected LREM -1 < LREM 0: {results}"
