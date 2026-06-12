"""@load - Queue.add() must enqueue in ONE round trip: the "added" event is
published from inside the ADD_JOB script, not as a separate awaited PUBLISH.

Compares single-coroutine add() throughput against the bare ADD_JOB script
issued directly with identical args - add() may only cost client-side
serialization on top of it. Baseline recorded on local Redis 7.4 when the
publish was a second round trip: add() 2.9k jobs/s vs bare script 5.8k (the
publish was 49% of enqueue latency).
"""

import json
import time

from toro import scripts
from toro.queue import Queue


async def _wipe(q: Queue) -> None:
    keys = await q.redis.keys(q.keys.base + "*")
    if keys:
        await q.redis.delete(*keys)


async def test_add_costs_a_single_round_trip(q, load_scale):
    n = int(2000 * load_scale)

    await _wipe(q)
    t0 = time.perf_counter()
    for i in range(n):
        await q.add("bench", {"i": i})
    full = time.perf_counter() - t0
    assert (await q.counts())["wait"] == n

    # The floor: the raw script with no Python wrapping at all.
    await _wipe(q)
    add_job = q.redis.register_script(scripts.ADD_JOB)
    opts = json.dumps({"delay": 0, "attempts": 1, "priority": 0})
    keys = [
        q.keys.id,
        q.keys.prioritized,
        q.keys.marker,
        q.keys.delayed,
        q.keys.base,
        q.keys.pc,
        q.keys.events,
    ]
    now = int(time.time() * 1000)
    t0 = time.perf_counter()
    for i in range(n):
        await add_job(
            keys=keys, args=["bench", json.dumps({"i": i}), opts, now, 0, 0, "", "", 0, 60_000]
        )
    bare = time.perf_counter() - t0
    assert (await q.counts())["wait"] == n

    print("\n--- add() vs bare ADD_JOB script (both must be one round trip) ---")
    print(
        f" add():       {n / full:>8,.0f} jobs/s  ({full / n * 1000:.3f}ms/job)\n"
        f" bare script: {n / bare:>8,.0f} jobs/s  ({bare / n * 1000:.3f}ms/job)\n"
        f" add() overhead over the script ≈ {(full - bare) / bare:.0%}"
    )
    # One round trip each: add() may only add client-side overhead, never
    # another network round trip (which would show as ~2x here).
    assert full < bare * 1.3, (
        f"add() costs more than one round trip: {full / n * 1000:.3f}ms/job "
        f"vs bare {bare / n * 1000:.3f}ms/job"
    )
