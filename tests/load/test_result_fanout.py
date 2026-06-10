"""@load — result() waiters must share ONE events subscription (a per-Queue
dispatcher that routes each event to the future waiting on that jobId), not
one pubsub connection per waiter.

The shared channel inherently delivers every event to every subscriber, so a
per-waiter design costs waiters x events client work AND a hard ceiling: each
waiter holds ~2 pool connections, so waiters past the pool limit fail outright.
Baseline recorded on local Redis 7.4 with per-waiter pubsubs (redis-py 8 pool
max=100): client CPU 26ms -> 488ms from W=1 to W=50, +95 connections at W=50,
and only 50 of 120 concurrent waiters resolved (70 MaxConnectionsError).

Publishes E synthetic "completed" events for foreign jobs while W waiters wait
on jobs that never finish, then releases them.
"""

import asyncio
import json
import time

from toro.queue import Queue


async def _connected_clients(q: Queue) -> int:
    return int((await q.redis.info("clients"))["connected_clients"])


async def _publish_events(q: Queue, e: int) -> None:
    pipe = q.redis.pipeline(transaction=False)
    for i in range(e):
        pipe.publish(
            q.keys.events,
            json.dumps({"jobId": f"other{i}", "event": "completed", "result": None}),
        )
        if i % 5000 == 4999:  # keep pipeline buffers bounded at volume
            await pipe.execute()
            pipe = q.redis.pipeline(transaction=False)
    await pipe.execute()


async def _run_window(q: Queue, w: int, events: int) -> tuple[float, int, int]:
    """W waiters on never-finishing jobs while `events` foreign events fire.
    Returns (client CPU seconds in the window, connection delta, events-channel
    subscriber count while the waiters were parked).
    """
    before = await _connected_clients(q)
    waiters = [asyncio.create_task(q.result(f"ghost{i}", timeout=30.0)) for i in range(w)]
    await asyncio.sleep(0.3)  # let every waiter subscribe
    during = await _connected_clients(q)
    subs = int((await q.redis.pubsub_numsub(q.keys.events))[0][1])

    cpu0 = time.process_time()
    await _publish_events(q, events)
    await asyncio.sleep(1.0)  # window in which the events get chewed through
    cpu = time.process_time() - cpu0

    # Release the waiters: publish each ghost job's "completed" event.
    pipe = q.redis.pipeline(transaction=False)
    for i in range(w):
        pipe.publish(
            q.keys.events,
            json.dumps({"jobId": f"ghost{i}", "event": "completed", "result": "ok"}),
        )
    await pipe.execute()
    results = await asyncio.gather(*waiters, return_exceptions=True)
    assert all(r == "ok" for r in results), "waiters did not all resolve"
    return cpu, during - before, subs


async def test_result_waiters_share_one_subscription(q, load_scale):
    events = int(1000 * load_scale)
    print("\n--- result() cost at W=1 vs W=50 (shared dispatcher) ---")
    cpu1, conns1, subs1 = await _run_window(q, 1, events)
    cpu50, conns50, subs50 = await _run_window(q, 50, events)
    print(
        f" W=1:  client CPU {cpu1 * 1000:>6.1f}ms   +{conns1} connections   "
        f"{subs1} channel subscriber(s)\n"
        f" W=50: client CPU {cpu50 * 1000:>6.1f}ms   +{conns50} connections   "
        f"{subs50} channel subscriber(s)\n"
        f" (per-waiter pubsubs would deliver WxE = 50x{events} = {50 * events:,} messages)"
    )
    # One shared subscription, no matter how many waiters...
    assert subs50 <= 2, f"waiters hold their own subscriptions: {subs50} at W=50"
    # ...so each event is parsed once and client work scales with E, not WxE.
    assert cpu50 < cpu1 * 3, (
        f"client CPU scales with waiters: {cpu1 * 1000:.0f}ms -> {cpu50 * 1000:.0f}ms"
    )


async def test_concurrent_waiters_are_not_capped_by_the_connection_pool(q):
    # With per-waiter pubsubs the pool size was a hard ceiling on concurrent
    # result() calls (waiters past it raised MaxConnectionsError). The shared
    # subscription removes the ceiling: all of them must resolve.
    pool_max = q.redis.connection_pool.max_connections
    w = pool_max + 20
    waiters = [asyncio.create_task(q.result(f"ghost{i}", timeout=10.0)) for i in range(w)]
    await asyncio.sleep(0.5)
    pipe = q.redis.pipeline(transaction=False)
    for i in range(w):
        pipe.publish(
            q.keys.events,
            json.dumps({"jobId": f"ghost{i}", "event": "completed", "result": "ok"}),
        )
    await pipe.execute()
    results = await asyncio.gather(*waiters, return_exceptions=True)
    ok = sum(1 for r in results if r == "ok")
    failed = [r for r in results if isinstance(r, Exception)]
    print(
        f"\n--- result() concurrency vs pool limit ---\n"
        f" pool max={pool_max}: {ok}/{w} concurrent waiters resolved, {len(failed)} failed"
    )
    assert not failed, f"waiters failed at the pool ceiling: {failed[0]!r}"
    assert ok == w
