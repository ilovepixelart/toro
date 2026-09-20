"""Integration: shutdown must not abandon a job the worker has already claimed."""

import asyncio

from toro import Worker

PREFIX = "torotest"


async def test_stop_during_a_finish_round_trip_keeps_the_claimed_job(q, run_until):
    """The fetch flag is read BEFORE the finish script runs. If stop() lands while
    that round trip is in flight, the script has already claimed the next job. It
    is in flight like any other and must be processed, not dropped: dropped, it
    sits locked in `active` until its lock expires and the sweep comes round."""
    await q.add("a", {})
    await q.add("b", {})
    done: list[str] = []

    async def proc(job):
        done.append(job.name)

    w = Worker(q.name, proc, prefix=PREFIX, stalled_interval=0)
    real_finish = w._finish_completed

    async def finish_then_stop(job, result):
        nxt = await real_finish(job, result)  # fetch was "1": the script claimed `b`
        w._running = False  # stop() lands while that round trip was in flight
        return nxt

    w._finish_completed = finish_then_stop
    task = asyncio.create_task(w.run())
    try:
        assert await run_until(lambda: "a" in done)
        await asyncio.sleep(0.3)  # room for the loop to either process `b` or drop it
        assert done == ["a", "b"]
        assert await q.redis.llen(q.keys.active) == 0
    finally:
        await w.stop(grace_period=1)
        task.cancel()
