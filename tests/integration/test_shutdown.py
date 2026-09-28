"""Integration: shutdown must not abandon a job the worker has already claimed."""

import asyncio
import contextlib

import pytest

import toro.worker as worker_module
from toro import Worker
from toro.connection import connect

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
        assert await run_until(lambda: done == ["a", "b"]), f"`b` was dropped: {done}"

        async def active_empty() -> bool:
            return await q.redis.llen(q.keys.active) == 0

        assert await run_until(active_empty)
    finally:
        await w.stop(grace_period=1)
        task.cancel()


async def test_stop_before_run_starts_ends_it(q):
    """stop() awaited right after run() was scheduled, before it ran at all."""
    processed: list[str] = []
    w = Worker(q.name, lambda job: processed.append(job.id), prefix=PREFIX, stalled_interval=0)
    task = asyncio.create_task(w.run())
    try:
        await w.stop()
        # asyncio.wait, not wait_for: wait_for cancels run() on timeout, and run()
        # returns quietly from a cancellation, which would hide the bug it looks for
        done, _ = await asyncio.wait({task}, timeout=3)
        assert task in done, "run() carried on after stop()"
        await q.add("after-stop", {})
        await asyncio.sleep(0.3)  # nothing may claim it: asserting an absence
        assert processed == []
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.parametrize("own_pool", [True, False], ids=["own-pool", "caller-pool"])
async def test_stop_while_run_is_starting_ends_it(q, monkeypatch, own_pool):
    """A shutdown hook can fire while run() is still in its first round trips. stop()
    then found nothing to cancel and finished its cleanup, and run() carried on: the
    stopped worker registered itself again, subscribed, and claimed jobs for good."""
    release = asyncio.Event()
    real_stamp = worker_module.stamp_data_model

    async def held_stamp(*args, **kwargs):
        await release.wait()  # run() waits here until stop() has returned
        return await real_stamp(*args, **kwargs)

    monkeypatch.setattr(worker_module, "stamp_data_model", held_stamp)
    conn = None if own_pool else connect("redis://localhost:6379")
    processed: list[str] = []
    w = Worker(
        q.name,
        lambda job: processed.append(job.id),
        prefix=PREFIX,
        stalled_interval=0,
        connection=conn,
    )
    task = asyncio.create_task(w.run())
    try:
        await asyncio.sleep(0)  # run() is now held in its first round trip
        await w.stop()
        release.set()
        done, _ = await asyncio.wait({task}, timeout=3)  # not wait_for: see above
        assert task in done, "run() carried on after stop()"

        await q.add("after-stop", {})
        await asyncio.sleep(0.3)  # nothing may claim it: asserting an absence
        assert processed == []
        assert w.token not in [entry["id"] for entry in await q.workers()]
        assert await q.redis.pubsub_numsub(q.keys.cancel) == [(q.keys.cancel, 0)]
        if own_pool:  # what startup reopened is closed again
            assert not [c for c in w.redis.connection_pool._available_connections if c.is_connected]
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        if conn is not None:
            await conn.aclose()


async def test_a_stopped_worker_can_be_run_again(q, run_until):
    """stop() marks the worker stopped until its run() returns, and no longer: the
    same worker started again processes jobs as a fresh one would."""
    processed: list[str] = []
    w = Worker(q.name, lambda job: processed.append(job.id), prefix=PREFIX, stalled_interval=0)
    first = asyncio.create_task(w.run())
    assert await run_until(lambda: w._running)
    await w.stop()
    await asyncio.wait({first}, timeout=3)

    second = asyncio.create_task(w.run())
    try:
        job = await q.add("again", {})
        assert await run_until(lambda: processed == [job.id], timeout=5)
        mine = [entry for entry in await q.workers() if entry["id"] == w.token]
        assert [entry["state"] for entry in mine] == ["running"]  # not still draining
    finally:
        await w.stop()
        await asyncio.wait({second}, timeout=3)
        second.cancel()
