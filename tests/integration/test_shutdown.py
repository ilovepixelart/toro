"""Integration: shutdown must not abandon a job the worker has already claimed."""

import asyncio
import contextlib
import threading
import time

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
        # returns quietly from a cancellation, which would hide the bug it looks for.
        # The bound only keeps a run() that carries on from hanging the test; it is
        # not a speed claim, and a loaded CI runner has spent 3 s on the shutdown.
        done, _ = await asyncio.wait({task}, timeout=10)
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
        done, _ = await asyncio.wait({task}, timeout=10)  # not wait_for: see above
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


async def test_cancelling_run_directly_ends_the_worker(q):
    """A framework that cancels its tasks on shutdown, or Ctrl-C under asyncio.run(),
    cancels run() without stop(). The job in flight goes back to the queue, as one cut
    off past stop()'s grace period does. On Python 3.10, which cannot tell a task's own
    cancellation from one it asked for, the worker instead failed the job with a reason
    of its own and went on claiming, so the cancellation never landed."""
    started = asyncio.Event()

    async def proc(job):
        started.set()
        await asyncio.sleep(30)

    w = Worker(q.name, proc, prefix=PREFIX, stalled_interval=0, grace_period=0)
    task = asyncio.create_task(w.run())
    try:
        job = await q.add("cut-off", {})
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=10)  # not wait_for: see above
        assert task in done, "run() carried on after being cancelled"
        assert (await q.get_job(job.id)).state == "wait"  # handed back, not failed
    finally:
        # stop() first: it lowers the flag and cancels the loops whatever run() did, so
        # a run() that absorbed its cancellation can still end, and the wait is bounded.
        await w.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)


async def test_a_job_cut_off_past_the_grace_period_goes_straight_back_to_the_queue(q, run_until):
    """A job whose processor stop() had to cancel sat locked in `active` until the
    lock expired and a sweep recovered it, 60 to 90 s later, and that recovery spent
    one of its stalls: at max_stalled_count=1 a job cut off by two deploys in a row was
    failed for good. It now goes back to `wait` at once, nothing counted."""
    started = asyncio.Event()

    async def proc(job):
        started.set()
        await asyncio.sleep(30)

    job = await q.add("long", {})
    w = Worker(q.name, proc, prefix=PREFIX, stalled_interval=0, grace_period=0.2)
    task = asyncio.create_task(w.run())
    try:
        await asyncio.wait_for(started.wait(), 5)
        await w.stop()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    fields = await q.redis.hgetall(q.keys.job(job.id))
    assert fields["state"] == "wait"
    assert "stalledCounter" not in fields and fields.get("attemptsMade", "0") == "0"
    assert await q.redis.lrange(q.keys.active, 0, -1) == []
    assert await q.redis.exists(q.keys.lock(job.id)) == 0
    assert await q.redis.zscore(q.keys.prioritized, job.id) is not None  # claimable at once

    done: list[str] = []
    again = Worker(q.name, lambda j: done.append(j.id), prefix=PREFIX, stalled_interval=0)
    task = asyncio.create_task(again.run())
    try:
        assert await run_until(lambda: done == [job.id], timeout=5)
    finally:
        await again.stop()
        task.cancel()


async def test_a_sync_job_cut_off_past_the_grace_period_keeps_its_lock(q, run_until):
    """A thread cannot be taken back: the job stays claimed and locked while it runs
    on, and the sweep recovers it once the lock lapses, as before."""
    started = threading.Event()

    def proc(job):
        started.set()
        time.sleep(1.5)

    job = await q.add("blocking", {})
    w = Worker(q.name, proc, prefix=PREFIX, stalled_interval=0, grace_period=0.2)
    task = asyncio.create_task(w.run())
    try:
        assert await asyncio.to_thread(started.wait, 5)
        await w.stop()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert await q.redis.hget(q.keys.job(job.id), "state") == "active"
    assert await q.redis.lrange(q.keys.active, 0, -1) == [job.id]
    assert await q.redis.exists(q.keys.lock(job.id)) == 1


async def test_a_run_that_lost_its_lock_hands_nothing_back_at_shutdown(q):
    """The release is token-guarded like a finish: a job taken over by another run
    (its lock now carries another token) stays with that run, or two workers would
    hold it at once."""
    started = asyncio.Event()

    async def proc(job):
        started.set()
        await asyncio.sleep(30)

    job = await q.add("taken-over", {})
    w = Worker(q.name, proc, prefix=PREFIX, stalled_interval=0, grace_period=0.2)
    task = asyncio.create_task(w.run())
    try:
        await asyncio.wait_for(started.wait(), 5)
        await q.redis.set(q.keys.lock(job.id), "another-workers-token")  # taken over
        await w.stop()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert await q.redis.hget(q.keys.job(job.id), "state") == "active"
    assert await q.redis.lrange(q.keys.active, 0, -1) == [job.id]
    assert await q.redis.get(q.keys.lock(job.id)) == "another-workers-token"
