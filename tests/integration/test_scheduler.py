"""Integration: repeatable schedules (every / cron) firing through a real worker.

(The pure next_run math is covered in tests/unit/test_scheduler.py.)
"""

import asyncio
import json

from toro import Queue

PREFIX = "torotest"


async def test_scheduler_every_fires_repeatedly(q, run_worker, run_until):
    runs: list = []

    async def proc(job):
        runs.append(job.name)

    async with run_worker(q, proc):
        await q.add_scheduler("tick", every=400, name="tick")
        assert await run_until(lambda: len(runs) >= 3, timeout=8.0)

    assert all(n == "tick" for n in runs)
    scheds = await q.schedulers()
    assert len(scheds) == 1 and scheds[0]["id"] == "tick"


async def test_remove_scheduler_stops_it(q, run_worker, run_until):
    runs: list = []

    async def proc(job):
        runs.append(1)

    async with run_worker(q, proc):
        await q.add_scheduler("tick", every=400)
        assert await run_until(lambda: len(runs) >= 1, timeout=8.0)

        await q.remove_scheduler("tick")
        assert await q.schedulers() == []

        fired = len(runs)
        await asyncio.sleep(2.5)  # well past interval + delayed poll
        assert len(runs) - fired <= 1  # at most one in-flight occurrence


async def test_scheduler_template_carries_the_queue_defaults(q):
    """A worker mints every occurrence from the STORED template and never sees the
    producer's `default_job_options`, so they have to be in the template."""
    keep = {"remove_on_complete": False, "remove_on_fail": False}
    producer = Queue(q.name, prefix=PREFIX, default_job_options={**keep, "attempts": 4})
    try:
        await producer.add_scheduler("tick", every=60_000, backoff=250)
        stored = json.loads(await q.redis.hget(q.keys.scheduler("tick"), "opts"))
        first = (await q.get_jobs("delayed", 0, 0))[0]
    finally:
        await producer.close()

    assert (stored["removeOnComplete"], stored["removeOnFail"]) == (False, False)
    assert (stored["attempts"], stored["backoff"]) == (4, 250)  # merged, not replaced
    assert first.opts.remove_on_complete is False  # and stamped on the occurrence


async def test_scheduler_options_win_over_the_queue_defaults(q):
    producer = Queue(q.name, prefix=PREFIX, default_job_options={"attempts": 4})
    try:
        await producer.add_scheduler("tick", every=60_000, attempts=2)
        stored = json.loads(await q.redis.hget(q.keys.scheduler("tick"), "opts"))
    finally:
        await producer.close()
    assert stored["attempts"] == 2
