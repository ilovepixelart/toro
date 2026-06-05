"""Integration: repeatable schedules (every / cron) firing through a real worker.

(The pure next_run math is covered in tests/unit/test_scheduler.py.)
"""

import asyncio


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
