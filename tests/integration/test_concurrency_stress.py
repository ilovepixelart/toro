"""Concurrency stress: many workers (each its own connection, like separate
processes) hammering one Redis must process every job EXACTLY once - the atomic
BLMOVE+Lua-lock claim is what makes running N processes against one queue safe -
and flows must still settle under that contention. The stalled sweep runs the
whole time on a short interval, so this also proves healthy jobs aren't falsely
recovered (which would double-process).
"""

import asyncio
import collections
import contextlib

from toro import FlowChild as c  # noqa: N813
from toro import Worker

PREFIX = "torotest"


def _completed(q, n):
    async def check():
        return (await q.counts())["completed"] >= n

    return check


@contextlib.asynccontextmanager
async def _fleet(name, proc, *, n: int, concurrency: int):
    """n independent workers (own connections), all sweeping on a short interval;
    lock_duration comfortably exceeds a job so a healthy job is never false-swept."""
    workers = [
        Worker(
            name,
            proc,
            prefix=PREFIX,
            concurrency=concurrency,
            stalled_interval=100,
            lock_duration=3000,
            block_timeout=0.2,
        )
        for _ in range(n)
    ]
    tasks = [asyncio.create_task(w.run()) for w in workers]
    try:
        yield
    finally:
        for w in workers:
            await w.stop(grace_period=2)
        for t in tasks:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t


async def test_every_job_processed_exactly_once_under_contention(q, run_until):
    total, n_workers, concurrency = 200, 4, 4
    runs: collections.Counter[str] = collections.Counter()

    async def proc(job):
        await asyncio.sleep(0.002)  # hold the slot so claims genuinely contend
        runs[job.id] += 1  # single event loop, no await between r-m-w: safe

    expected = {(await q.add(f"j{i}", {"i": i})).id for i in range(total)}

    async with _fleet(q.name, proc, n=n_workers, concurrency=concurrency):
        assert await run_until(_completed(q, total), timeout=30), "fleet did not drain"

    counts = await q.counts()
    assert counts["completed"] == total and counts["failed"] == 0
    assert set(runs) == expected  # nothing missed, nothing spurious
    dupes = [jid for jid, runcount in runs.items() if runcount != 1]
    assert not dupes, f"processed more than once: {dupes}"


async def test_flows_settle_under_contention(q, run_until):
    n_flows, kids, n_workers = 30, 4, 4
    parents = []

    async def proc(job):
        if job.name.startswith("flow"):
            return sorted((await job.children_results()).values())
        return job.data["i"]

    for f in range(n_flows):
        p = await q.add_flow(
            f"flow{f}", {}, children=[c(f"c{f}-{i}", {"i": i}) for i in range(kids)]
        )
        parents.append(p.id)

    nodes = n_flows * (kids + 1)
    async with _fleet(q.name, proc, n=n_workers, concurrency=4):
        assert await run_until(_completed(q, nodes), timeout=30), "flows did not drain"

    # every parent ran exactly once on its children's results; nothing stuck
    assert (await q.counts())["completed"] == nodes
    assert (await q.counts())["waiting-children"] == 0
    for pid in parents:
        job = await q.get_job(pid)
        assert job is not None and job.state == "completed"
        assert len(job.returnvalue) == kids  # the fan-in collected every child
