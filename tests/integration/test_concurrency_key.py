"""Integration: jobs that share a `concurrency_key` run one at a time
(docs/specs/concurrency-key.md). A held job waits on its key, not on a worker: it
holds no slot and no place in the queue until the key is free.
"""

import asyncio

from toro import Queue

PREFIX = "torotest"


def _recorder(gate: asyncio.Event):
    started: list[str] = []

    async def proc(job):
        started.append(job.name)
        if job.data.get("hold"):
            await gate.wait()
        return job.name

    return started, proc


async def _count(q: Queue, state: str) -> int:
    return (await q.counts())[state]


def _count_is(q: Queue, state: str, n: int):
    """A run_until predicate: an async closure, so the comparison happens on the value."""

    async def check() -> bool:
        return await _count(q, state) == n

    return check


async def _state(q: Queue, job_id: str) -> str | None:
    return await q.redis.hget(q.keys.job(job_id), "state")


async def test_one_job_per_key_at_a_time(q, run_worker, run_until):
    """CK-001: the second job under a key waits for the first to settle, whatever the
    worker's concurrency. Other keys, and jobs with no key, run meanwhile."""
    gate = asyncio.Event()
    started, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=8):
        holder = await q.add("holder", {"hold": True}, concurrency_key="order-1")
        held = await q.add("held", {}, concurrency_key="order-1")
        await q.add("other-key", {}, concurrency_key="order-2")
        await q.add("no-key", {})
        assert await run_until(_count_is(q, "completed", 2), timeout=10)

        assert sorted(started) == ["holder", "no-key", "other-key"]
        assert await _count(q, "held") == 1
        assert await _state(q, held.id) == "held"
        assert held.id not in await q.redis.zrange(q.keys.prioritized, 0, -1)

        gate.set()
        assert await holder.result(timeout=10) == "holder"
        assert await held.result(timeout=10) == "held"

    assert started[-1] == "held"  # it ran only once the key was free


async def test_held_jobs_keep_their_order_and_priority(q, run_worker, run_until):
    """CK-002: held jobs run in the order they were added, and a more urgent one added
    later goes first, at the score it would have had in the queue."""
    gate = asyncio.Event()
    started, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=8):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        await q.add("first", {}, concurrency_key="k")
        await q.add("second", {}, concurrency_key="k")
        await q.add("urgent", {}, priority=5, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 3), timeout=10)

        gate.set()
        assert await run_until(_count_is(q, "completed", 4), timeout=10)

    assert started == ["holder", "urgent", "first", "second"]


async def test_a_key_leaves_nothing_behind(q, run_worker, run_until):
    """CK-004: the per-key bookkeeping lives only while jobs are using the key."""
    gate = asyncio.Event()
    _, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=4):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        await q.add("held", {}, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 1), timeout=10)
        assert await q.redis.get(q.keys.concurrency("k")) is not None

        gate.set()
        assert await run_until(_count_is(q, "completed", 2), timeout=10)

    assert await q.redis.keys(q.keys.base + "ck:*") == []
    assert await q.redis.keys(q.keys.base + "held:*") == []
    assert await _count(q, "held") == 0
