"""Integration: jobs that share a `concurrency_key` run one at a time
(docs/specs/concurrency-key.md). A held job waits on its key, not on a worker: it
holds no slot and no place in the queue until the key is free.
"""

import asyncio
import time

import pytest

from toro import FlowChild, Queue, Worker

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


async def _in_state(q: Queue, job_id: str, state: str) -> bool:
    return await _state(q, job_id) == state


async def _left_held(q: Queue, job_id: str) -> bool:
    return await _state(q, job_id) != "held"


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


@pytest.mark.parametrize(
    "path", ["completed", "failed", "failed with its parent", "stalled out", "removed at once"]
)
async def test_the_key_passes_on_every_terminal_path(q, run_worker, run_until, path):
    """CK-003: however the holder ends, the next job under its key runs."""
    gate = asyncio.Event()
    started, proc = _recorder(gate)

    async def failing(job):
        started.append(job.name)
        raise RuntimeError("boom")

    if path == "stalled out":
        holder = await q.add("holder", {}, concurrency_key="k")
        held = await q.add("held", {}, concurrency_key="k")
        await q.redis.zrem(q.keys.prioritized, holder.id)  # its worker died holding it
        await q.redis.rpush(q.keys.active, holder.id)
        w = Worker(q.name, proc, prefix=PREFIX, max_stalled_count=0, connection=q.redis)
        await w.check_stalled(throttle_ms=0)
        failed, _ = await w.check_stalled(throttle_ms=0)
        assert failed == [holder.id]
    else:
        opts = {"remove_on_complete": True} if path == "removed at once" else {}
        async with run_worker(q, failing if path == "failed" else proc, concurrency=4) as w:
            w.on("failed", lambda *a, **k: None)
            if path == "failed with its parent":
                await q.add_flow(
                    "report", {}, children=[FlowChild("holder", {}, concurrency_key="k")]
                )
            else:
                await q.add("holder", {}, **opts, concurrency_key="k")
            held = await q.add("held", {}, concurrency_key="k")
            assert await run_until(_count_is(q, "held", 1), timeout=10)
            await _until(lambda: _left_held(q, held.id))

    # it left the held set and ran: however the holder ended, the key moved on
    assert await _left_held(q, held.id)
    assert held.id not in await q.redis.zrange(q.keys.held, 0, -1)


async def test_a_retry_keeps_the_key(q, run_worker, run_until):
    """CK-003: a failure with attempts left is not terminal. The key stays with the job
    through its backoff, or another job would run beside its retry."""
    tries = []

    async def proc(job):
        tries.append(job.name)
        if job.name == "flaky" and len(tries) == 1:
            raise RuntimeError("boom")
        return job.name

    async with run_worker(q, proc, concurrency=4) as w:
        w.on("failed", lambda *a, **k: None)
        flaky = await q.add("flaky", {}, attempts=2, backoff=400, concurrency_key="k")
        held = await q.add("held", {}, concurrency_key="k")
        await _until(lambda: _in_state(q, flaky.id, "delayed"))

        assert await _state(q, held.id) == "held"  # the retry still holds the key
        assert await q.redis.get(q.keys.concurrency("k")) == flaky.id

        assert await run_until(_count_is(q, "completed", 2), timeout=10)
    assert tries == ["flaky", "flaky", "held"]


async def test_removing_a_holder_hands_the_key_on(q, run_worker, run_until):
    """CK-003: a job removed before it finishes must not take its key to the grave."""
    gate = asyncio.Event()
    _, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=4):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        held = await q.add("held", {}, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 1), timeout=10)
        holder_id = await q.redis.get(q.keys.concurrency("k"))

        assert await q.remove_job(holder_id) is True

        assert await run_until(_count_is(q, "completed", 1), timeout=10)
        assert await _state(q, held.id) == "completed"
        gate.set()
    assert await q.redis.get(q.keys.concurrency("k")) is None


async def test_removing_a_held_job_leaves_the_key_alone(q, run_worker, run_until):
    """CK-004: a held job that is removed leaves both held sets and holds nothing."""
    gate = asyncio.Event()
    _, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=4):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        held = await q.add("held", {}, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 1), timeout=10)

        assert await q.remove_job(held.id) is True

        assert await _count(q, "held") == 0
        assert await q.redis.zrange(q.keys.held_for("k"), 0, -1) == []
        gate.set()
        assert await run_until(_count_is(q, "completed", 1), timeout=10)

    assert await q.redis.keys(q.keys.base + "ck:*") == []


async def _until(predicate, *, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition never held")


async def test_a_retried_job_waits_for_the_key_again(q, run_worker, run_until):
    """CK-005: a failed job gave its key up. Retrying it puts it back in the queue for
    the key, behind whoever holds it now, rather than beside them."""
    gate = asyncio.Event()
    started: list[str] = []

    async def failing(job):
        started.append(job.name)
        if job.name == "flaky":
            raise RuntimeError("boom")
        if job.data.get("hold"):
            await gate.wait()
        return job.name

    async with run_worker(q, failing, concurrency=4) as w:
        w.on("failed", lambda *a, **k: None)
        flaky = await q.add("flaky", {}, concurrency_key="k")
        await _until(lambda: _in_state(q, flaky.id, "failed"))
        holder = await q.add("holder", {"hold": True}, concurrency_key="k")
        await _until(lambda: _in_state(q, holder.id, "active"))

        assert await q.retry_job(flaky.id) is True

        assert await _in_state(q, flaky.id, "held")
        assert await q.redis.get(q.keys.concurrency("k")) == holder.id
        gate.set()
        assert await run_until(_count_is(q, "completed", 1), timeout=10)
        await _until(lambda: _left_held(q, flaky.id))


async def test_a_scheduled_occurrence_waits_for_the_key(q, run_worker, run_until):
    """CK-005: an occurrence a worker mints is a job like any other, and waits for the
    key rather than running beside its holder."""
    gate = asyncio.Event()
    _, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=4):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        await _until(lambda: _count_is(q, "active", 1)())
        await q.add_scheduler("tick", every=60_000, concurrency_key="k")

        assert await run_until(_count_is(q, "held", 1), timeout=10)
        assert await _count(q, "delayed") == 0  # it waits on the key, not on the clock
        gate.set()
        assert await run_until(_count_is(q, "completed", 1), timeout=10)

    # the key is free, so the occurrence goes back to waiting out its schedule
    await _until(lambda: _count_is(q, "delayed", 1)())
    assert await _count(q, "held") == 0
