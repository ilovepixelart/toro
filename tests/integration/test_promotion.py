"""Integration: delayed jobs are promoted by the claim, and an idle worker is told
when the next one is due.

Every worker used to sweep `delayed` once a second on its own, so the queue's idle
load grew with the fleet rather than with the work. The claim now promotes what is
due before it picks a job and answers an empty claim with the next due time, so an
idle worker blocks until then. Whatever delays a job marks that time on the marker
("1", scored at the earliest due time), so an idle worker blocked past it wakes,
hears the sooner time, and blocks until it.
"""

import asyncio
import time

from toro import FlowChild, Job, Queue, Worker
from toro.worker import block_for

PREFIX = "torotest"


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _noop(job):
    return None


async def _due(q: Queue, job_id: str) -> float | None:
    return await q.redis.zscore(q.keys.delayed, job_id)


async def _wake_at(q: Queue) -> float | None:
    return await q.redis.zscore(q.keys.marker, "1")


async def test_a_claim_promotes_a_due_delayed_job(q, monkeypatch):
    """The first claim after a delayed job's time takes it, with no delay left on
    the hash: left there, it is listed with a delay it has served, and a concurrency
    key handed to it later parks it in `delayed` again for that delay."""
    j = await q.add("x", {}, delay=60_000)
    monkeypatch.setattr("toro.worker._now_ms", lambda: j.timestamp + 60_000)
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)

    loaded = await w._acquire()

    assert loaded is not None and loaded[0] == j.id
    assert loaded[1]["delay"] == "0"
    assert await q.redis.zcard(q.keys.delayed) == 0
    assert (await q.get_job(j.id)).state == "active"


async def test_an_idle_worker_runs_a_delayed_job_when_it_is_due(q, run_worker, run_until):
    """A sweep ran a delayed job up to a second after its time, and an idle worker
    blocked for its whole `block_timeout` would run it only when the block ended.
    The delayed add wakes the worker, the claim tells it when the job is due, and
    the worker blocks exactly until then."""
    started: list[int] = []

    async def proc(job):
        started.append(_now_ms())

    async with run_worker(q, proc, block_timeout=5.0, heartbeat_interval=0, stalled_interval=0):
        await asyncio.sleep(0.1)  # idle: blocked on the marker for block_timeout
        job = await q.add("x", {}, delay=1200)
        assert await run_until(lambda: started, timeout=5.0)

    late = started[0] - (job.timestamp + 1200)
    assert 0 <= late < 400, f"ran {late} ms after its due time"


def _counting(w: Worker) -> list[str]:
    """Record every command this worker sends from now on (its client's only)."""
    sent: list[str] = []
    real = w.redis.execute_command

    async def counted(*args, **kw):
        sent.append(str(args[0]))
        return await real(*args, **kw)

    w.redis.execute_command = counted
    return sent


_QUIET = {"block_timeout": 5.0, "heartbeat_interval": 0, "stalled_interval": 0}


async def test_an_idle_worker_does_not_poll_for_delayed_jobs(q, run_worker):
    """Idle, a worker sends nothing between its blocking pops: the sweep that ran
    every second on every worker is gone, and the schedule check is seconds apart."""
    async with run_worker(q, _noop, **_QUIET) as w:
        await asyncio.sleep(0.2)  # started: the first claim is done, the pop is blocking
        sent = _counting(w)
        await asyncio.sleep(2.2)
        assert sent == []


async def test_a_delayed_job_once_run_leaves_its_worker_idle(q, run_worker, run_until):
    """Every claim answer replaces the due time the last one gave. Kept past the
    job's run, a due time now in the past would cut every block short, and the slot
    would claim in a loop, a command a millisecond, for nothing."""
    done: list[str] = []

    async def proc(job):
        done.append(job.id)

    async with run_worker(q, proc, **_QUIET) as w:
        await q.add("x", {}, delay=200)
        assert await run_until(lambda: done, timeout=5.0)
        await asyncio.sleep(0.1)  # the finish fetched nothing: the slot is blocking
        sent = _counting(w)
        await asyncio.sleep(1.0)
        assert sent == []


async def test_the_shortest_block_returns_on_every_supported_redis(q):
    """The block an idle slot sends for a due time that has already passed. Redis 6.2
    turns a timeout of 0.001 s into 0, which blocks for good, and Redis 7 rounds it
    up: the floor has to be a wait every supported server reads as short."""
    now = _now_ms()
    timeout = block_for(5.0, now - 1, now)

    popped = await asyncio.wait_for(q.redis.bzpopmin(q.keys.marker, timeout), 2.0)

    assert popped is None


async def test_a_delayed_add_marks_when_the_next_job_is_due(q):
    """The marker's "1" carries the earliest due time: a later add leaves it alone."""
    first = await q.add("x", {}, delay=60_000)
    assert await _wake_at(q) == first.timestamp + 60_000
    sooner = await q.add("x", {}, delay=30_000)
    assert await _wake_at(q) == sooner.timestamp + 30_000
    await q.add("x", {}, delay=90_000)
    assert await _wake_at(q) == sooner.timestamp + 30_000


async def test_a_delayed_flow_child_marks_when_it_is_due(q):
    await q.add_flow("root", {}, children=[FlowChild("leaf", {}, delay=60_000)])
    (leaf,) = await q.redis.zrange(q.keys.delayed, 0, -1)

    assert await _wake_at(q) == await _due(q, leaf)


async def test_a_retry_with_backoff_marks_when_it_is_due(q):
    j = await q.add("x", {}, attempts=2, backoff=60_000)
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    loaded = await w._acquire()
    assert loaded is not None

    await w._finish_failed(Job.from_hash(*loaded), RuntimeError("boom"))

    assert (await q.get_job(j.id)).state == "delayed"
    assert await _wake_at(q) == await _due(q, j.id)


async def test_a_scheduled_occurrence_marks_when_it_is_due(q):
    await q.add_scheduler("tick", every=60_000)
    (occurrence,) = await q.redis.zrange(q.keys.delayed, 0, -1)

    assert await _wake_at(q) == await _due(q, occurrence)


async def test_a_held_job_handed_its_key_marks_when_it_is_due(q):
    """A job that waited for its concurrency key with a delay still to serve goes to
    `delayed` when the key frees, inside the holder's finish."""
    await q.add("a", {}, concurrency_key="k")
    waiting = await q.add("b", {}, concurrency_key="k", delay=60_000)
    assert await _wake_at(q) is None  # held, not delayed: nothing is due yet
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    loaded = await w._acquire()
    assert loaded is not None

    await w._finish_completed(Job.from_hash(*loaded), None)

    assert await _due(q, waiting.id) == waiting.timestamp + 60_000
    assert await _wake_at(q) == waiting.timestamp + 60_000
