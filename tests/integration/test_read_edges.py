"""Integration: the read API at its edges.

A job hash lives at `<base><id>`, beside the queue's own keys; ranges have Redis's
negative-index semantics in one state and not another; latency counted a delayed
job's wait from the moment it was added, not from the moment it could run.
"""

import asyncio
import enum

import toro.queue as queue_module
from toro import Queue
from toro.queue import _now_ms

PREFIX = "torotest"


def _count_is(q: Queue, state: str, n: int):
    async def check() -> bool:
        return (await q.counts())[state] == n

    return check


async def _boom(job):
    raise RuntimeError("boom")


async def test_get_job_on_one_of_the_queues_own_keys_is_no_job(q, run_worker, run_until):
    """Asking for the id "failed" once a job had failed read the failed set itself, and
    the WRONGTYPE error reached the caller (a dashboard's search box became a 500). A
    hash that is not a job's, such as a scheduler's template, hydrated into a phantom
    job with no state and no data."""
    await q.add("boom", {}, attempts=1)
    async with run_worker(q, _boom, stalled_interval=0):
        assert await run_until(_count_is(q, "failed", 1))
    await q.add_scheduler("nightly", cron="0 0 * * *")

    assert await q.get_job("failed") is None
    assert await q.get_job("repeat:nightly") is None
    assert await q.get_job("meta") is None


async def test_get_jobs_ranges_read_as_redis_reads_them_in_every_state(q, run_worker, run_until):
    """An end before the start is empty, and a negative index counts from the end of
    the listing: what `wait` and `active` already did through Redis, `completed` and
    `failed` turned into "everything from the start" or an error."""
    for i in range(3):
        await q.add(f"c{i}", {})
    async with run_worker(q, lambda job: None, stalled_interval=0):
        assert await run_until(_count_is(q, "completed", 3))
    for i in range(3):
        await q.add(f"w{i}", {})
    listing = [j.name for j in await q.get_jobs("completed", 0, -1)]
    assert sorted(listing) == ["c0", "c1", "c2"]

    assert await q.get_jobs("completed", 2, 0) == []
    assert await q.get_jobs("wait", 2, 0) == []
    assert [j.name for j in await q.get_jobs("completed", -2, -1)] == listing[-2:]
    assert [j.name for j in await q.get_jobs("wait", -2, -1)] == ["w1", "w2"]
    assert await q.get_jobs("completed", 3, 10) == []


async def test_latency_counts_from_when_a_delayed_job_could_first_run(q, run_worker, run_until):
    """A job added with a delay is not late while it waits for its delay to pass:
    latency counted from the add, so a queue full of scheduled work read as badly
    behind while every worker sat idle."""
    job = await q.add("later", {}, delay=500)
    await q.pause()
    async with run_worker(q, lambda job: None, stalled_interval=0):  # its sweep promotes it
        assert await run_until(_count_is(q, "wait", 1), timeout=5)
        latency = await q.latency()
        since_add = _now_ms() - job.timestamp
    assert latency <= since_add - 400, (latency, since_add)  # the 500 ms delay is not lateness
    await asyncio.sleep(0)


async def test_latency_of_a_scheduler_occurrence_counts_from_its_due_time(
    q, run_worker, run_until, monkeypatch
):
    """An occurrence is minted a whole cadence ahead, with its wait in the hash rather
    than in its options: once due, it read as a full hour late the moment it became
    runnable, on a queue no worker was behind on."""
    real_now = _now_ms()
    monkeypatch.setattr(queue_module, "_now_ms", lambda: real_now - 3_600_000)
    await q.add_scheduler("hourly", every=3_600_000)  # minted an hour ago: due about now
    monkeypatch.undo()
    await q.pause()
    async with run_worker(q, lambda job: None, stalled_interval=0):  # promotes, claims nothing
        assert await run_until(_count_is(q, "wait", 1), timeout=5)
        latency = await q.latency()
    assert latency < 1_000, latency


async def test_latency_of_a_retry_counts_from_the_end_of_its_backoff(q, run_worker, run_until):
    """A retry waits out its backoff in `delayed` with the wait nowhere in its options:
    promoted, it read as late by the backoff and the failed run before it."""

    async def proc(job):
        raise RuntimeError("boom")

    await q.add("flaky", {}, attempts=2, backoff=1_000)
    async with run_worker(q, proc, stalled_interval=0):
        assert await run_until(_count_is(q, "delayed", 1), timeout=5)
        await q.pause()
        assert await run_until(_count_is(q, "wait", 1), timeout=5)
        latency = await q.latency()
    assert latency < 500, latency


async def test_latency_of_a_retried_job_counts_from_the_retry(q, run_worker, run_until):
    """A failed job put back by retry_job() has been runnable since the retry, not
    since its add: the time it spent failed is history, not lateness."""

    async def proc(job):
        raise RuntimeError("boom")

    job = await q.add("flaky", {}, attempts=1)
    async with run_worker(q, proc, stalled_interval=0):
        assert await run_until(_count_is(q, "failed", 1), timeout=5)
        await q.pause()
        await asyncio.sleep(0.3)  # time spent failed
        assert await q.retry_job(job.id) is True
        assert await run_until(_count_is(q, "wait", 1), timeout=5)
        latency = await q.latency()
    assert latency < 200, latency


class _Delay(enum.IntEnum):
    SLOW = 5000


async def test_an_int_subclass_option_reaches_the_script_as_a_number(q):
    """`delay=Delay.SLOW` passed validation as an int and went over the wire as the
    enum's repr, so the add script wrote the job hash and then failed on the delay:
    the caller got an error, the id was spent, and the hash sat in no state set."""
    job = await q.add("later", {}, delay=_Delay.SLOW)
    assert (await q.counts())["delayed"] == 1
    assert (await q.get_job(job.id)).opts.delay == 5000
