"""Integration: worker presence/heartbeat - a running worker registers itself,
tracks throughput, reports what it's running, deregisters on shutdown, and stale
records (a crashed worker that never deregistered) get pruned on read.
"""

import asyncio
import time

from toro import Worker
from toro.worker import PRESENCE_TTL_MS

PREFIX = "torotest"


async def _workers_has(q, predicate):
    ws = await q.workers()
    return ws and predicate(ws[0])


async def test_running_worker_registers_its_presence(q, run_worker, run_until):
    async def proc(job):
        return "ok"

    async with run_worker(q, proc) as w:
        assert await run_until(lambda: q.workers())  # it shows up
        ws = await q.workers()
        assert len(ws) == 1
        rec = ws[0]
        assert rec["id"] == w.token
        assert rec["queue"] == q.name
        assert rec["concurrency"] == 1
        assert rec["pid"] > 0
        assert rec["host"]
        assert rec["started"] > 0
        assert rec["heartbeat"] >= rec["started"]
        assert rec["state"] == "running"

    assert await q.workers() == []  # deregistered on graceful shutdown


async def test_worker_tracks_processed_and_failed(q, run_worker, run_until):
    async def proc(job):
        if job.data.get("boom"):
            raise RuntimeError("nope")
        return "ok"

    async with run_worker(q, proc, heartbeat_interval=100):
        await q.add("a", {})
        await q.add("b", {})
        await q.add("c", {"boom": True}, attempts=1)
        ok = await run_until(
            lambda: _workers_has(q, lambda r: r["processed"] >= 2 and r["failed"] >= 1),
            timeout=5,
        )
        assert ok, await q.workers()


async def test_worker_reports_the_job_it_is_running(q, run_worker, run_until):
    started, release = asyncio.Event(), asyncio.Event()

    async def proc(job):
        started.set()
        await release.wait()
        return "ok"

    async with run_worker(q, proc, heartbeat_interval=100):
        jid = (await q.add("slow", {})).id
        await asyncio.wait_for(started.wait(), timeout=5)
        # the heartbeat snapshots `current`, so wait one out
        assert await run_until(lambda: _workers_has(q, lambda r: jid in r["current"]), timeout=5)
        release.set()


async def test_stale_worker_is_pruned_on_read(q):
    old = int(time.time() * 1000) - 60_000  # 60s ago - well past the 30s default
    await q.redis.zadd(q.keys.workers, {"ghost": old})
    await q.redis.hset(
        q.keys.worker("ghost"),
        mapping={"heartbeat": old, "started": old, "host": "x", "pid": 1, "concurrency": 1},
    )
    assert await q.workers() == []  # treated as dead
    assert await q.redis.zcard(q.keys.workers) == 0  # and physically removed
    assert await q.redis.exists(q.keys.worker("ghost")) == 0


async def test_graceful_shutdown_is_recorded_as_stopped(q, run_worker, run_until):
    async def proc(job):
        return "ok"

    async with run_worker(q, proc) as w:
        assert await run_until(lambda: q.workers())
        token = w.token
    # leaving the context stops the worker gracefully → logged as "stopped"
    departed = await q.departed_workers()
    assert any(d["id"] == token and d["reason"] == "stopped" for d in departed)


async def test_clear_departed_drops_the_history(q):
    await q.redis.lpush(q.keys.departed, '{"id": "w1", "reason": "stopped"}')
    assert await q.departed_workers()  # recorded
    assert await q.clear_departed() == 1  # returns how many were cleared
    assert await q.departed_workers() == []  # history gone
    assert await q.redis.exists(q.keys.departed) == 0


async def test_lost_worker_is_recorded_when_pruned(q):
    old = int(time.time() * 1000) - 60_000  # 60s stale
    await q.redis.zadd(q.keys.workers, {"ghost": old})
    await q.redis.hset(
        q.keys.worker("ghost"),
        mapping={
            "heartbeat": old,
            "started": old,
            "host": "node-7",
            "pid": 4242,
            "concurrency": 3,
            "processed": 99,
            "failed": 5,
            "current": '["210", "211"]',  # it was mid-flight on these when it vanished
        },
    )
    assert await q.workers() == []  # pruned
    departed = await q.departed_workers()
    assert len(departed) == 1
    rec = departed[0]
    assert rec["id"] == "ghost"
    assert rec["reason"] == "lost"  # crashed, not a graceful stop
    assert rec["host"] == "node-7"
    assert rec["pid"] == 4242
    assert rec["processed"] == 99
    # the death record froze WHAT IT WAS RUNNING - the whole point of the post-mortem
    assert rec["current"] == ["210", "211"]
    assert rec["last_seen"] < rec["at"]  # last heartbeat vs when the sweep detected it


async def test_presence_reports_global_concurrency(q, run_worker, run_until):
    async def proc(job):
        return "ok"

    async with run_worker(q, proc, global_concurrency=3):
        assert await run_until(lambda: q.workers())
        assert (await q.workers())[0]["global_concurrency"] == 3

    async with run_worker(q, proc):
        assert await run_until(lambda: q.workers())
        assert (await q.workers())[0]["global_concurrency"] == 0  # unset = no cap


async def _noop(job):
    return None


async def test_presence_expires_without_a_reader(q):
    """Dead workers are pruned when something reads `workers()` - in practice a
    dashboard. With no reader, a worker killed without deregistering (an OOM, a
    SIGKILL) must not leave its record behind forever: the record carries an expiry
    that every heartbeat renews."""
    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    await w._write_heartbeat()

    ttl = await q.redis.pttl(q.keys.worker(w.token))
    assert PRESENCE_TTL_MS - 5_000 < ttl <= PRESENCE_TTL_MS


async def test_a_heartbeat_sweeps_entries_whose_record_has_expired(q):
    """The `workers` index has no expiry of its own: a live worker's heartbeat drops
    entries older than the record's lifetime. One the dashboard could still report as
    lost (its record is alive) is left for `workers()` to log and prune."""
    now = int(time.time() * 1000)
    await q.redis.zadd(
        q.keys.workers,
        {"long-gone": now - PRESENCE_TTL_MS - 60_000, "just-died": now - 60_000},
    )
    await q.redis.hset(q.keys.worker("just-died"), mapping={"heartbeat": now - 60_000})

    w = Worker(q.name, _noop, prefix=PREFIX, connection=q.redis)
    await w._write_heartbeat()

    assert await q.redis.zscore(q.keys.workers, "long-gone") is None
    assert await q.redis.zscore(q.keys.workers, "just-died") is not None
    assert await q.redis.zscore(q.keys.workers, w.token) is not None
    assert [d["id"] for d in await _lost(q)] == ["just-died"]  # still reported


async def _lost(q):
    await q.workers()
    return [d for d in await q.departed_workers() if d["reason"] == "lost"]
