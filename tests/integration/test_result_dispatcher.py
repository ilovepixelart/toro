"""The shared result() dispatcher: one events subscription per Queue, routing
terminal events to waiting futures — and its edge paths: already-finished
short-circuits, timeouts, garbage on the channel, a crashed subscription
(waiters fail fast, the next call restarts it), and close() with waiters.
"""

import asyncio
import json
import time

import pytest

from toro import Queue, scripts
from toro.errors import JobFailedError

PREFIX = "torotest"


async def _publish(q, job_id, event="completed", **extra):
    await q.redis.publish(q.keys.events, json.dumps({"jobId": job_id, "event": event, **extra}))


async def test_result_short_circuits_for_already_finished_jobs(q):
    now = int(time.time() * 1000)
    await q.redis.hset(
        q.keys.job("done1"),
        mapping={
            "id": "done1",
            "state": "completed",
            "returnvalue": json.dumps(42),
            "timestamp": now,
        },
    )
    await q.redis.hset(
        q.keys.job("bad1"),
        mapping={"id": "bad1", "state": "failed", "failedReason": "boom", "timestamp": now},
    )
    assert await q.result("done1", timeout=1) == 42
    with pytest.raises(JobFailedError, match="boom"):
        await q.result("bad1", timeout=1)


async def test_result_times_out_with_a_clear_message(q):
    with pytest.raises(TimeoutError, match="ghost-never"):
        await q.result("ghost-never", timeout=0.2)


def _waiting(q, *job_ids):
    """True once every job id has a registered result() future — the
    deterministic 'waiter is ready' signal (no sleep guessing)."""
    return all(j in q._result_waiters for j in job_ids)


async def test_concurrent_waiters_share_one_dispatcher(q, run_until):
    # Both racers pass the "no dispatcher yet" check; the lock makes the loser
    # reuse the winner's subscription instead of opening a second one.
    w1 = asyncio.create_task(q.result("g1", timeout=5))
    w2 = asyncio.create_task(q.result("g2", timeout=5))
    assert await run_until(lambda: _waiting(q, "g1", "g2"))
    subs = int((await q.redis.pubsub_numsub(q.keys.events))[0][1])
    assert subs == 1
    await _publish(q, "g1", result="a")
    await _publish(q, "g2", result="b")
    assert await w1 == "a"
    assert await w2 == "b"
    # A later call finds the dispatcher already running and reuses it.
    w3 = asyncio.create_task(q.result("g3", timeout=5))
    assert await run_until(lambda: _waiting(q, "g3"))
    await _publish(q, "g3", result="c")
    assert await w3 == "c"
    assert int((await q.redis.pubsub_numsub(q.keys.events))[0][1]) == 1


async def test_garbage_and_foreign_events_do_not_disturb_waiters(q, run_until):
    waiter = asyncio.create_task(q.result("real", timeout=5))
    assert await run_until(lambda: _waiting(q, "real"))
    await q.redis.publish(q.keys.events, "not json at all")
    await _publish(q, "real", event="progress", progress=50)  # non-terminal: ignored
    await _publish(q, "someone-else", result="theirs")  # other job: not ours
    await _publish(q, "real", result="ok")
    assert await waiter == "ok"


async def test_dispatcher_crash_fails_waiters_fast_then_restarts(q, run_until):
    waiter = asyncio.create_task(q.result("victim", timeout=10))
    assert await run_until(lambda: _waiting(q, "victim"))

    # Inject a fault into the live subscription: the next get_message raises.
    async def boom(*a, **kw):
        raise ConnectionError("subscription died")

    q._events_pubsub.get_message = boom
    await _publish(q, "anything", event="progress")  # unblock the in-flight read
    t0 = time.monotonic()
    with pytest.raises(ConnectionError):
        await waiter
    assert time.monotonic() - t0 < 2, "waiter should fail fast, not sit out its timeout"

    # The next result() call sweeps the dead listener's leftovers and starts a
    # fresh one — the dispatcher heals without restarting the Queue.
    waiter2 = asyncio.create_task(q.result("phoenix", timeout=5))
    assert await run_until(lambda: _waiting(q, "phoenix"))
    await _publish(q, "phoenix", result="alive")
    assert await waiter2 == "alive"


async def test_close_fails_pending_waiters_fast(run_until):
    queue = Queue("torotest-close", prefix=PREFIX)
    waiter = asyncio.create_task(queue.result("ghost", timeout=10))
    assert await run_until(lambda: _waiting(queue, "ghost"))
    t0 = time.monotonic()
    await queue.close()
    with pytest.raises(RuntimeError, match="queue closed"):
        await waiter
    assert time.monotonic() - t0 < 2


async def test_add_rejects_bad_deduplication(q):
    with pytest.raises(ValueError, match="deduplication"):
        await q.add("x", deduplication={"id": "", "ttl": 0})


async def test_retry_all_failed_on_an_empty_queue(q):
    assert await q.retry_all_failed() == 0


async def test_promote_drains_more_than_one_full_batch(q, run_worker, run_until):
    # A due-backlog larger than PROMOTE_BATCH forces the worker's drain loop to
    # go around again instead of waiting a tick per batch.
    n = scripts.PROMOTE_BATCH + 5
    due = int(time.time() * 1000) - 1000
    pipe = q.redis.pipeline(transaction=False)
    for i in range(n):
        jid = f"due{i}"
        pipe.hset(
            q.keys.job(jid),
            mapping={
                "id": jid,
                "name": "bench",
                "data": "{}",
                "opts": "{}",
                "timestamp": due,
                "attemptsMade": 0,
                "priority": 0,
                "state": "delayed",
                "delay": 1000,
            },
        )
        pipe.zadd(q.keys.delayed, {jid: due})
    await pipe.execute()

    done = 0

    async def proc(job):
        nonlocal done
        done += 1

    async with run_worker(q, proc, concurrency=16, stalled_interval=0):
        assert await run_until(lambda: done >= n, timeout=30.0), f"only {done}/{n} ran"
    counts = await q.counts()
    assert counts["delayed"] == 0
    assert counts["completed"] == n
