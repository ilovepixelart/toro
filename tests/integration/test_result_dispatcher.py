"""The shared result() dispatcher: one events subscription per Queue, routing
terminal events to waiting futures - and its edge paths: already-finished
short-circuits, timeouts, garbage on the channel, a crashed subscription
(waiters fail fast, the next call restarts it), and close() with waiters.
"""

import asyncio
import json
import time

import pytest
import redis.asyncio as aioredis

import toro.queue as queue_module
from toro import Queue, scripts
from toro.errors import JobFailedError

PREFIX = "torotest"


async def test_a_waiter_outlives_channel_silence_longer_than_the_read_timeout(
    q, run_worker, run_until
):
    """The events subscription read with no timeout, and on the declared redis-py
    floor (5.0.1) a read with none falls back to the connection's socket timeout:
    after that much silence on the channel the read raised, the dispatcher died
    and failed every waiter with a Redis TimeoutError, well inside the waiter's
    own timeout. The read is bounded under the socket timeout and loops."""
    conn = aioredis.from_url("redis://localhost:6379", socket_timeout=0.5, decode_responses=True)
    producer = Queue(q.name, prefix=PREFIX, connection=conn)
    started = asyncio.Event()

    async def slow(job):
        started.set()
        await asyncio.sleep(1.2)  # longer than the socket timeout, no event meanwhile
        return "late"

    try:
        job = await producer.add("slow", {})
        async with run_worker(q, slow):
            assert await producer.result(job.id, timeout=5) == "late"
    finally:
        await producer.close()
        await conn.aclose()


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
    """True once every job id has a registered result() future - the
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
    # fresh one - the dispatcher heals without restarting the Queue.
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
    # A due backlog larger than PROMOTE_BATCH takes more than one claim to promote:
    # each claim promotes a batch, and the claims that follow take the rest.
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
                "opts": '{"removeOnComplete": false}',  # the count below is of every job
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


async def test_a_worker_before_1_0_3_still_resolves_its_waiters(q, run_until):
    """During a rolling upgrade an older worker still sends the decoded `result`, not
    the stored text: the waiter takes it as it comes."""
    waiting = asyncio.create_task(q.result("old-worker", timeout=5))
    assert await run_until(lambda: _waiting(q, "old-worker"))

    await _publish(q, "old-worker", result={"n": 7})

    assert await waiting == {"n": 7}


async def test_result_text_that_does_not_parse_falls_back_to_the_hash(q, run_until):
    """The event is only the fast path: when its copy of the result cannot be read,
    the waiter reads the value from the job's hash, where the finish wrote it."""
    await q.redis.hset(
        q.keys.job("unreadable"),
        mapping={
            "id": "unreadable",
            "state": "active",
            "timestamp": int(time.time() * 1000),  # every job hash carries its add time
            "returnvalue": json.dumps([1, 2]),
        },
    )
    waiting = asyncio.create_task(q.result("unreadable", timeout=5))
    assert await run_until(lambda: _waiting(q, "unreadable"))

    await _publish(q, "unreadable", resultJson="{not json")

    assert await waiting == [1, 2]


async def test_a_message_that_is_not_an_object_does_not_disturb_waiters(q, run_until):
    """Anything can be published on the channel. A JSON array, number or null used to
    raise inside the dispatcher and fail every waiter at once."""
    waiting = asyncio.create_task(q.result("steady", timeout=5))
    assert await run_until(lambda: _waiting(q, "steady"))

    for raw in ("[1, 2]", "42", "null"):
        await q.redis.publish(q.keys.events, raw)
    await _publish(q, "steady", resultJson=json.dumps("ok"))

    assert await waiting == "ok"


async def test_close_ends_a_read_back_still_in_flight(q, run_until, monkeypatch):
    """A read-back started before close() carried on after it and used the closed
    client, which redis-py quietly reconnects: a connection nothing would close."""
    held = asyncio.Event()
    real_get_job = q.get_job

    async def slow_get_job(job_id):
        if asyncio.current_task() in q._read_backs:
            await held.wait()  # only the read-back is held, in flight when close() runs
        return await real_get_job(job_id)

    monkeypatch.setattr(q, "get_job", slow_get_job)
    waiting = asyncio.create_task(q.result("read-back", timeout=5))
    assert await run_until(lambda: _waiting(q, "read-back"))
    await _publish(q, "read-back")  # nothing inline: the waiter reads the hash
    assert await run_until(lambda: q._read_backs)
    in_flight = set(q._read_backs)
    try:
        # Bounded: a close() that waited for the read-back instead of ending it would
        # wait for `held`, set only below, and hang the suite instead of failing here.
        await asyncio.wait_for(q.close(), 5)
        assert all(task.done() for task in in_flight)
    finally:
        held.set()
    with pytest.raises(RuntimeError, match="queue closed"):
        await waiting


async def test_a_result_whose_event_was_lost_is_still_delivered(q, run_until, monkeypatch):
    """A pub/sub reconnect (redis-py re-subscribes without a word) drops whatever was
    published in the gap. The waiter re-reads the job's hash while it waits, so a job
    that finished meanwhile is delivered rather than timed out."""
    monkeypatch.setattr(queue_module, "RESULT_RECHECK_S", 0.2)
    waiting = asyncio.create_task(q.result("quiet", timeout=5))
    assert await run_until(lambda: _waiting(q, "quiet"))

    now = int(time.time() * 1000)
    await q.redis.hset(  # the finish landed in the hash; its event never arrived
        q.keys.job("quiet"),
        mapping={"id": "quiet", "state": "completed", "timestamp": now, "returnvalue": '"done"'},
    )

    assert await asyncio.wait_for(waiting, 3) == "done"  # well inside the 5 s timeout


async def test_a_failure_whose_event_was_lost_is_still_raised(q, run_until, monkeypatch):
    monkeypatch.setattr(queue_module, "RESULT_RECHECK_S", 0.2)
    waiting = asyncio.create_task(q.result("quiet-fail", timeout=5))
    assert await run_until(lambda: _waiting(q, "quiet-fail"))

    now = int(time.time() * 1000)
    await q.redis.hset(
        q.keys.job("quiet-fail"),
        mapping={"id": "quiet-fail", "state": "failed", "timestamp": now, "failedReason": "boom"},
    )

    with pytest.raises(JobFailedError, match="boom"):
        await asyncio.wait_for(waiting, 3)
