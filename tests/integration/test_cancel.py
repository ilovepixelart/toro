"""Integration: cancelling a job, wherever it is (docs/specs/cancel.md).

A job that has not started is ended at once; a running one is told to stop and its
processor is cancelled where it awaits. Either way it lands in `cancelled`, which is
a terminal state of its own: a cancellation is not a failure.
"""

import asyncio

import pytest

from toro import FlowChild, JobCancelledError, Queue

PREFIX = "torotest"


async def _count(q: Queue, state: str) -> int:
    return (await q.counts())[state]


async def _state(q: Queue, job_id: str) -> str | None:
    return await q.redis.hget(q.keys.job(job_id), "state")


async def test_cancelled_is_a_state(q, run_worker, run_until):
    """CN-005: an eighth state that every listing has to answer for, or a cancelled
    job is one nobody can find."""
    job = await q.add("doomed", {"tag": "needle"})
    assert await q.cancel_job(job.id) is True

    assert await _state(q, job.id) == "cancelled"
    assert await _count(q, "cancelled") == 1
    assert await _count(q, "wait") == 0
    assert [j.id for j in await q.get_jobs("cancelled", 0, -1)] == [job.id]
    total, roots = await q.get_jobs_roots("cancelled", 0, -1)
    assert (total, [j.id for j in roots]) == (1, [job.id])
    assert (await q.roots_counts())["cancelled"] == 1
    assert [j.id for j in await q.search("cancelled", "needle")] == [job.id]
    assert (await q.get_job(job.id)).state == "cancelled"

    assert await q.clean("cancelled") == 1
    assert await _count(q, "cancelled") == 0


async def _in_state(q: Queue, job_id: str, state: str) -> bool:
    return await _state(q, job_id) == state


def _count_is(q: Queue, state: str, n: int):
    """A run_until predicate: an async closure, so the comparison happens on the value."""

    async def check() -> bool:
        return await _count(q, state) == n

    return check


@pytest.mark.parametrize("where", ["wait", "delayed", "held", "waiting-children"])
async def test_a_job_that_has_not_started_is_cancelled_at_once(q, run_worker, run_until, where):
    """CN-001: no worker is involved, so there is nothing to ask and nothing to wait
    for. The job is terminal by the time cancel_job returns, and never runs."""
    if where == "delayed":
        job = await q.add("doomed", {}, delay=60_000)
    elif where == "held":
        await q.add("holder", {}, concurrency_key="k")
        job = await q.add("doomed", {}, concurrency_key="k")
    elif where == "waiting-children":
        job = await q.add_flow("doomed", {}, children=[FlowChild("leaf", {}, delay=60_000)])
    else:
        job = await q.add("doomed", {})
    assert await _in_state(q, job.id, where)

    assert await q.cancel_job(job.id) is True

    assert await _state(q, job.id) == "cancelled"
    # a flow parent takes its subtree with it, so its leaf is cancelled too (CN-007)
    assert await _count(q, "cancelled") == (2 if where == "waiting-children" else 1)
    assert await _count(q, where) == 0
    # it is in no other collection: nothing can hand it to a worker
    assert job.id not in await q.redis.zrange(q.keys.prioritized, 0, -1)
    assert job.id not in await q.redis.zrange(q.keys.delayed, 0, -1)
    assert job.id not in await q.redis.zrange(q.keys.held, 0, -1)

    started = []
    async with run_worker(q, lambda j: started.append(j.name), concurrency=4):
        assert not await run_until(lambda: "doomed" in started, timeout=1)


async def test_cancelling_a_held_job_leaves_its_keys_queue(q):
    """CN-006: a held job named in its key's queue after it is gone would be handed
    the key and resurrected."""
    await q.add("holder", {}, concurrency_key="k")
    doomed = await q.add("doomed", {}, concurrency_key="k")

    assert await q.cancel_job(doomed.id) is True

    assert await q.redis.zrange(q.keys.held_for("k"), 0, -1) == []


async def test_cancelling_a_key_holder_hands_the_key_on(q):
    """CN-006: a holder that is cancelled must not take its key to the grave."""
    holder = await q.add("holder", {}, concurrency_key="k")
    behind = await q.add("behind", {}, concurrency_key="k")

    assert await q.cancel_job(holder.id) is True

    assert await q.redis.get(q.keys.concurrency("k")) == behind.id
    assert await _state(q, behind.id) == "wait"


@pytest.mark.parametrize(
    ("on_fail", "parent"), [("fail_parent", "cancelled"), ("continue", "wait")]
)
async def test_a_cancelled_child_settles_its_parent_by_policy(q, on_fail, parent):
    """CN-006: a parent waits on its children, so a cancelled one has to settle it or
    the flow is parked forever. A child that was stopped will never deliver what its
    parent waits for, which is what `on_fail` already decides: one rule, not two.

    `fail_parent` stops the parent rather than failing it. The stop was deliberate, and
    recording it as a failure is the one thing the separate state exists to prevent.
    """
    root = await q.add_flow(
        "report", {}, children=[FlowChild("leaf", {}, delay=60_000, on_fail=on_fail)]
    )
    tree = await q.get_flow(root.id)
    leaf = tree["children"][0]["job"].id

    assert await q.cancel_job(leaf) is True

    assert await _state(q, leaf) == "cancelled"
    assert await _in_state(q, root.id, parent)
    if parent == "cancelled":
        assert await _count(q, "failed") == 0  # nothing failed, so nothing is counted
    else:
        # the parent runs and can see why the child never delivered
        assert await q.redis.hget(q.keys.cfail(root.id), leaf) == "cancelled"


@pytest.mark.parametrize("state", ["completed", "failed"])
async def test_cancelling_what_cannot_be_cancelled(q, run_worker, run_until, state):
    """CN-009: a job that has already finished, or that never existed, is not a job to
    stop. Returning True would tell a caller it stopped something."""
    assert await q.cancel_job("no-such-job") is False

    async def proc(job):
        if state == "failed":
            raise RuntimeError("boom")
        return job.name

    async with run_worker(q, proc, concurrency=2) as w:
        w.on("failed", lambda *a, **k: None)
        job = await q.add("job", {})
        assert await run_until(lambda: _in_state(q, job.id, state), timeout=10)

    assert await q.cancel_job(job.id) is False
    assert await _state(q, job.id) == state
    assert await _count(q, "cancelled") == 0


async def test_cancelling_a_running_job_stops_its_processor(q, run_worker, run_until):
    """CN-002: a running job's worker owns its processor, so only the worker can stop
    it. Cancellation lands where the processor awaits, so its cleanup runs."""
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def proc(job):
        started.set()
        try:
            await asyncio.sleep(60)  # it never finishes on its own
        finally:
            cleaned.set()
        return job.name

    async with run_worker(q, proc, concurrency=2):
        job = await q.add("long", {})
        await asyncio.wait_for(started.wait(), 10)

        assert await q.cancel_job(job.id) is True

        await asyncio.wait_for(cleaned.wait(), 5)  # the processor's finally ran
        assert await run_until(lambda: _in_state(q, job.id, "cancelled"), timeout=10)

    assert await _count(q, "cancelled") == 1
    assert await _count(q, "active") == 0
    assert await _count(q, "failed") == 0  # a cancellation is not a failure


async def test_a_cancel_arrives_promptly(q, run_worker, run_until):
    """CN-003: the cancel channel is what makes a cancellation prompt. With the lock
    backstop half a minute away, only the message can land it in time."""
    started = asyncio.Event()

    async def proc(job):
        started.set()
        await asyncio.sleep(60)

    async with run_worker(q, proc, concurrency=2, lock_duration=60_000, lock_renew_time=30_000):
        job = await q.add("long", {})
        await asyncio.wait_for(started.wait(), 10)

        assert await q.cancel_job(job.id) is True

        assert await run_until(lambda: _in_state(q, job.id, "cancelled"), timeout=3)


async def test_a_cancel_with_no_message_still_lands(q, run_worker, run_until):
    """CN-003: a dropped message has to cost latency, never the cancellation. This
    sets the flag the script sets and publishes nothing: the lock renewal finds it."""
    started = asyncio.Event()

    async def proc(job):
        started.set()
        await asyncio.sleep(60)

    async with run_worker(q, proc, concurrency=2, lock_duration=2000, lock_renew_time=200):
        job = await q.add("long", {})
        await asyncio.wait_for(started.wait(), 10)
        await q.redis.hset(q.keys.job(job.id), "cancel", "1")  # no PUBLISH

        assert await run_until(lambda: _in_state(q, job.id, "cancelled"), timeout=10)


async def test_a_cancelled_job_does_not_retry(q, run_worker, run_until):
    """CN-004: a job told to stop has been told not to run. Attempts left are not a
    reason to run it again, and neither is an operator's retry."""
    runs = []
    started = asyncio.Event()

    async def proc(job):
        runs.append(job.name)
        started.set()
        await asyncio.sleep(60)

    async with run_worker(q, proc, concurrency=2):
        job = await q.add("long", {}, attempts=5, backoff=10)
        await asyncio.wait_for(started.wait(), 10)

        assert await q.cancel_job(job.id) is True
        assert await run_until(lambda: _in_state(q, job.id, "cancelled"), timeout=10)
        await asyncio.sleep(0.3)  # a backoff retry would have landed by now

    assert runs == ["long"]  # it ran once and was not tried again
    assert await _count(q, "delayed") == 0
    assert await q.retry_job(job.id) is False
    assert await _state(q, job.id) == "cancelled"


async def test_cancelling_a_flow_takes_its_subtree(q, run_worker, run_until):
    """CN-007: a flow is cancelled as a unit. A child left running would report into a
    parent that is already gone."""
    started = asyncio.Event()

    async def proc(job):
        started.set()
        await asyncio.sleep(60)

    async with run_worker(q, proc, concurrency=4):
        root = await q.add_flow(
            "report",
            {},
            children=[FlowChild("running", {}), FlowChild("queued", {}, delay=60_000)],
        )
        await asyncio.wait_for(started.wait(), 10)

        assert await q.cancel_job(root.id) is True

        # the root, the running child and the queued one. `_count_is` already returns
        # the predicate, so wrapping it in a lambda hands run_until a function object:
        # not a coroutine, and truthy, so the poll would pass without ever counting.
        assert await run_until(_count_is(q, "cancelled", 3), timeout=10)

    assert await _count(q, "active") == 0
    assert await _count(q, "delayed") == 0
    assert await _count(q, "waiting-children") == 0
    assert await _count(q, "failed") == 0  # cancelling a flow does not fail it


async def test_result_reports_a_cancellation(q):
    """CN-008: a caller waiting on a cancelled job must be told, not left to wait out
    its timeout for a job that will never finish."""
    job = await q.add("doomed", {})
    waiting = asyncio.create_task(job.result(timeout=10))
    await asyncio.sleep(0.2)  # it is registered and waiting

    assert await q.cancel_job(job.id) is True

    with pytest.raises(JobCancelledError):
        await waiting
    with pytest.raises(JobCancelledError):  # and asking after the fact
        await q.result(job.id, timeout=5)


async def test_a_worker_does_not_listen_to_the_job_firehose(q, run_worker, run_until):
    """CN-010: `events` carries a message per job. A worker subscribed there parses
    every one of them to catch a cancellation, which measured as a 7% throughput cost
    before cancellations got a channel of their own."""

    async def proc(job):
        return job.name

    async with run_worker(q, proc, concurrency=2):
        assert await run_until(lambda: _subscribed(q, q.keys.cancel), timeout=5), (
            "the worker never subscribed for cancellations"
        )

        listening = await q.redis.pubsub_channels(q.keys.base + "*")

    assert q.keys.events not in listening


async def _subscribed(q: Queue, channel: str) -> bool:
    return channel in await q.redis.pubsub_channels(channel)


async def test_a_cleanup_that_outlives_a_renewal_is_not_cut_short(q, run_worker):
    """CN-002: a cancellation is ONE signal. The lock keeps reporting it for as long
    as the job is active, so a second delivery must not land inside the processor's
    cleanup and abort the very unwinding the cancellation promised to allow."""
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def proc(job):
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            await asyncio.sleep(0.6)  # spans several lock renewals
            cleaned.set()

    async with run_worker(q, proc, concurrency=2, lock_duration=5000, lock_renew_time=100):
        job = await q.add("long", {})
        await asyncio.wait_for(started.wait(), 10)

        assert await q.cancel_job(job.id) is True

        await asyncio.wait_for(cleaned.wait(), 5)  # the cleanup ran to the end


async def test_a_processor_that_swallows_the_cancellation_is_still_cancelled(q, run_worker):
    """CN-002: the worker asked this job to stop. A processor that catches the
    cancellation and returns a value must not land the job in `completed`: its lock is
    still held and it is still in `active`, so the commit would succeed."""
    started = asyncio.Event()

    async def proc(job):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            return "done anyway"
        return None

    async with run_worker(q, proc, concurrency=2):
        job = await q.add("stubborn", {})
        await asyncio.wait_for(started.wait(), 10)

        assert await q.cancel_job(job.id) is True

        with pytest.raises(JobCancelledError):
            await job.result(timeout=10)

    assert await _state(q, job.id) == "cancelled"
    assert await _count(q, "completed") == 0


async def test_a_cleanup_that_raises_does_not_resurrect_a_cancelled_job(q, run_worker):
    """CN-004: a cleanup failing on the way out is ordinary. It must not turn the
    cancellation into a failure, which with attempts left would run the job again."""
    runs = []
    started = asyncio.Event()

    async def proc(job):
        runs.append(job.name)
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            raise ConnectionError("the cleanup could not reach its store")

    async with run_worker(q, proc, concurrency=2) as w:
        w.on("failed", lambda *a, **k: None)
        job = await q.add("long", {}, attempts=5, backoff=10)
        await asyncio.wait_for(started.wait(), 10)

        assert await q.cancel_job(job.id) is True

        with pytest.raises(JobCancelledError):
            await job.result(timeout=10)
        await asyncio.sleep(0.3)  # a retry would have landed by now

    assert runs == ["long"]
    assert await _state(q, job.id) == "cancelled"
    assert await _count(q, "failed") == 0
    assert await _count(q, "delayed") == 0


async def test_the_lock_is_held_while_a_cancelled_job_cleans_up(q, run_worker, run_until):
    """CN-002: the renewal that delivered a cancellation has to keep renewing. Stop,
    and the lock lapses under a long cleanup: the commit is then refused as a lost
    lock and the stalled sweep re-runs the whole job on another worker."""
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def proc(job):
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            await asyncio.sleep(0.8)  # outlives lock_duration if nobody renews it
            cleaned.set()

    # nothing is published, so the lock is what delivers: the path that used to stop
    async with run_worker(q, proc, concurrency=2, lock_duration=300, lock_renew_time=100):
        job = await q.add("long", {})
        await asyncio.wait_for(started.wait(), 10)
        await q.redis.hset(q.keys.job(job.id), "cancel", "1")  # no PUBLISH

        await asyncio.wait_for(cleaned.wait(), 5)
        # the commit still owns the lock, so it lands instead of being refused
        assert await run_until(lambda: _in_state(q, job.id, "cancelled"), timeout=5)


async def test_a_job_claimed_with_a_cancellation_pending_never_runs(q, run_worker, run_until):
    """CN-004: the claim hands the worker the whole job hash, cancel flag included. A
    job re-queued by the stalled sweep after it was cancelled must not be run from the
    top and killed at its first renewal: it was already told to stop."""
    ran = []

    async def proc(job):
        ran.append(job.name)
        return job.name

    # cancelled while it waits, then put back in the queue as the stalled sweep would
    job = await q.add("doomed", {})
    await q.redis.hset(q.keys.job(job.id), "cancel", "1")

    async with run_worker(q, proc, concurrency=2):
        assert await run_until(lambda: _in_state(q, job.id, "cancelled"), timeout=10)

    assert ran == [], "a job with a cancellation pending was run"
    assert await _count(q, "completed") == 0


async def test_a_cancellation_cascades_upward_as_a_cancellation(q):
    """CN-005: the whole reason `cancelled` is a state of its own is that counting a
    deliberate stop as a failure corrupts the failure signal. An ancestor failed by a
    cancelled child would do exactly that, one bucket at a time."""
    root = await q.add_flow(
        "report",
        {},
        children=[FlowChild("mid", {}, children=[FlowChild("leaf", {}, delay=60_000)])],
    )
    mid = (await q.get_flow(root.id))["children"][0]["job"].id
    leaf = (await q.get_flow(mid))["children"][0]["job"].id

    assert await q.cancel_job(leaf) is True

    assert await _state(q, leaf) == "cancelled"
    assert await _state(q, mid) == "cancelled"  # not "failed": nothing failed
    assert await _state(q, root.id) == "cancelled"
    assert await _count(q, "failed") == 0
    assert await _count(q, "cancelled") == 3

    minute = (await q.metrics(minutes=5))[-1]
    assert minute["failed"] == 0, "a cancellation was counted as a failure"


async def test_removing_a_running_job_stops_its_processor(q, run_worker):
    """Removing an active job took it out of the queue and left its processor running
    with nowhere to report: the worker slot, and any global-concurrency slot, stayed
    occupied until the work happened to end on its own."""
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def proc(job):
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            cleaned.set()

    async with run_worker(q, proc, concurrency=2):
        job = await q.add("long", {})
        await asyncio.wait_for(started.wait(), 10)

        assert await q.remove_job(job.id) is True

        await asyncio.wait_for(cleaned.wait(), 5)  # the processor was stopped too

    assert await q.redis.exists(q.keys.job(job.id)) == 0  # removed, not cancelled
    assert await _count(q, "cancelled") == 0
