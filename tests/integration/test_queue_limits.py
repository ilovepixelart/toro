"""Integration: the queue's own limits, set once and enforced by every claim.

`global_concurrency` and `rate_limit` were worker arguments: every replica had to
carry the same value, a change took a rollout, and a fleet mid-rollout enforced two
caps at once. `Queue.set_limits()` stores them on the queue, where the claim script
reads them, so they apply to every worker at once. The worker arguments still apply
to a queue whose limits were never set.
"""

import asyncio

import pytest
from redis.exceptions import ResponseError

from toro import Job, Queue, Worker

PREFIX = "torotest"


async def _noop(job):
    return None


def _worker(q: Queue, **kw) -> Worker:
    return Worker(q.name, _noop, prefix=PREFIX, connection=q.redis, **kw)


async def test_a_cap_set_on_the_queue_holds_for_workers_started_without_one(q):
    for i in range(3):
        await q.add("job", {"i": i})
    await q.set_limits(global_concurrency=1)
    w1, w2 = _worker(q), _worker(q)

    assert await w1._acquire() is not None  # takes the only slot
    assert await w1._acquire() is None
    assert await w2._acquire() is None
    assert (await q.counts())["wait"] == 2


async def test_a_rate_limit_set_on_the_queue_holds_for_workers_started_without_one(q):
    for i in range(2):
        await q.add("job", {"i": i})
    await q.set_limits(rate_limit={"max": 1, "duration": 60_000})
    w = _worker(q)

    assert await w._acquire() is not None  # spends the one token
    assert await w._acquire() is None  # rate limited: the job stays queued
    assert (await q.counts())["wait"] == 1


async def test_the_queue_limits_replace_the_workers_arguments(q):
    for i in range(3):
        await q.add("job", {"i": i})
    tight = _worker(q, global_concurrency=1)
    await q.set_limits(global_concurrency=2)

    assert await tight._acquire() is not None
    assert await tight._acquire() is not None  # the queue says 2, the worker said 1
    assert await tight._acquire() is None


async def test_limits_set_to_none_lift_a_workers_arguments(q):
    """A queue whose limits were set has limits of its own, even when they are none:
    what the workers were started with no longer applies."""
    for i in range(2):
        await q.add("job", {"i": i})
    capped = _worker(q, global_concurrency=1)
    await q.set_limits()

    assert await q.limits() == {"global_concurrency": None, "rate_limit": None}
    assert await capped._acquire() is not None
    assert await capped._acquire() is not None


async def test_limits_read_back_and_are_absent_when_never_set(q):
    assert await q.limits() is None

    await q.set_limits(global_concurrency=3, rate_limit={"max": 10, "duration": 1000})
    assert await q.limits() == {
        "global_concurrency": 3,
        "rate_limit": {"max": 10, "duration": 1000},
    }

    await q.set_limits(global_concurrency=3)
    assert await q.limits() == {"global_concurrency": 3, "rate_limit": None}


async def test_raising_the_cap_wakes_a_worker_parked_at_it(q, run_worker, run_until):
    """A slot turned away at the cap blocks for `block_timeout`; a cap raised
    meanwhile would leave it parked with room to run. The change wakes it."""
    await q.set_limits(global_concurrency=1)
    hold = asyncio.Event()
    started: list[str] = []

    async def proc(job):
        started.append(job.id)
        await hold.wait()

    opts = {"concurrency": 2, "block_timeout": 5.0, "heartbeat_interval": 0, "stalled_interval": 0}
    async with run_worker(q, proc, **opts):
        await q.add("a", {})
        await q.add("b", {})
        assert await run_until(lambda: len(started) == 1)
        await asyncio.sleep(0.2)  # the other slot is parked at the cap
        assert len(started) == 1

        await q.set_limits(global_concurrency=2)

        assert await run_until(lambda: len(started) == 2, timeout=1.0)
        hold.set()


@pytest.mark.parametrize(
    "bad",
    [
        {"global_concurrency": 0},
        {"global_concurrency": True},
        {"global_concurrency": 2.5},
        {"rate_limit": {"max": 0, "duration": 1000}},
        {"rate_limit": {"max": 1}},
    ],
)
async def test_limits_that_cannot_be_enforced_are_refused(q, bad):
    with pytest.raises(ValueError):
        await q.set_limits(**bad)
    assert await q.limits() is None


async def test_a_queue_limit_that_is_not_a_number_stops_a_finish_before_it_commits(q):
    """A finish read the queue's limits only when it fetched, after it had committed,
    so a field edited into something that is not a number erred past the commit: the
    job was completed and the worker saw an error. The limits are read and checked
    before the first write, like the cap argument, so nothing is committed under a
    limit that cannot be read."""
    await q.add("x", {})
    w = _worker(q)
    loaded = await w._acquire()
    assert loaded is not None
    w._running = True  # a running worker's finish fetches the next job
    await q.redis.hset(q.keys.meta, "globalConcurrency", "x")

    with pytest.raises(ResponseError):
        await w._finish_completed(Job.from_hash(*loaded), None)

    assert (await q.get_job(loaded[0])).state == "active"
    assert await q.redis.get(q.keys.lock(loaded[0])) == w.token


async def test_a_queue_rate_limit_with_no_duration_refuses_the_claim(q):
    """rlMax with rlDuration 0 made the refill infinite and every claim allowed: a
    rate limit that silently meant none. A limit must never fail open."""
    await q.add("x", {})
    await q.redis.hset(q.keys.meta, mapping={"rlMax": 5, "rlDuration": 0})

    with pytest.raises(ResponseError):
        await _worker(q)._acquire()
