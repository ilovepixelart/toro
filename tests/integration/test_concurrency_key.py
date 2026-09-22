"""Integration: jobs that share a `concurrency_key` run one at a time
(docs/specs/concurrency-key.md). A held job waits on its key, not on a worker: it
holds no slot and no place in the queue until the key is free.
"""

import asyncio
import json
import time

import pytest
from redis.exceptions import ResponseError

from toro import FlowChild, Queue, Worker, scripts

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
        if job.name == "holder":
            raise RuntimeError("boom")
        if job.data.get("hold"):
            await gate.wait()
        return job.name

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
        fails = path in ("failed", "failed with its parent")
        opts = {"remove_on_complete": True} if path == "removed at once" else {}
        # Both enqueued before any worker exists: a holder that fails the moment it is
        # claimed cannot free the key before the second add asks for it.
        if path == "failed with its parent":
            # the child holds the key and fails, which fails its parent eagerly
            root = await q.add_flow(
                "report", {}, children=[FlowChild("holder", {}, concurrency_key="k")]
            )
        else:
            await q.add("holder", {"hold": True}, **opts, concurrency_key="k")
        held = await q.add("held", {}, concurrency_key="k")
        assert await _state(q, held.id) == "held"
        async with run_worker(q, failing if fails else proc, concurrency=4) as w:
            w.on("failed", lambda *a, **k: None)
            gate.set()
            if path == "failed with its parent":
                await _until(lambda: _in_state(q, root.id, "failed"))
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


async def test_held_is_a_state(q, run_worker, run_until):
    """CK-008: a held job is listed, counted, searchable and removable like any other,
    and the operations that make no sense for it say so rather than half-working."""
    gate = asyncio.Event()
    _, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=4):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        held = await q.add("held", {"tag": "needle"}, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 1), timeout=10)

        assert [j.id for j in await q.get_jobs("held", 0, -1)] == [held.id]
        assert (await q.get_job(held.id)).state == "held"
        assert [j.id for j in await q.search("held", "needle")] == [held.id]
        assert await q.retry_job(held.id) is False  # it never failed
        assert await q.promote_job(held.id) is False  # it waits on a key, not a clock

        assert await q.clean("held") == 1
        assert await _count(q, "held") == 0
        gate.set()
        assert await run_until(_count_is(q, "completed", 1), timeout=10)

    assert await q.redis.keys(q.keys.base + "ck:*") == []


async def test_a_held_leaf_keeps_its_parent_parked(q, run_worker, run_until):
    """CK-006: a held child has not settled, so its flow waits for it as for any child."""
    gate = asyncio.Event()
    _, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=8):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        root = await q.add_flow("report", {}, children=[FlowChild("leaf", {}, concurrency_key="k")])
        assert await run_until(_count_is(q, "held", 1), timeout=10)

        assert await _in_state(q, root.id, "waiting-children")
        gate.set()
        assert await root.result(timeout=10) == "report"


async def test_removing_a_flow_whose_keyed_child_was_retried_frees_the_key(
    q, run_worker, run_until
):
    """CK-004: a retried child is queued again under its key, and is indexed among its
    flow's finished jobs from its first run. Removing the flow must free the key: no
    job holds it afterwards, so nothing else ever would."""

    gate = asyncio.Event()

    async def proc(job):
        if (
            job.name == "child"
            and json.loads(await q.redis.hget(q.keys.job(job.id), "data"))["fail"]
        ):
            raise RuntimeError("boom")
        await gate.wait()
        return job.name

    async with run_worker(q, proc, concurrency=4) as w:
        w.on("failed", lambda *a, **k: None)
        root = await q.add_flow(
            "report", {}, children=[FlowChild("child", {"fail": True}, concurrency_key="k")]
        )
        await _until(lambda: _in_state(q, root.id, "failed"))
        child = (await _tree_ids(q, root.id))[0]
        await q.redis.hset(q.keys.job(child), "data", json.dumps({"fail": False}))
        assert await q.retry_flow(root.id) >= 1
        await _until(lambda: _in_state(q, child, "active"))  # running, holding the key

        assert await q.remove_job(root.id) is True
        gate.set()

        assert await q.redis.keys(q.keys.base + "ck:*") == [], "a removed flow kept its key"
        runs = await q.add("after", {}, concurrency_key="k")
        assert await runs.result(timeout=10) == "after"


async def _tree_ids(q: Queue, root_id: str) -> list[str]:
    tree = await q.get_flow(root_id)
    assert tree is not None
    return [n["job"].id for n in tree["children"]]


async def test_a_job_that_is_gone_is_not_made_a_holder(q, run_worker, run_until):
    """A queue for a key can name a job whose hash is gone. Handing it the key would
    resurrect it as an empty job, run it, and pin the key to it for good."""
    gate = asyncio.Event()
    _, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=4) as w:
        ran: list[str] = []
        w.on("completed", lambda job, _res: ran.append(job.name))
        holder = await q.add("holder", {"hold": True}, concurrency_key="k")
        real = await q.add("real", {}, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 1), timeout=10)
        await q.redis.zadd(q.keys.held_for("k"), {"ghost": 1})  # a job that no longer exists

        gate.set()
        assert await holder.result(timeout=10) == "holder"
        assert await real.result(timeout=10) == "real"

    assert "" not in ran, "an empty job was run"
    assert await q.redis.get(q.keys.concurrency("k")) is None


async def test_held_jobs_keep_their_order_when_the_queue_empties(q, run_worker, run_until):
    """CK-002: a held job carries a sequence number minted from the queue's counter,
    which starts over whenever nothing is waiting anywhere. A held job IS waiting, so
    the counter has to stand: two jobs held either side of an empty queue still run in
    the order they were added."""
    gate = asyncio.Event()
    started, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=2):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        await q.add("first", {}, concurrency_key="k")
        # the free loop polls an empty queue over and over meanwhile
        assert not await run_until(lambda: _gone(q, q.keys.pc), timeout=1)
        await q.add("second", {}, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 2), timeout=10)

        gate.set()
        assert await run_until(_count_is(q, "completed", 3), timeout=10)

    assert started == ["holder", "first", "second"]


async def _gone(q: Queue, key: str) -> bool:
    return not await q.redis.exists(key)


async def test_a_released_job_keeps_its_place_among_jobs_with_no_key(q, run_worker, run_until):
    """CK-002: a held job waits for its key, not for its turn. Released, it goes back
    where it would have been, ahead of jobs added after it."""
    gate = asyncio.Event()
    started, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=1):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        await q.add("held", {}, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 1), timeout=10)
        await q.add("later", {})

        gate.set()
        assert await run_until(_count_is(q, "completed", 3), timeout=10)

    assert started == ["holder", "held", "later"]


async def test_a_released_job_wakes_a_worker(q, run_worker, run_until):
    """A holder that is removed rather than finished promotes the next job with no
    claim of its own to follow it: the release has to wake an idle worker itself, or
    the job waits out the whole idle poll."""
    _, proc = _recorder(asyncio.Event())

    holder = await q.add("holder", {}, concurrency_key="k", delay=60_000)
    held = await q.add("held", {}, concurrency_key="k")
    async with run_worker(q, proc, concurrency=1, block_timeout=30):
        # the worker pops the marker it armed at startup and blocks: nothing else
        # will arm it, so only the release can end that 30s wait
        assert await run_until(lambda: _gone(q, q.keys.marker), timeout=10)

        assert await q.remove_job(holder.id) is True
        assert await held.result(timeout=5) == "held"  # well inside the idle poll


async def test_a_flow_parent_waits_for_its_own_key(q, run_worker, run_until):
    """CK-006: a parent takes its key when its children settle and it becomes runnable,
    not at enqueue - it was not runnable then."""
    gate = asyncio.Event()
    started, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=8):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        root = await q.add_flow("report", {}, children=[FlowChild("leaf", {})], concurrency_key="k")
        await _until(lambda: _in_state(q, root.id, "held"))

        assert started == ["holder", "leaf"]  # the leaf has no key and ran
        gate.set()
        assert await root.result(timeout=10) == "report"


async def test_only_the_holder_hands_the_key_on(q, run_worker, run_until):
    """A flow parent carries its key from enqueue but takes it only once its children
    settle. One that fails before then must not hand on a key it never held, or the job
    waiting for the real holder runs beside it."""
    gate = asyncio.Event()
    started: list[str] = []

    async def proc(job):
        started.append(job.name)
        if job.name == "leaf":
            raise RuntimeError("boom")  # fails its parent eagerly, which releases it
        if job.data.get("hold"):
            await gate.wait()
        return job.name

    async with run_worker(q, proc, concurrency=8) as w:
        w.on("failed", lambda *a, **k: None)
        holder = await q.add("holder", {"hold": True}, concurrency_key="k")
        await _until(lambda: _in_state(q, holder.id, "active"))
        root = await q.add_flow("report", {}, children=[FlowChild("leaf", {})], concurrency_key="k")
        held = await q.add("held", {}, concurrency_key="k")
        await _until(lambda: _in_state(q, root.id, "failed"))

        assert await q.redis.get(q.keys.concurrency("k")) == holder.id, "the parent gave it away"
        assert await _state(q, held.id) == "held"
        gate.set()
        assert await run_until(_count_is(q, "completed", 2), timeout=10)
    assert started.index("holder") < started.index("held")


@pytest.mark.parametrize("bad", ["a:b", "", 7])
async def test_a_flow_node_and_a_scheduler_validate_their_key(q, bad):
    """CK-007: every way of enqueuing validates the key, or a caller computing one
    from data gets no serialization at all where `add()` would have raised."""
    with pytest.raises(ValueError, match="concurrency_key"):
        await q.add_flow("report", {}, children=[FlowChild("leaf", {}, concurrency_key=bad)])
    with pytest.raises(ValueError, match="concurrency_key"):
        await q.add_scheduler("tick", every=60_000, concurrency_key=bad)


async def test_removing_a_held_job_does_not_scan_the_active_list(q, run_worker, run_until):
    """A held job is in one place. Removing it must not pay the blanket sweep, which
    scans the whole active list - `clean("held")` would do that a thousand times."""
    gate = asyncio.Event()
    _, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=4):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        held = await q.add("held", {}, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 1), timeout=10)

        before = await _lrem_calls(q)
        assert await q.remove_job(held.id) is True
        assert await _lrem_calls(q) == before

        gate.set()
        assert await run_until(_count_is(q, "completed", 1), timeout=10)


async def _lrem_calls(q: Queue) -> int:
    stats = await q.redis.info("commandstats")
    return int(stats.get("cmdstat_lrem", {}).get("calls", 0))


async def test_roots_listings_know_the_held_state(q, run_worker, run_until):
    """`held` is a JobState, so every listing that takes one has to answer for it."""
    gate = asyncio.Event()
    _, proc = _recorder(gate)

    async with run_worker(q, proc, concurrency=4):
        await q.add("holder", {"hold": True}, concurrency_key="k")
        held = await q.add("held", {}, concurrency_key="k")
        assert await run_until(_count_is(q, "held", 1), timeout=10)

        total, roots = await q.get_jobs_roots("held", 0, -1)
        assert (total, [j.id for j in roots]) == (1, [held.id])
        assert (await q.roots_counts())["held"] == 1

        gate.set()
        assert await run_until(_count_is(q, "completed", 2), timeout=10)


async def test_the_job_an_add_returns_knows_it_is_held(q):
    """`add()` answers with a Job. One parked on a key that reads `wait` sends a caller
    looking for a worker that was never the problem."""
    await q.add("holder", {}, concurrency_key="k")

    held = await q.add("held", {}, concurrency_key="k")

    assert held.state == "held"
    assert (await q.get_job(held.id)).state == "held"


async def test_an_add_that_enqueued_nothing_answers_for_the_job_that_is_there(q):
    """A dedup hit and an id replay return an existing job's id; its state is that
    job's, not a guess about the add that did nothing."""
    first = await q.add("job", {}, job_id="fixed", concurrency_key="k")
    behind = await q.add("behind", {}, concurrency_key="k")
    assert (first.state, behind.state) == ("wait", "held")

    assert (await q.add("job", {}, job_id="fixed")).state == "wait"  # the id replay
    await q.redis.hset(q.keys.job("fixed"), "state", "active")
    assert (await q.add("job", {}, job_id="fixed")).state == "active"


async def test_a_removal_with_no_clock_changes_nothing(q):
    """REMOVE_JOB grew a `now` argument when a removal started handing keys on. A
    caller still on the old shape has to fail the call, not halfway through it: Redis
    rolls nothing back, so a check after the first write leaves the removal standing
    and the promoted job in no collection at all."""
    holder = await q.add("holder", {}, concurrency_key="k")
    behind = await q.add("behind", {}, concurrency_key="k")
    sha = await q.redis.script_load(scripts.REMOVE_JOB)
    keys = q._remove_job_keys()

    with pytest.raises(ResponseError):
        await q.redis.evalsha(sha, len(keys), *keys, holder.id)

    assert await _state(q, holder.id) == "wait"  # nothing was removed
    assert holder.id in await q.redis.zrange(q.keys.prioritized, 0, -1)
    assert await _state(q, behind.id) == "held"  # and nobody was handed the key
    assert await q.redis.get(q.keys.concurrency("k")) == holder.id
    assert await q.redis.zrange(q.keys.held_for("k"), 0, -1) == [behind.id]
