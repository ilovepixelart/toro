"""Integration: cancelling a job, wherever it is (docs/specs/cancel.md).

A job that has not started is ended at once; a running one is told to stop and its
processor is cancelled where it awaits. Either way it lands in `cancelled`, which is
a terminal state of its own: a cancellation is not a failure.
"""

import pytest

from toro import FlowChild, Queue

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
    assert await _count(q, "cancelled") == 1
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


@pytest.mark.parametrize(("on_fail", "parent"), [("fail_parent", "failed"), ("continue", "wait")])
async def test_a_cancelled_child_settles_its_parent_by_policy(q, on_fail, parent):
    """CN-006: a parent waits on its children, so a cancelled one has to settle it or
    the flow is parked forever. A child that was stopped will never deliver what its
    parent waits for, which is what `on_fail` already decides: one rule, not two."""
    root = await q.add_flow(
        "report", {}, children=[FlowChild("leaf", {}, delay=60_000, on_fail=on_fail)]
    )
    tree = await q.get_flow(root.id)
    leaf = tree["children"][0]["job"].id

    assert await q.cancel_job(leaf) is True

    assert await _state(q, leaf) == "cancelled"
    assert await _in_state(q, root.id, parent)
    if parent == "failed":
        assert "cancelled" in (await q.get_job(root.id)).failed_reason
    else:
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
