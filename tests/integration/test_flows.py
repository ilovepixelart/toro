"""Integration: flows - atomic tree enqueue, fan-in release, failure policies,
stalled escalation, removal and cleanup. See docs/flows-design.md.
"""

import asyncio

import pytest

from toro import FlowChild as c  # noqa: N813 - `c("fetch", ...)` keeps trees readable
from toro import Queue, Worker

PREFIX = "torotest"


async def _count(q, state):
    return (await q.counts())[state]


def _count_is(q, state, n):
    """A run_until predicate: true once the state's count reaches exactly n.
    (An async closure - `lambda: _count(...) == n` would compare a coroutine.)
    """

    async def check():
        return (await q.counts())[state] == n

    return check


# ---- enqueue shape ---------------------------------------------------------------


async def test_add_flow_enqueues_tree_atomically(q):
    parent = await q.add_flow(
        "report", {"id": 7}, children=[c("fetch", {"part": 1}), c("fetch", {"part": 2})]
    )

    counts = await q.counts()
    assert counts["waiting-children"] == 1  # the parent, parked
    assert counts["wait"] == 2  # both leaves, runnable

    loaded = await q.get_job(parent.id)
    assert loaded.state == "waiting-children"

    flow = await q.get_flow(parent.id)
    assert flow["job"].id == parent.id
    assert len(flow["children"]) == 2
    child_ids = [n["job"].id for n in flow["children"]]
    # children point back at the parent; the parent's deps set is the barrier
    for cid in child_ids:
        assert (await q.get_job(cid)).parent_id == parent.id
    assert loaded.children_ids == child_ids  # parents expose their child list
    deps = await q.redis.smembers(q.keys.deps(parent.id))
    assert deps == set(child_ids)


async def test_nested_flow_parks_interior_nodes(q):
    parent = await q.add_flow(
        "root", {}, children=[c("mid", {}, children=[c("leaf", {"n": 1}), c("leaf", {"n": 2})])]
    )
    counts = await q.counts()
    assert counts["waiting-children"] == 2  # root + mid
    assert counts["wait"] == 2  # only the leaves run

    flow = await q.get_flow(parent.id)
    mid = flow["children"][0]
    assert mid["job"].state == "waiting-children"
    assert len(mid["children"]) == 2


async def test_waiting_children_jobs_are_listable(q):
    parent = await q.add_flow("report", {}, children=[c("fetch", {})])
    listed = await q.get_jobs("waiting-children")
    assert [j.id for j in listed] == [parent.id]


async def test_add_flow_validation(q):
    with pytest.raises(ValueError):
        await q.add_flow("report", {}, children=[])  # that's add(), not a flow
    with pytest.raises(ValueError):
        await q.add_flow("report", {}, children=[c("fetch", {})], delay=1000)  # no parent delay
    with pytest.raises(ValueError):  # custom ids invite id-reuse zombies; server ids only
        await q.add_flow("report", {}, children=[c("fetch", {}, job_id="custom")])
    with pytest.raises(ValueError):
        c("fetch", {}, on_fail="explode")  # not a policy


# ---- the happy path: fan-in release and results ----------------------------------


async def test_parent_runs_after_children_with_their_results(q, run_worker, run_until):
    ran = []

    async def proc(job):
        ran.append(job.name)
        if job.name == "fetch":
            return job.data["part"] * 10
        # the parent pulls children results explicitly (no implicit arg injection)
        results = await job.children_results()
        return sum(results.values())

    async with run_worker(q, proc, concurrency=4):
        parent = await q.add_flow(
            "report", {}, children=[c("fetch", {"part": 1}), c("fetch", {"part": 2})]
        )
        assert await run_until(_count_is(q, "completed", 3))

    assert ran[-1] == "report"  # parent strictly after every child
    done = await q.get_job(parent.id)
    assert done.state == "completed"
    assert done.returnvalue == 30
    assert await _count(q, "waiting-children") == 0


async def test_flow_result_waits_for_the_whole_flow(q, run_worker):
    async def proc(job):
        if job.name == "fetch":
            return job.data["part"]
        return sorted((await job.children_results()).values())

    async with run_worker(q, proc, concurrency=4):
        parent = await q.add_flow(
            "report", {}, children=[c("fetch", {"part": 2}), c("fetch", {"part": 1})]
        )
        assert await parent.result(timeout=10) == [1, 2]


async def test_nested_flow_releases_inner_then_root(q, run_worker, run_until):
    order = []

    async def proc(job):
        order.append(job.name)
        if job.name == "leaf":
            return 1
        results = await job.children_results()
        return sum(results.values())

    async with run_worker(q, proc, concurrency=4):
        parent = await q.add_flow(
            "root", {}, children=[c("mid", {}, children=[c("leaf", {}), c("leaf", {})])]
        )
        assert await run_until(_count_is(q, "completed", 4))

    assert order.index("mid") > max(i for i, n in enumerate(order) if n == "leaf")
    assert order[-1] == "root"
    assert (await q.get_job(parent.id)).returnvalue == 2


# ---- failure policies ------------------------------------------------------------


async def test_child_failure_fails_parent_eagerly_by_default(q, run_worker, run_until):
    ran = []

    async def proc(job):
        ran.append(job.name)
        if job.name == "bad":
            raise RuntimeError("boom")
        return "ok"

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("bad", {})])
        assert await run_until(_count_is(q, "failed", 2))  # child AND parent

    assert "report" not in ran  # the parent never ran - it was failed, not processed
    failed_parent = await q.get_job(parent.id)
    assert failed_parent.state == "failed"
    assert "failed" in failed_parent.failed_reason  # names the child
    assert await _count(q, "waiting-children") == 0  # never parked forever on a failure


async def test_fail_parent_propagates_up_the_tree(q, run_worker, run_until):
    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        return "ok"

    async with run_worker(q, proc):
        root = await q.add_flow("root", {}, children=[c("mid", {}, children=[c("bad", {})])])
        # bad fails -> mid fails eagerly -> root fails eagerly
        assert await run_until(_count_is(q, "failed", 3))

    assert (await q.get_job(root.id)).state == "failed"


async def test_child_retries_before_failing_parent(q, run_worker, run_until):
    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("bad", {}, attempts=3)])
        assert await run_until(_count_is(q, "failed", 2))

    flow = await q.get_flow(parent.id)
    assert flow["children"][0]["job"].attempts_made == 3  # exhausted its own retries first


async def test_on_fail_continue_runs_parent_with_failure_report(q, run_worker, run_until):
    seen = {}

    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        if job.name == "good":
            return 42
        seen["results"] = await job.children_results()
        seen["failures"] = await job.failed_children()
        return "done"

    async with run_worker(q, proc):
        parent = await q.add_flow(
            "report", {}, children=[c("good", {}), c("bad", {}, on_fail="continue")]
        )
        assert await run_until(_count_is(q, "completed", 2))  # good child + parent

    assert (await q.get_job(parent.id)).state == "completed"
    assert list(seen["results"].values()) == [42]
    assert list(seen["failures"].values()) == ["boom"]
    assert await _count(q, "failed") == 1  # the child still counts as failed


# ---- flow_view: one projected read for dashboards ---------------------------------


async def test_flow_view_projects_tree_results_and_failures(q, run_worker, run_until):
    # flow_view folds the tree, the collected results and the tolerated failures into
    # one projection (the three reads a dashboard otherwise makes for a flow detail).
    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        if job.name == "good":
            return 42
        return "done"

    async with run_worker(q, proc):
        parent = await q.add_flow(
            "report", {}, children=[c("good", {}), c("bad", {}, on_fail="continue")]
        )
        assert await run_until(_count_is(q, "completed", 2))  # good child + parent

    view = await q.flow_view(parent.id)
    # the tree, same {job, children} shape as get_flow
    assert view.tree["job"].id == parent.id
    assert {n["job"].name for n in view.tree["children"]} == {"good", "bad"}
    # results + tolerated failures, folded into the same projection
    assert list(view.results.values()) == [42]
    assert list(view.failures.values()) == ["boom"]
    # fan-in counts derived over the root's direct children, from one snapshot
    assert view.total == 2
    assert view.done == 1  # good completed
    assert view.failed == 1  # bad failed (on_fail=continue)
    assert view.live is False  # every node terminal


async def test_flow_view_is_live_until_every_node_settles(q):
    # a delayed child keeps the flow moving even with no worker yet running
    parent = await q.add_flow("report", {}, children=[c("a", {}), c("b", {}, delay=600_000)])
    view = await q.flow_view(parent.id)
    assert view.live is True
    assert (view.total, view.done, view.failed) == (2, 0, 0)


async def test_flow_view_none_for_a_missing_job(q):
    assert await q.flow_view("nope") is None


async def test_late_sibling_does_not_resurrect_failed_parent(q, run_worker, run_until):
    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        await asyncio.sleep(0.3)  # still running when the parent fails
        return "slow-ok"

    async with run_worker(q, proc, concurrency=2):
        parent = await q.add_flow("report", {}, children=[c("bad", {}), c("slow", {})])
        assert await run_until(_count_is(q, "failed", 2))
        # the slow sibling finishes AFTER its parent already failed
        assert await run_until(_count_is(q, "completed", 1))
        await asyncio.sleep(0.1)  # give a wrong implementation time to mis-release

    parent_after = await q.get_job(parent.id)
    assert parent_after.state == "failed"  # settled siblings must not re-enqueue it
    assert await _count(q, "wait") == 0


# ---- the crash path: stalled children settle the parent too ----------------------


async def test_stalled_child_escalation_settles_parent(q):
    parent = await q.add_flow("report", {}, children=[c("doomed", {})])
    flow = await q.get_flow(parent.id)
    cid = flow["children"][0]["job"].id

    # Simulate a worker that claimed the child and died: on `active`, no lock.
    await q.redis.zrem(q.keys.prioritized, cid)
    await q.redis.rpush(q.keys.active, cid)

    w = Worker(q.name, lambda j: None, prefix=PREFIX, max_stalled_count=0, connection=q.redis)
    await w.check_stalled(throttle_ms=0)  # pass 1 marks
    failed, _ = await w.check_stalled(throttle_ms=0)  # pass 2 escalates to failed
    assert failed == [cid]

    # the barrier resolved on the crash path: the parent failed with it
    assert (await q.get_job(parent.id)).state == "failed"
    assert await _count(q, "waiting-children") == 0


# ---- removal & cleanup -----------------------------------------------------------


async def test_remove_parent_removes_subtree_and_aux_keys(q):
    parent = await q.add_flow(
        "root", {}, children=[c("mid", {}, children=[c("leaf", {})]), c("leaf2", {})]
    )
    flow = await q.get_flow(parent.id)
    all_ids = [parent.id] + [n["job"].id for n in flow["children"]]
    all_ids.append(flow["children"][0]["children"][0]["job"].id)

    assert await q.remove_job(parent.id)

    for jid in all_ids:
        assert await q.get_job(jid) is None
    for jid in all_ids:
        for key in (q.keys.deps(jid), q.keys.results(jid), q.keys.cfail(jid)):
            assert not await q.redis.exists(key)
    counts = await q.counts()
    assert counts["waiting-children"] == 0 and counts["wait"] == 0


async def test_remove_last_pending_child_releases_parent(q):
    parent = await q.add_flow("report", {}, children=[c("fetch", {})])
    flow = await q.get_flow(parent.id)
    cid = flow["children"][0]["job"].id

    assert await q.remove_job(cid)

    released = await q.get_job(parent.id)
    assert released.state == "wait"  # nothing left to wait for
    assert await _count(q, "waiting-children") == 0
    assert await _count(q, "wait") == 1


async def test_auto_removal_cleans_aux_keys(q, run_worker, run_until):
    async def proc(job):
        if job.name == "fetch":
            return 1
        return sum((await job.children_results()).values())

    async with run_worker(q, proc, concurrency=2):
        parent = await q.add_flow(
            "report", {}, children=[c("fetch", {}), c("fetch", {})], remove_on_complete=True
        )
        # the parent ran and auto-removed itself - its aux keys must go with it
        assert await run_until(lambda: _not_exists(q, parent.id), timeout=5.0)
        assert await _count(q, "completed") == 2  # the children remain


async def _not_exists(q: Queue, jid: str) -> bool:
    if await q.redis.exists(q.keys.job(jid)):
        return False
    for key in (q.keys.deps(jid), q.keys.results(jid), q.keys.cfail(jid)):
        if await q.redis.exists(key):
            return False
    return True


# ---- dashboard surface -----------------------------------------------------------


async def test_queue_side_flow_introspection(q, run_worker, run_until):
    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        if job.name == "good":
            return {"v": 1}
        return "done"

    async with run_worker(q, proc):
        parent = await q.add_flow(
            "report", {}, children=[c("good", {}), c("bad", {}, on_fail="continue")]
        )
        assert await run_until(_count_is(q, "completed", 2))

    # the dashboard reads results/failures without a worker context
    results = await q.children_results(parent.id)
    failures = await q.failed_children(parent.id)
    assert list(results.values()) == [{"v": 1}]
    assert list(failures.values()) == ["boom"]
