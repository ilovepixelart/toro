"""Integration: flow corner cases - the edges of the state machine.

Each test pins one corner: delayed/retried/stalled children settling exactly
once, policy interactions, admin actions (remove/retry/clean) against live
flows, and JSON edge values surviving the pre-encoded round trip.
"""

import asyncio

import pytest

from toro import FlowChild as c  # noqa: N813 - `c("fetch", ...)` keeps trees readable
from toro import Queue, Worker

PREFIX = "torotest"


async def _count(q, state):
    return (await q.counts())[state]


def _count_is(q, state, n):
    async def check():
        return (await q.counts())[state] == n

    return check


async def test_delayed_child_releases_parent_after_promotion(q, run_worker, run_until):
    async def proc(job):
        return "ok" if job.name == "fetch" else "done"

    parent = await q.add_flow("report", {}, children=[c("fetch", {}, delay=150)])
    assert await _count(q, "delayed") == 1  # the child waits out its delay first

    async with run_worker(q, proc):
        assert await run_until(_count_is(q, "completed", 2))
    assert (await q.get_job(parent.id)).state == "completed"


async def test_child_retry_then_success_settles_exactly_once(q, run_worker, run_until):
    attempts = 0

    async def proc(job):
        nonlocal attempts
        if job.name == "flaky":
            attempts += 1
            if attempts < 3:
                raise RuntimeError("transient")
            return "recovered"
        return await job.children_results()

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("flaky", {}, attempts=3)])
        assert await run_until(_count_is(q, "completed", 2))

    done = await q.get_job(parent.id)
    assert list(done.returnvalue.values()) == ["recovered"]  # one entry, not three
    assert await _count(q, "failed") == 0


async def test_all_children_fail_continue_parent_still_runs(q, run_worker, run_until):
    seen = {}

    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        seen["results"] = await job.children_results()
        seen["failures"] = await job.failed_children()
        return "survived"

    async with run_worker(q, proc):
        parent = await q.add_flow(
            "report",
            {},
            children=[c("bad", {}, on_fail="continue"), c("bad", {}, on_fail="continue")],
        )
        assert await run_until(_count_is(q, "completed", 1))

    assert (await q.get_job(parent.id)).returnvalue == "survived"
    assert seen["results"] == {}
    assert sorted(seen["failures"].values()) == ["boom", "boom"]


async def test_fail_parent_sibling_overrides_continue_record(q, run_worker, run_until):
    async def proc(job):
        if job.name == "tolerated":
            raise RuntimeError("recorded")
        if job.name == "fatal":
            await asyncio.sleep(0.25)  # the tolerated child settles first
            raise RuntimeError("boom")
        return "never"

    async with run_worker(q, proc, concurrency=2):
        parent = await q.add_flow(
            "report", {}, children=[c("tolerated", {}, on_fail="continue"), c("fatal", {})]
        )
        assert await run_until(_count_is(q, "failed", 3))  # both children + the parent

    failed_parent = await q.get_job(parent.id)
    assert failed_parent.state == "failed"
    assert "boom" in failed_parent.failed_reason  # the fatal child named, not the tolerated one
    # the tolerated child's record survives for the post-mortem
    assert list((await q.failed_children(parent.id)).values()) == ["recorded"]


async def test_stall_recovery_rerun_settles_parent_once(q, run_worker, run_until):
    parent = await q.add_flow("report", {}, children=[c("fetch", {})])
    flow = await q.get_flow(parent.id)
    cid = flow["children"][0]["job"].id

    # The claiming worker dies; the sweep recovers the child (counter under the
    # limit) instead of failing it - the parent must simply keep waiting.
    await q.redis.zrem(q.keys.prioritized, cid)
    await q.redis.rpush(q.keys.active, cid)
    w = Worker(q.name, lambda j: None, prefix=PREFIX, max_stalled_count=2, connection=q.redis)
    await w.check_stalled(throttle_ms=0)  # mark
    _, recovered = await w.check_stalled(throttle_ms=0)  # recover to wait
    assert recovered == [cid]
    assert await _count(q, "waiting-children") == 1  # still parked, not settled

    async def proc(job):
        return "ok" if job.name == "fetch" else (await job.children_results())

    async with run_worker(q, proc):
        assert await run_until(_count_is(q, "completed", 2))
    assert list((await q.get_job(parent.id)).returnvalue.values()) == ["ok"]


async def test_remove_middle_node_releases_root(q):
    root = await q.add_flow("root", {}, children=[c("mid", {}, children=[c("leaf", {})])])
    flow = await q.get_flow(root.id)
    mid_id = flow["children"][0]["job"].id
    leaf_id = flow["children"][0]["children"][0]["job"].id

    assert await q.remove_job(mid_id)

    assert await q.get_job(mid_id) is None
    assert await q.get_job(leaf_id) is None  # the subtree went with it
    released = await q.get_job(root.id)
    assert released.state == "wait"  # nothing left to wait for
    assert await _count(q, "wait") == 1


async def test_clean_waiting_children_removes_whole_flows(q):
    await q.add_flow("root", {}, children=[c("a", {}), c("b", {})])
    assert await q.clean("waiting-children") == 1  # one parked parent cleaned

    counts = await q.counts()
    # removing a parent takes its subtree: nothing may survive anywhere
    assert all(n == 0 for n in counts.values()), counts


async def test_retry_failed_child_after_parent_failed(q, run_worker, run_until):
    fail = True

    async def proc(job):
        if job.name == "bad" and fail:
            raise RuntimeError("boom")
        return "fixed" if job.name == "bad" else "report-ran"

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("bad", {})])
        assert await run_until(_count_is(q, "failed", 2))

        fail = False
        cid = (await q.get_flow(parent.id))["children"][0]["job"].id
        assert await q.retry_job(cid)
        assert await run_until(_count_is(q, "completed", 1))  # the child recovered

    # v1 semantics, pinned: a child retried to success does NOT resurrect the parent
    assert (await q.get_job(parent.id)).state == "failed"


async def test_retry_reparks_failed_parent_until_children_settle(q, run_worker, run_until):
    fail = True

    async def proc(job):
        if job.name == "bad" and fail:
            raise RuntimeError("boom")
        return "fixed" if job.name == "bad" else "report-ran"

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("bad", {})])
        assert await run_until(_count_is(q, "failed", 2))

        # retrying the parent re-arms the barrier - it must NOT run with
        # partial results while its failed child is unsettled
        assert await q.retry_job(parent.id)
        assert (await q.get_job(parent.id)).state == "waiting-children"
        assert await _count(q, "completed") == 0

        # retrying the child completes it, which releases the re-parked parent
        fail = False
        cid = (await q.get_flow(parent.id))["children"][0]["job"].id
        assert await q.retry_job(cid)
        assert await run_until(_count_is(q, "completed", 2))

    assert (await q.get_job(parent.id)).returnvalue == "report-ran"


async def test_retry_all_failed_recovers_a_whole_flow(q, run_worker, run_until):
    fail = True

    async def proc(job):
        if job.name == "bad" and fail:
            raise RuntimeError("boom")
        return "ok" if job.name == "bad" else sorted((await job.children_results()).values())

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("bad", {})])
        assert await run_until(_count_is(q, "failed", 2))

        fail = False
        # the incident-recovery sweep: parent and child both in `failed`, any order
        assert await q.retry_all_failed() == 2
        assert await run_until(_count_is(q, "completed", 2))

    assert (await q.get_job(parent.id)).returnvalue == ["ok"]


async def test_clean_completed_preserves_pending_parent_results(q, run_worker, run_until):
    async def proc(job):
        if job.name == "fast":
            return "fast-done"
        if job.name == "slow":
            await asyncio.sleep(0.5)
            return "slow-done"
        return sorted((await job.children_results()).values())

    async with run_worker(q, proc, concurrency=2):
        parent = await q.add_flow("report", {}, children=[c("fast", {}), c("slow", {})])
        assert await run_until(_count_is(q, "completed", 1))  # fast child done, slow running

        # routine history cleanup while the flow is still in flight must not
        # destroy the parent's already-collected results
        assert await q.clean("completed") == 1
        assert await run_until(_count_is(q, "completed", 2))  # slow child + parent

    assert (await q.get_job(parent.id)).returnvalue == ["fast-done", "slow-done"]


async def test_retried_continue_child_rejoins_the_barrier(q, run_worker, run_until):
    fail = True

    async def proc(job):
        if job.name == "flaky" and fail:
            raise RuntimeError("first-run")
        if job.name == "flaky":
            return "second-run"
        if job.name == "slow":
            return "slow-ok"
        return {"results": await job.children_results(), "failures": await job.failed_children()}

    async with run_worker(q, proc):
        parent = await q.add_flow(
            "report",
            {},
            # the slow sibling is delayed so the parent stays parked while we retry
            children=[c("flaky", {}, on_fail="continue"), c("slow", {}, delay=800)],
        )
        assert await run_until(_count_is(q, "failed", 1))

        fail = False
        cid = (await q.get_flow(parent.id))["children"][0]["job"].id
        assert await q.retry_job(cid)
        assert await run_until(_count_is(q, "completed", 3), timeout=10.0)

    report = (await q.get_job(parent.id)).returnvalue
    assert sorted(report["results"].values()) == ["second-run", "slow-ok"]
    assert report["failures"] == {}  # the stale :cfail record was cleared by the retry


async def test_stall_escalated_job_resolves_result_waiters(q):
    parent = await q.add_flow("report", {}, children=[c("doomed", {})])
    cid = (await q.get_flow(parent.id))["children"][0]["job"].id

    waiter = asyncio.create_task(q.result(cid, timeout=10))
    await asyncio.sleep(0.1)  # let the waiter subscribe

    await q.redis.zrem(q.keys.prioritized, cid)
    await q.redis.rpush(q.keys.active, cid)
    w = Worker(q.name, lambda j: None, prefix=PREFIX, max_stalled_count=0, connection=q.redis)
    await w.check_stalled(throttle_ms=0)
    await w.check_stalled(throttle_ms=0)  # escalates to failed

    from toro import JobFailedError

    with pytest.raises(JobFailedError, match="stalled"):
        await waiter  # the event published from the sweep, not a 10s timeout


async def test_remove_on_fail_applies_to_eagerly_failed_parents(q, run_worker, run_until):
    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        return "never"

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("bad", {})], remove_on_fail=True)
        assert await run_until(_count_is(q, "failed", 1))  # only the child remains

    # the eagerly-failed parent honored remove_on_fail: hash + aux keys gone
    assert await q.get_job(parent.id) is None
    for key in (q.keys.deps(parent.id), q.keys.results(parent.id), q.keys.cfail(parent.id)):
        assert not await q.redis.exists(key)


async def test_no_orphan_keys_after_parent_fully_trimmed(q, run_worker, run_until):
    async def proc(job):
        if job.name == "slow":
            await asyncio.sleep(0.4)
        return "ok"

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("slow", {})])
        # simulate a retention trim deleting the parked parent out from under
        # the still-running child (hash + aux keys + park entry, like delJobs)
        await q.redis.delete(
            q.keys.job(parent.id),
            q.keys.deps(parent.id),
            q.keys.results(parent.id),
            q.keys.cfail(parent.id),
        )
        await q.redis.zrem(q.keys.waiting_children, parent.id)
        assert await run_until(_count_is(q, "completed", 1))

    # the late child's settle must not recreate aux keys nothing will ever delete
    assert not await q.redis.exists(q.keys.results(parent.id))


async def test_default_delay_cannot_reach_interior_nodes(q):
    q2 = Queue(q.name, prefix=PREFIX, default_job_options={"delay": 5000})
    try:
        with pytest.raises(ValueError, match="can't be delayed"):
            await q2.add_flow("report", {}, children=[c("fetch", {})])
        counts = await q.counts()
        assert all(n == 0 for n in counts.values()), counts  # rejected before Redis
    finally:
        await q2.close()


async def test_flow_added_metric_counts_every_node(q):
    await q.add_flow("report", {}, children=[c("a", {}), c("b", {})])
    point = (await q.metrics(minutes=1))[-1]
    assert point["added"] == 3  # batched into one increment, but counts all nodes


async def test_get_flow_depth_bound(q):
    root = await q.add_flow("root", {}, children=[c("mid", {}, children=[c("leaf", {})])])
    flow = await q.get_flow(root.id, depth=1)
    assert len(flow["children"]) == 1
    assert flow["children"][0]["children"] == []  # the recursion stopped here


async def test_remove_parent_while_child_active_is_safe(q, run_worker, run_until):
    started = asyncio.Event()

    async def proc(job):
        started.set()
        await asyncio.sleep(0.5)
        return "too late"

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("slow", {})])
        await asyncio.wait_for(started.wait(), timeout=5)
        assert await q.remove_job(parent.id)  # subtree removal yanks the live child
        # the worker's commit must hit the token guard (lock gone), not error
        assert await run_until(_count_is(q, "active", 0))
        await asyncio.sleep(0.2)  # let a wrong implementation commit something

    counts = await q.counts()
    assert all(n == 0 for n in counts.values()), counts  # nothing survived, nothing leaked


async def test_child_result_json_edges_survive_verbatim(q, run_worker, run_until):
    seen = {}

    async def proc(job):
        if job.name == "emit":
            return job.data["value"]
        seen["results"] = await job.children_results()
        return "done"

    values = [None, [], {"a": [1]}]
    async with run_worker(q, proc, concurrency=3):
        parent = await q.add_flow("report", {}, children=[c("emit", {"value": v}) for v in values])
        assert await run_until(_count_is(q, "completed", 4))

    flow = await q.get_flow(parent.id)
    expected = {n["job"].id: n["job"].data["value"] for n in flow["children"]}
    assert seen["results"] == expected  # `[]` stayed a list, `None` stayed None


async def test_flow_node_cap_rejects_before_touching_redis(q):
    too_many = [c("leaf", {}) for _ in range(1000)]  # 1000 children + parent = 1001
    with pytest.raises(ValueError, match="limit"):
        await q.add_flow("root", {}, children=too_many)
    counts = await q.counts()
    assert all(n == 0 for n in counts.values()), counts  # validation never hit Redis


async def test_default_job_options_apply_to_children(q, run_worker, run_until):
    attempts = 0

    async def proc(job):
        nonlocal attempts
        if job.name == "flaky":
            attempts += 1
            if attempts == 1:
                raise RuntimeError("transient")
            return "ok"
        return "done"

    q2 = Queue(q.name, prefix=PREFIX, default_job_options={"attempts": 2})
    try:
        async with run_worker(q, proc):
            parent = await q2.add_flow("report", {}, children=[c("flaky", {})])
            assert await run_until(_count_is(q, "completed", 2))

        flow = await q.get_flow(parent.id)
        assert flow["children"][0]["job"].attempts_made == 2  # the default reached the child
    finally:
        await q2.close()


# ---- pins from the deep-dive: documented behavior, now tested ---------------------


async def test_parent_result_raises_on_flow_failure(q, run_worker):
    from toro import JobFailedError

    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        return "never"

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("bad", {})])
        with pytest.raises(JobFailedError, match=r"child \d+ failed"):
            await parent.result(timeout=10)


async def test_eager_failure_reason_compounds_per_ancestor(q, run_worker, run_until):
    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        root = await q.add_flow("root", {}, children=[c("mid", {}, children=[c("bad", {})])])
        assert await run_until(_count_is(q, "failed", 3))

    reason = (await q.get_job(root.id)).failed_reason
    # the chain narrates the path down to the real failure
    assert reason.count("child") == 2
    assert reason.endswith("boom")


async def test_late_sibling_results_collect_into_failed_parent(q, run_worker, run_until):
    """Documented: siblings aren't cancelled and their results still collect -
    that's what makes a later parent retry see them."""

    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        await asyncio.sleep(0.3)
        return "slow-ok"

    async with run_worker(q, proc, concurrency=2):
        parent = await q.add_flow("report", {}, children=[c("bad", {}), c("slow", {})])
        assert await run_until(_count_is(q, "failed", 2))
        assert await run_until(_count_is(q, "completed", 1))

    assert (await q.get_job(parent.id)).state == "failed"
    assert "slow-ok" in (await q.children_results(parent.id)).values()


async def test_get_flow_truncates_at_default_depth(q):
    node = c("leaf", {})
    for lvl in range(12):
        node = c(f"mid-{lvl}", {}, children=[node])
    root = await q.add_flow("root", {}, children=[node])

    def depth_of(n):
        return 1 + max((depth_of(ch) for ch in n["children"]), default=0)

    assert depth_of(await q.get_flow(root.id)) == 11  # default depth=10 -> 11 levels
    assert depth_of(await q.get_flow(root.id, depth=20)) == 14  # the whole tree


async def test_failed_metric_counts_child_and_each_ancestor(q, run_worker, run_until):
    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        await q.add_flow("root", {}, children=[c("mid", {}, children=[c("bad", {})])])
        assert await run_until(_count_is(q, "failed", 3))

    point = (await q.metrics(minutes=1))[-1]
    assert point["failed"] == 3  # the leaf + both eagerly-failed ancestors


async def test_add_flow_publishes_one_added_event(q):
    pubsub = q.redis.pubsub()
    await pubsub.subscribe(q.keys.events)
    try:
        parent = await q.add_flow("report", {}, children=[c("a", {}), c("b", {})])
        added = []
        for _ in range(20):
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.05)
            if msg is None:
                continue
            import json as _json

            data = _json.loads(msg["data"])
            if data.get("event") == "added":
                added.append(data["jobId"])
        assert added == [parent.id]  # one announce for the whole tree, the root's id
    finally:
        await pubsub.aclose()


async def test_on_fail_is_mutable_after_enqueue(q, run_worker, run_until):
    """The design claim: the policy is a plain hash field, so tooling can
    change its mind after the flow exists."""
    seen = {}

    async def proc(job):
        if job.name == "bad":
            raise RuntimeError("boom")
        seen["failures"] = await job.failed_children()
        return "done"

    parent = await q.add_flow("report", {}, children=[c("bad", {})])  # fail_parent default
    cid = (await q.get_flow(parent.id))["children"][0]["job"].id
    await q.redis.hset(q.keys.job(cid), "onFail", "continue")  # change of heart

    async with run_worker(q, proc):
        assert await run_until(_count_is(q, "completed", 1))  # the parent RAN

    assert (await q.get_job(parent.id)).state == "completed"
    assert list(seen["failures"].values()) == ["boom"]


async def test_count_retention_keeps_eagerly_failed_parent(q, run_worker, run_until):
    """remove_on_fail=N (the count form) through the eager-fail Lua path."""

    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("bad", {})], remove_on_fail=5)
        assert await run_until(_count_is(q, "failed", 2))

    kept = await q.get_job(parent.id)
    assert kept is not None and kept.state == "failed"  # well under keep-5, retained


async def test_add_flow_on_paused_queue_waits_for_resume(q, run_worker, run_until):
    async def proc(job):
        return "ok" if job.name == "fetch" else "done"

    await q.pause()
    async with run_worker(q, proc):
        parent = await q.add_flow("report", {}, children=[c("fetch", {})])
        await asyncio.sleep(0.3)  # a paused queue must not start the children
        counts = await q.counts()
        assert counts["wait"] == 1 and counts["completed"] == 0

        await q.resume()
        assert await run_until(_count_is(q, "completed", 2))
    assert (await q.get_job(parent.id)).state == "completed"


async def test_children_run_in_priority_order(q, run_worker, run_until):
    order = []

    async def proc(job):
        order.append(job.data.get("tag"))
        if job.name == "report":
            return "done"
        return "ok"

    parent = await q.add_flow(
        "report",
        {},
        children=[
            c("fetch", {"tag": "low"}),  # declared first, priority 0
            c("fetch", {"tag": "high"}, priority=5),
        ],
    )
    async with run_worker(q, proc):  # concurrency 1: strict claim order
        assert await run_until(_count_is(q, "completed", 3))

    assert order == ["high", "low", None]  # priority beats declaration order
    assert (await q.get_job(parent.id)).state == "completed"


# ---- retry_flow: one-call whole-flow recovery ------------------------------------


async def test_retry_flow_recovers_a_whole_failed_flow(q, run_worker, run_until):
    fail = True

    async def proc(job):
        if job.name == "bad":
            if fail:
                raise RuntimeError("boom")
            return "fixed"
        return sorted((await job.children_results()).values())

    async with run_worker(q, proc, concurrency=2):
        parent = await q.add_flow("report", {}, children=[c("bad", {}), c("bad", {})])
        assert await run_until(_count_is(q, "failed", 3))  # both children + the parent

        fail = False
        n = await q.retry_flow(parent.id)
        assert n == 3  # parent + both failed children, all re-driven in one call
        assert await run_until(_count_is(q, "completed", 3))

    assert (await q.get_job(parent.id)).returnvalue == ["fixed", "fixed"]


async def test_retry_flow_leaves_completed_children_untouched(q, run_worker, run_until):
    fail_bad = True

    async def proc(job):
        if job.name == "good":
            return "g"
        if job.name == "bad":
            if fail_bad:
                raise RuntimeError("boom")
            return "b"
        return await job.children_results()

    async with run_worker(q, proc, concurrency=2):
        parent = await q.add_flow("report", {}, children=[c("good", {}), c("bad", {})])
        assert await run_until(_count_is(q, "failed", 2))  # bad child + the parent

        fail_bad = False
        n = await q.retry_flow(parent.id)
        assert n == 2  # only the parent and the failed child - good was never failed
        assert await run_until(_count_is(q, "completed", 3))

    # the parent ran with BOTH results: good's survived from the first run
    assert sorted((await q.get_job(parent.id)).returnvalue.values()) == ["b", "g"]


async def test_retry_flow_recovers_a_nested_flow(q, run_worker, run_until):
    fail = True

    async def proc(job):
        if job.name == "leaf":
            if fail:
                raise RuntimeError("boom")
            return 1
        return sum((await job.children_results()).values())

    async with run_worker(q, proc):
        root = await q.add_flow("root", {}, children=[c("mid", {}, children=[c("leaf", {})])])
        assert await run_until(_count_is(q, "failed", 3))  # leaf, mid, root all eager-failed

        fail = False
        assert await q.retry_flow(root.id) == 3
        assert await run_until(_count_is(q, "completed", 3))

    assert (await q.get_job(root.id)).returnvalue == 1


async def test_retry_flow_on_unknown_id_is_a_noop(q):
    assert await q.retry_flow("nope") == 0
