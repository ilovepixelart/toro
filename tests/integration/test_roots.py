"""Integration: the root-first listing API (get_jobs_roots / roots_counts).

A root-first dashboard hides flow children - they live only in the parent's
tree - and shows each flow as its root job moving through the normal state
tabs. toro answers that directly off a `children` index (every node with a
parentId), so the lists and counts are EXACT and UNBOUNDED: no scan cap, and a
deep page still resolves. Roots of a state = state \\ children.
"""

import time

import pytest

from toro import FlowChild as c  # noqa: N813 - `c("fetch", ...)` keeps trees readable
from toro.queue import Queue

PREFIX = "torotest"


async def _seed_completed(
    q: Queue, n: int, *, parent: str | None = None, prefix: str = "j"
) -> list[str]:
    """n completed jobs {prefix}0..{prefix}{n-1}, index 0 OLDEST (lowest finish
    score). When `parent` is set they are flow children (a parentId + a
    children-index entry).
    """
    now = int(time.time() * 1000) - n * 1000
    ids = []
    pipe = q.redis.pipeline(transaction=False)
    for i in range(n):
        jid = f"{prefix}{i}"
        ids.append(jid)
        mapping = {
            "id": jid,
            "name": f"name-{jid}",
            "data": "{}",
            "state": "completed",
            "timestamp": now + i * 1000,
            "finishedOn": now + i * 1000,
        }
        if parent is not None:
            mapping["parentId"] = parent
            pipe.zadd(q.keys.children, {jid: now + i * 1000})
        pipe.hset(q.keys.job(jid), mapping=mapping)
        pipe.zadd(q.keys.completed, {jid: now + i * 1000})
    await pipe.execute()
    return ids


# ---- children are hidden everywhere -------------------------------------------------


async def test_plain_jobs_are_all_roots(q):
    added = [(await q.add("task", {"n": i})).id for i in range(5)]
    assert (await q.roots_counts())["wait"] == 5
    total, jobs = await q.get_jobs_roots("wait", 0, 100)
    assert total == 5
    # roots listing matches get_jobs() exactly when there are no flows
    assert [j.id for j in jobs] == [j.id for j in await q.get_jobs("wait", 0, 100)]
    assert set(added) == {j.id for j in jobs}


async def test_roots_exclude_flow_children(q):
    parent = await q.add_flow("report", {}, children=[c("fetch", {"n": 1}), c("fetch", {"n": 2})])
    counts = await q.roots_counts()
    assert counts["wait"] == 0  # both children hidden, even though they're runnable
    assert counts["waiting-children"] == 1  # the parked root parent IS a root

    total, jobs = await q.get_jobs_roots("wait", 0, 100)
    assert total == 0 and jobs == []

    total, jobs = await q.get_jobs_roots("waiting-children", 0, 100)
    assert total == 1 and [j.id for j in jobs] == [parent.id]


async def test_nested_subflow_parent_is_not_a_root(q):
    parent = await q.add_flow(
        "root", {}, children=[c("mid", {}, children=[c("leaf", {}), c("leaf", {})])]
    )
    # raw waiting-children holds root + the interior `mid`; only the true root counts.
    assert (await q.counts())["waiting-children"] == 2
    counts = await q.roots_counts()
    assert counts["waiting-children"] == 1
    total, jobs = await q.get_jobs_roots("waiting-children", 0, 100)
    assert total == 1 and [j.id for j in jobs] == [parent.id]


async def test_plain_root_and_hidden_children_coexist_in_wait(q):
    solo = await q.add("solo", {})
    await q.add_flow("report", {}, children=[c("fetch", {}), c("fetch", {})])
    counts = await q.roots_counts()
    assert counts["wait"] == 1  # just `solo`; the two children are hidden
    total, jobs = await q.get_jobs_roots("wait", 0, 100)
    assert total == 1 and [j.id for j in jobs] == [solo.id]


# ---- settling and removal keep the index honest -------------------------------------


async def test_settled_flow_shows_as_one_completed_root(q, run_worker, run_until):
    async def processor(job):
        return {"ok": True} if job.name == "report" else {"part": job.data["n"]}

    parent = await q.add_flow("report", {}, children=[c("fetch", {"n": 1}), c("fetch", {"n": 2})])
    async with run_worker(q, processor, concurrency=4):
        done = await run_until(_state_is(q, parent.id, "completed"))
    assert done

    assert (await q.counts())["completed"] == 3  # parent + 2 children, raw
    counts = await q.roots_counts()
    assert counts["completed"] == 1  # only the root flow
    total, jobs = await q.get_jobs_roots("completed", 0, 100)
    assert total == 1 and jobs[0].id == parent.id


async def test_removal_prunes_children_from_the_index(q):
    solo = await q.add("solo", {})
    parent = await q.add_flow("report", {}, children=[c("fetch", {}), c("fetch", {})])
    assert await q.redis.zcard(q.keys.children) == 2

    await q.remove_job(parent.id)  # cascades the whole subtree

    counts = await q.roots_counts()
    assert counts["wait"] == 1 and counts["waiting-children"] == 0
    _, jobs = await q.get_jobs_roots("wait", 0, 100)
    assert [j.id for j in jobs] == [solo.id]
    assert await q.redis.zcard(q.keys.children) == 0  # nothing stale left behind


# ---- ordering and unboundedness -----------------------------------------------------


async def test_completed_roots_are_newest_first_and_unbounded(q):
    await _seed_completed(q, 600)  # well past matador's old 500 scan cap
    assert (await q.roots_counts())["completed"] == 600  # exact, not capped

    total, page = await q.get_jobs_roots("completed", 0, 4)
    assert total == 600
    assert [j.id for j in page] == [f"j{i}" for i in (599, 598, 597, 596, 595)]

    total, deep = await q.get_jobs_roots("completed", 550, 554)  # a page past 500
    assert total == 600 and len(deep) == 5


async def test_completed_roots_exclude_completed_children(q):
    roots = await _seed_completed(q, 3)  # j0..j2 plain roots
    await _seed_completed(q, 2, parent="p", prefix="ch")  # completed flow children (hidden)
    counts = await q.roots_counts()
    assert counts["completed"] == 3
    total, jobs = await q.get_jobs_roots("completed", 0, 100)
    assert total == 3 and {j.id for j in jobs} == set(roots)


async def test_end_negative_returns_remaining_roots(q):
    await _seed_completed(q, 7)
    total, jobs = await q.get_jobs_roots("completed", 2, -1)  # from index 2 to the end
    assert total == 7 and len(jobs) == 5


# ---- the active LIST path -----------------------------------------------------------


async def test_active_roots_exclude_running_children(q):
    # `active` is a LIST, not a ZSET - roots are filtered by parentId, not the diff.
    await q.redis.hset(q.keys.job("r1"), mapping={"id": "r1", "name": "root", "state": "active"})
    await q.redis.hset(
        q.keys.job("ch1"),
        mapping={"id": "ch1", "name": "child", "state": "active", "parentId": "r1"},
    )
    await q.redis.rpush(q.keys.active, "r1", "ch1")
    await q.redis.zadd(q.keys.children, {"ch1": 1})

    assert (await q.roots_counts())["active"] == 1
    total, jobs = await q.get_jobs_roots("active", 0, 100)
    assert total == 1 and [j.id for j in jobs] == ["r1"]


async def test_a_running_plain_job_is_an_active_root(q):
    """An active job outside the children index is a root and counts; the count
    must not come from the children it excludes."""
    await q.redis.hset(q.keys.job("r1"), mapping={"id": "r1", "name": "root", "state": "active"})
    await q.redis.rpush(q.keys.active, "r1")

    assert (await q.roots_counts())["active"] == 1


def _state_is(q: Queue, job_id: str, state: str):
    async def check():
        job = await q.get_job(job_id)
        return job is not None and job.state == state

    return check


async def test_empty_state_is_zero_roots(q):
    total, jobs = await q.get_jobs_roots("failed", 0, 100)
    assert total == 0 and jobs == []
    assert (await q.roots_counts())["failed"] == 0


async def test_unknown_state_raises(q):
    with pytest.raises(ValueError, match="unknown state"):
        await q.get_jobs_roots("nope", 0, 10)


async def _page(q: Queue, start: int, end: int, *, chunk: int, deep: int) -> tuple[int, list[str]]:
    """LIST_ROOTS on the completed set, newest first, with the walk's knobs exposed."""
    total, ids = await q._list_roots(
        keys=[q.keys.completed, q.keys.children, q.keys.roots_scratch],
        args=[start, end, 1, chunk, deep],
    )
    return int(total), [i.decode() if isinstance(i, bytes) else i for i in ids]


@pytest.mark.parametrize("chunk", [1, 2, 3, 500])
@pytest.mark.parametrize("deep", [0, 10_000], ids=["diff", "walk"])
async def test_a_page_of_roots_is_the_same_by_walk_and_by_diff(q, chunk, deep):
    """With flows in the queue the roots are found by walking the state (or the
    children index, whichever is smaller) in chunks; a page starting past `deep` is
    diffed instead. Every chunk size and both paths must agree with the plain listing.
    """
    roots = await _seed_completed(q, 7, prefix="r")  # r0 oldest .. r6 newest
    await _seed_completed(q, 12, parent="p", prefix="k")  # more children than roots
    newest_first = list(reversed(roots))

    assert await _page(q, 0, 2, chunk=chunk, deep=deep) == (7, newest_first[:3])
    assert await _page(q, 3, -1, chunk=chunk, deep=deep) == (7, newest_first[3:])
    assert await _page(q, -2, -1, chunk=chunk, deep=deep) == (7, newest_first[-2:])
    assert await _page(q, 0, -2, chunk=chunk, deep=deep) == (7, newest_first[:-1])
    assert await _page(q, 5, 2, chunk=chunk, deep=deep) == (7, [])
    assert await _page(q, 9, 12, chunk=chunk, deep=deep) == (7, [])


@pytest.mark.parametrize("chunk", [1, 2, 500])
async def test_roots_are_counted_by_the_smaller_side(q, chunk):
    """Fewer children than roots walks the index; more walks the state. Both count."""
    await _seed_completed(q, 12, prefix="r")
    await _seed_completed(q, 3, parent="p", prefix="k")
    fewer_children = await q._roots_counts_script(keys=_count_keys(q), args=[chunk])
    await _seed_completed(q, 20, parent="p", prefix="m")
    more_children = await q._roots_counts_script(keys=_count_keys(q), args=[chunk])

    assert int(fewer_children[2]) == 12  # completed is the third count
    assert int(more_children[2]) == 12
    assert (await q.roots_counts())["completed"] == 12


def _count_keys(q: Queue) -> list[str]:
    k = q.keys
    return [
        k.prioritized,
        k.delayed,
        k.completed,
        k.failed,
        k.waiting_children,
        k.active,
        k.held,
        k.children,
        k.cancelled,
    ]
