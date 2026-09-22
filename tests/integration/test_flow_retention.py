"""Integration: a flow is retained as one unit (docs/specs/flow-retention.md).

Children finish before their parent, so a rank-based trim reaches them first. While
a flow runs, nothing of it is trimmed; when its root is trimmed, the subtree goes.
"""

import asyncio
import time

import pytest

from toro import FlowChild as c  # noqa: N813 - `c("part", ...)` keeps trees readable
from toro import Queue, Worker
from toro.job import DEFAULT_KEEP_COMPLETED
from toro.scripts import LIVE_SCORE

PREFIX = "torotest"


async def _seed_history(q: Queue, n: int, *, newer_than_now: bool = False) -> list[str]:
    """`n` finished jobs, with hashes: older than anything live, or newer than it."""
    base = int(time.time() * 1000) + (60_000 if newer_than_now else -86_400_000)
    pipe = q.redis.pipeline(transaction=False)
    ids = [f"hist{i}" for i in range(n)]
    for i, jid in enumerate(ids):
        pipe.hset(q.keys.job(jid), mapping={"id": jid, "name": "hist", "state": "completed"})
        pipe.zadd(q.keys.completed, {jid: base + i})
    await pipe.execute()
    return ids


async def _present(q: Queue, ids: list[str]) -> int:
    pipe = q.redis.pipeline(transaction=False)
    for jid in ids:
        pipe.exists(q.keys.job(jid))
    return sum(await pipe.execute())


def _holding(gate: asyncio.Event, held: str):
    async def proc(job):
        if job.name == held:
            await gate.wait()
        if job.name == "report":  # the root reads every child's result
            return sorted((await job.children_results()).values())
        return job.name

    return proc


async def _tree_ids(q: Queue, root_id: str) -> list[str]:
    tree = await q.get_flow(root_id)
    assert tree is not None
    return [n["job"].id for n in tree["children"]]


async def test_a_running_flow_keeps_its_finished_children(q, run_worker, run_until):
    """FR-001: two children done, one held, then a full history and a finish that trims.
    On a busy queue this is any flow at all."""
    gate = asyncio.Event()
    async with run_worker(q, _holding(gate, "slow"), concurrency=4):
        root = await q.add_flow("report", {}, children=[c("a", {}), c("b", {}), c("slow", {})])
        assert await run_until(lambda: _count_state(q, "completed", 2))
        done = await q.redis.zrange(q.keys.completed, 0, -1)
        await _seed_history(q, DEFAULT_KEEP_COMPLETED, newer_than_now=True)

        await (await q.add("unrelated", {})).result(timeout=10)  # trims the oldest

        assert await _present(q, done) == 2, "a running flow's finished children were trimmed"
        assert len(await _tree_ids(q, root.id)) == 3  # the tree is whole
        gate.set()
        assert await root.result(timeout=10) == ["a", "b", "slow"]


async def test_live_children_do_not_eat_the_bound(q, run_worker, run_until):
    """FR-002: a running flow with 998 finished children sits beside 999 retained
    jobs; a finish on top must not push any history out."""
    gate = asyncio.Event()
    history = await _seed_history(q, DEFAULT_KEEP_COMPLETED - 1)
    async with run_worker(q, _holding(gate, "slow"), concurrency=16):
        root = await q.add_flow(
            "report", {}, children=[c("part", {"i": i}) for i in range(998)] + [c("slow", {})]
        )
        assert await run_until(lambda: _count_state(q, "completed", len(history) + 998), timeout=30)

        await (await q.add("unrelated", {})).result(timeout=10)

        assert await _present(q, history) == len(history), "live children ate the bound"
        gate.set()
        assert len(await root.result(timeout=30)) == 999


async def _count_state(q: Queue, state: str, n: int) -> bool:
    return (await q.counts())[state] == n


# ---- a settled root places its subtree ---------------------------------------------


async def _scores(q: Queue, ids: list[str]) -> dict[str, float]:
    """Each id's score, in whichever finished set holds it."""
    out = {}
    for jid in ids:
        for key in (q.keys.completed, q.keys.failed):
            score = await q.redis.zscore(key, jid)
            if score is not None:
                out[jid] = score
    return out


async def _nested(q: Queue) -> tuple[str, dict[str, str]]:
    """root -> [mid -> [leaf1, leaf2], side]; returns the root id and ids by name."""
    root = await q.add_flow(
        "report",
        {},
        children=[c("mid", {}, children=[c("leaf1", {}), c("leaf2", {})]), c("side", {})],
    )
    tree = await q.get_flow(root.id)
    assert tree is not None
    names: dict[str, str] = {"report": root.id}

    def walk(node):
        for child in node["children"]:
            names[child["job"].name] = child["job"].id
            walk(child)

    walk(tree)
    return root.id, names


def _placed(scores: dict[str, float], names: dict[str, str], root_score: float) -> None:
    depth = {"report": 0, "mid": 1, "side": 1, "leaf1": 2, "leaf2": 2}
    for name, d in depth.items():
        assert scores[names[name]] == root_score + d, (name, scores[names[name]], root_score)
    assert root_score < LIVE_SCORE


async def _settled(q: Queue, root_id: str) -> bool:
    return await q.redis.hget(q.keys.job(root_id), "state") in ("completed", "failed")


@pytest.mark.parametrize("path", ["completed", "failed by its worker", "failed with a leaf"])
async def test_settled_flow_rides_with_its_root(q, run_worker, run_until, path):
    """FR-003: whichever way the root settles, every finished descendant is re-scored
    to the root's finish time plus its depth: the root is the oldest of its flow, so
    the trim reaches it first and takes the subtree with it."""
    leaf2_done = asyncio.Event()

    async def proc(job):
        if job.name == "leaf2":
            leaf2_done.set()
        if job.name == "leaf1":
            await leaf2_done.wait()  # so the eager failure finds leaf2 already settled
            if path == "failed with a leaf":
                raise RuntimeError("boom")
        if job.name == "report" and path == "failed by its worker":
            raise RuntimeError("boom")
        return job.name

    async with run_worker(q, proc, concurrency=4):
        root_id, names = await _nested(q)
        assert await run_until(lambda: _settled(q, root_id), timeout=10)

    scores = await _scores(q, list(names.values()))
    assert len(scores) == 5, scores  # every node is in a finished set
    _placed(scores, names, scores[root_id])


async def test_a_root_the_sweep_fails_places_its_subtree(q, run_until):
    """FR-003 on the crash path: the root's worker died holding it, and the stalled
    sweep fails it for good."""
    never = asyncio.Event()

    async def proc(job):
        if job.name == "report":
            await never.wait()  # the root's worker is killed while it runs this
        return job.name

    w = Worker(q.name, proc, prefix=PREFIX, concurrency=4, lock_duration=300, stalled_interval=0)
    task = asyncio.create_task(w.run())
    root_id, names = await _nested(q)
    assert await run_until(lambda: _in_state(q, root_id, "active"), timeout=10)
    await w.stop(grace_period=0.1)  # past the grace: the root is abandoned, lock and all
    task.cancel()
    assert await run_until(lambda: _lock_gone(q, root_id), timeout=5)  # its lock ran out

    sweeper = Worker(q.name, proc, prefix=PREFIX, max_stalled_count=0, connection=q.redis)
    await sweeper.check_stalled(throttle_ms=0)
    failed, _ = await sweeper.check_stalled(throttle_ms=0)
    assert failed == [root_id]

    scores = await _scores(q, list(names.values()))
    assert len(scores) == 5, scores
    _placed(scores, names, scores[root_id])


async def _lock_gone(q: Queue, job_id: str) -> bool:
    return not await q.redis.exists(q.keys.lock(job_id))


async def _in_state(q: Queue, job_id: str, state: str) -> bool:
    return await q.redis.hget(q.keys.job(job_id), "state") == state


async def test_a_retried_flow_is_running_again(q, run_worker, run_until):
    """FR-008: retrying a failed root puts its finished descendants back out of the
    trim's reach; when the flow settles again they ride with the root again."""
    fail_once = {"leaf1": True}

    async def proc(job):
        if job.name == "leaf1" and fail_once.pop("leaf1", False):
            raise RuntimeError("boom")
        return job.name

    async with run_worker(q, proc, concurrency=4):
        root_id, names = await _nested(q)
        assert await run_until(lambda: _in_state(q, root_id, "failed"), timeout=10)
        first = await _scores(q, list(names.values()))
        assert first[root_id] < LIVE_SCORE

        assert await q.retry_flow(root_id) >= 1
        live = await _scores(q, [names["leaf2"], names["side"]])  # completed, untouched
        assert all(s > LIVE_SCORE for s in live.values()), live

        assert await run_until(lambda: _in_state(q, root_id, "completed"), timeout=10)
    scores = await _scores(q, list(names.values()))
    _placed(scores, names, scores[root_id])
