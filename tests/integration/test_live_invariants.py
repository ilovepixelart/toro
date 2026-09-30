"""Property-based invariants under live workers: the producer-side operations of
test_invariants.py against workers that really run, renew locks and sweep, and that
are killed outright mid-job (run() cancelled, never stopped) or have the lock pulled
from under a run, which is what the lock and the stalled sweep exist for.

A snapshot under live workers means something only if it is atomic, so the checker
runs in its live mode: every set read in one MULTI/EXEC, counts() left out (a
second, separate read). Once the fleet is stopped and a worker that is never killed
has drained the queue, the full check runs, and this module's own property:
  5. no job is lost - every job added and not removed by the run ends completed or
     cancelled, and nothing stays active, waiting, delayed, held or locked.
"""

import asyncio
import contextlib
import random

import pytest

from toro import FlowChild as c  # noqa: N813
from toro import Queue, Worker

PREFIX = "torotest"

# Short leases and a fast sweep: a killed worker's job is back within half a second,
# and one that stalls out three times is failed for good, to be retried at the drain.
_WORKER = {
    "lock_duration": 300,
    "lock_renew_time": 100,
    "stalled_interval": 100,
    "max_stalled_count": 3,
    "concurrency": 2,
}


async def _proc(job):
    await asyncio.sleep(job.data.get("ms", 0) / 1000)
    if job.data.get("fail_first") and job.attempts_made == 0:
        raise RuntimeError("boom")  # fails its first run, then succeeds
    return job.name


class _Fleet:
    """Live workers on connections of their own, killed and replaced at random."""

    def __init__(self, q: Queue) -> None:
        self.q = q
        self.workers: list[tuple[Worker, asyncio.Task]] = []

    def start(self) -> None:
        w = Worker(self.q.name, _proc, prefix=PREFIX, **_WORKER)
        self.workers.append((w, asyncio.create_task(w.run())))

    async def kill(self, rng: random.Random) -> None:
        """Cancel a worker's run() outright: no stop(), no drain. The job in flight
        keeps its lock until it expires, which is what a dead process leaves."""
        w, task = self.workers.pop(rng.randrange(len(self.workers)))
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await w.redis.aclose()
        self.start()

    async def stop(self) -> None:
        for w, task in self.workers:
            await w.stop(grace_period=1.0)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.workers.clear()


async def _rand(q, ctx, rng, *names):
    members = await ctx["members"](q)
    pool = sorted(jid for n in names for jid in members[n])
    return rng.choice(pool) if pool else None


def _subtree_ids(node) -> set[str]:
    ids = {node["job"].id}
    for child in node["children"]:
        ids |= _subtree_ids(child)
    return ids


# ---- the random operations; `ctx` is the run's context -----------------------------


async def _op_add(q, ctx, rng):
    ctx["n"] += 1
    kw = {"priority": rng.randint(0, 5), "attempts": rng.randint(1, 3)}
    if rng.random() < 0.3:
        kw["delay"] = rng.randint(1, 100)
    data = {"ms": rng.randint(0, 30), "fail_first": rng.random() < 0.3}
    job = await q.add(f"job{ctx['n']}", data, **kw)
    ctx["added"].add(job.id)


async def _op_add_flow(q, ctx, rng):
    ctx["n"] += 1
    kids = [
        c(
            f"child{ctx['n']}-{i}",
            {"ms": rng.randint(0, 20), "fail_first": rng.random() < 0.3},
            on_fail=rng.choice(["fail_parent", "continue"]),
        )
        for i in range(rng.randint(1, 3))
    ]
    root = await q.add_flow(f"flow{ctx['n']}", {}, children=kids)
    tree = await q.get_flow(root.id)
    assert tree is not None
    ctx["added"] |= _subtree_ids(tree)


async def _op_retry(q, ctx, rng):
    jid = await _rand(q, ctx, rng, "failed")
    if jid:
        await q.retry_job(jid)


async def _op_remove(q, ctx, rng):
    jid = await _rand(q, ctx, rng, "prioritized", "delayed", "completed", "failed", "cancelled")
    if not jid:
        return
    tree = await q.get_flow(jid)  # a parent goes with its subtree
    ids = _subtree_ids(tree) if tree else {jid}
    if await q.remove_job(jid):
        ctx["gone"] |= ids


async def _op_cancel(q, ctx, rng):
    jid = await _rand(q, ctx, rng, "prioritized", "delayed", "active", "waiting_children")
    if jid:
        await q.cancel_job(jid, reason="fuzz")


async def _op_promote(q, ctx, rng):
    jid = await _rand(q, ctx, rng, "delayed")
    if jid:
        await q.promote_job(jid)


async def _op_pause(q, ctx, rng):
    await q.pause()


async def _op_resume(q, ctx, rng):
    await q.resume()


async def _op_kill(q, ctx, rng):
    ctx["kills"] += 1
    await ctx["fleet"].kill(rng)


async def _op_expire_lock(q, ctx, rng):
    """A lease run out under a run still going, what a stalled event loop does: the
    sweep hands the job on, and the first run's finish is refused as lock lost."""
    jid = await _rand(q, ctx, rng, "active")
    if jid:
        await q.redis.delete(q.keys.lock(jid))


_OPS = [
    _op_add,
    _op_add,
    _op_add,
    _op_add_flow,
    _op_retry,
    _op_remove,
    _op_cancel,
    _op_promote,
    _op_pause,
    _op_resume,
    _op_kill,
    _op_expire_lock,
]


async def _quiet(q: Queue) -> bool:
    counts = await q.counts()
    live = ("wait", "active", "delayed", "waiting-children", "held", "failed")
    return all(counts[state] == 0 for state in live)


async def _drain(q: Queue, run_until) -> None:
    """Run everything down with a worker that is never killed: what stalled out or
    failed is retried, what is delayed is promoted, until nothing is left to run."""
    w = Worker(q.name, _proc, prefix=PREFIX, **_WORKER)
    task = asyncio.create_task(w.run())
    try:
        for _ in range(200):
            await q.retry_all_failed()
            for jid in await q.redis.zrange(q.keys.delayed, 0, -1):
                await q.promote_job(jid)
            if await _quiet(q):
                return
            await asyncio.sleep(0.05)
        raise AssertionError(f"the queue never drained: {await q.counts()}")
    finally:
        await w.stop(grace_period=1.0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.parametrize("seed", range(6))
async def test_no_job_is_lost_under_live_workers_killed_at_random(
    q, seed, invariants, members, run_until
):
    rng = random.Random(seed)  # noqa: S311 - a reproducible test fuzzer, not crypto
    fleet = _Fleet(q)
    for _ in range(3):
        fleet.start()
    ctx = {"n": 0, "kills": 0, "added": set(), "gone": set(), "members": members, "fleet": fleet}
    try:
        for _ in range(50):
            await rng.choice(_OPS)(q, ctx, rng)
            await asyncio.sleep(rng.uniform(0, 0.02))
            await invariants(q, live=True)
        await q.resume()  # the run may have ended paused
        await fleet.stop()
        await _drain(q, run_until)
    finally:
        await fleet.stop()

    # the op sequence is the seed's, so this cannot flake: a seed that kills nobody
    # would be a seed that proves nothing about recovery
    assert ctx["kills"] > 0, "the run never killed a worker"
    await invariants(q)
    now = await members(q)
    expected = ctx["added"] - ctx["gone"]
    settled = now["completed"] | now["cancelled"]
    assert expected <= settled, f"lost: {sorted(expected - settled)}"
    assert await q.redis.keys(q.keys.base + "*:lock") == []
