"""Property-based invariants: drive the queue through long randomized operation
sequences and assert the global invariants hold after every step, then that every
flow settles once the queue is drained.

A deterministic seeded fuzzer (not Hypothesis): processing is driven by hand via
_acquire + _finish_* so each step is atomic with nothing else mutating - no live
worker, no timing races - and a failing case reproduces exactly from its seed.

The per-step invariants are the shared checker in tests/conftest.py (`invariants`),
which the `q` fixture also runs at the teardown of every passing integration test.
Final invariant after a full drain, this module's own:
  4. flows always settle - waiting-children is empty (no parent stranded).
"""

import random

import pytest

from toro import FlowChild as c  # noqa: N813
from toro import Queue, Worker
from toro.job import Job

PREFIX = "torotest"


async def _process_one(w: Worker, *, succeed: bool) -> bool:
    """Claim one runnable job and finish it (atomic, no fetch-next chaining since
    the worker isn't run()). Returns False when nothing was claimable."""
    loaded = await w._acquire()
    if loaded is None:
        return False
    job_id, fields = loaded
    job = Job.from_hash(job_id, fields)
    if succeed:
        await w._finish_completed(job, {"ok": 1})
    else:
        await w._finish_failed(job, RuntimeError("boom"))
    return True


async def _rand_member(q: Queue, ctr: dict, rng: random.Random, *names: str) -> str | None:
    members = await ctr["members"](q)
    pool = sorted(jid for n in names for jid in members[n])
    return rng.choice(pool) if pool else None


# ---- the random operations (module level, so the test body stays simple) -----------
# `ctr` is the run's context: the job counter, the held jobs of the cap fuzz, and the
# `members` snapshot fixture, which a module-level helper cannot import from conftest.


async def _op_add(q, w, rng, ctr):
    ctr["j"] += 1
    kw = {"priority": rng.randint(0, 5)}
    if rng.random() < 0.3:
        kw["delay"] = rng.randint(1, 50)
    if rng.random() < 0.3:
        kw["attempts"] = rng.randint(1, 3)
    await q.add(f"job{ctr['j']}", {"i": ctr["j"]}, **kw)


async def _op_add_flow(q, w, rng, ctr):
    ctr["j"] += 1
    kids = [
        c(f"child{ctr['j']}-{i}", {}, on_fail=rng.choice(["fail_parent", "continue"]))
        for i in range(rng.randint(1, 3))
    ]
    await q.add_flow(f"flow{ctr['j']}", {}, children=kids)


async def _op_process(q, w, rng, ctr):
    await _process_one(w, succeed=rng.random() < 0.7)


async def _op_retry(q, w, rng, ctr):
    jid = await _rand_member(q, ctr, rng, "failed")
    if jid:
        await q.retry_job(jid)


async def _op_remove(q, w, rng, ctr):
    jid = await _rand_member(q, ctr, rng, "prioritized", "active", "delayed", "completed", "failed")
    if jid:
        await q.remove_job(jid)


async def _op_clean(q, w, rng, ctr):
    await q.clean(rng.choice(["wait", "delayed", "completed", "failed"]))


async def _op_promote(q, w, rng, ctr):
    jid = await _rand_member(q, ctr, rng, "delayed")
    if jid:
        await q.promote_job(jid)


_OPS = [
    _op_add,
    _op_add_flow,
    _op_process,
    _op_process,  # process twice as often as the admin ops
    _op_retry,
    _op_remove,
    _op_clean,
    _op_promote,
]


async def _recover_and_drain(q: Queue, w: Worker) -> None:
    """Fully recover the queue: retry every failed job (re-arming any parked flow -
    the documented retry-all path), promote the delayed, process the runnable as
    success, to quiescence. A flow stranded on a failed child only settles because
    the child is retried too, which is exactly the guarantee under test."""
    for _ in range(5000):
        progressed = bool(await q.retry_all_failed())
        for jid in await q.redis.zrange(q.keys.delayed, 0, -1):
            progressed = await q.promote_job(jid) or progressed
        progressed = await _process_one(w, succeed=True) or progressed
        if not progressed:
            return


async def _settle_diagnostic(q: Queue) -> str:
    parts = []
    for pid in await q.redis.zrange(q.keys.waiting_children, 0, -1):
        info = {}
        for d in await q.redis.smembers(q.keys.deps(pid)):
            exists = await q.redis.exists(q.keys.job(d))
            info[d] = await q.redis.hget(q.keys.job(d), "state") if exists else "GONE"
        parts.append(f"parent {pid}: deps={info}")
    return " | ".join(parts)


@pytest.mark.parametrize("seed", range(12))
async def test_invariants_hold_under_random_ops(q, seed, invariants, members):
    rng = random.Random(seed)  # noqa: S311 - a reproducible test fuzzer, not crypto
    w = Worker(q.name, lambda j: None, prefix=PREFIX, connection=q.redis)
    ctr = {"j": 0, "members": members}

    for _ in range(40):
        await rng.choice(_OPS)(q, w, rng, ctr)
        await invariants(q)

    await _recover_and_drain(q, w)
    await invariants(q)
    settled = (await q.counts())["waiting-children"] == 0
    assert settled, "a flow never settled -> " + await _settle_diagnostic(q)


# ---- the same fuzz under a global concurrency cap ----------------------------------
# _op_process claims and finishes in one step, so `active` never holds more than one
# job and a cap assertion over it would be vacuous. These ops hold claimed jobs open
# and finish them later WITH fetch-next: the swap that lets the cap be enforced in
# MOVE_TO_ACTIVE alone. If a finish script ever grew `active`, this is the tripwire.

_CAP = 2


def _hold(held: list[Job], loaded: tuple[str, dict[str, str]] | None) -> None:
    if loaded is not None:
        held.append(Job.from_hash(*loaded))


async def _op_claim_hold(q, w, rng, ctr):
    _hold(ctr["held"], await w._acquire())


async def _op_finish_held(q, w, rng, ctr):
    held = ctr["held"]
    if not held:
        return
    job = held.pop(rng.randrange(len(held)))
    if rng.random() < 0.7:
        _hold(held, await w._finish_completed(job, {"ok": 1}))
    else:
        _hold(held, await w._finish_failed(job, RuntimeError("boom")))


# _op_process is left out: with fetch-next on it would drop the chained job, which
# would then sit in `active` forever and eat a slot.
_CAP_OPS = [
    _op_add,
    _op_add,
    _op_add_flow,
    _op_claim_hold,
    _op_claim_hold,
    _op_finish_held,
    _op_retry,
    _op_remove,
    _op_clean,
    _op_promote,
]


@pytest.mark.parametrize("seed", range(12))
async def test_cap_holds_under_random_ops(q, seed, invariants, members):
    rng = random.Random(seed)  # noqa: S311 - a reproducible test fuzzer, not crypto
    w = Worker(q.name, lambda j: None, prefix=PREFIX, connection=q.redis, global_concurrency=_CAP)
    w._running = True  # finish with fetch-next, as a running worker does
    ctr = {"j": 0, "held": [], "members": members}
    peak = 0

    for _ in range(60):
        await rng.choice(_CAP_OPS)(q, w, rng, ctr)
        await invariants(q)
        active = await q.redis.llen(q.keys.active)
        assert active <= _CAP, f"{active} active jobs under a cap of {_CAP}"
        peak = max(peak, active)
    assert peak == _CAP, "the run never reached the cap, so it proved nothing"

    w._running = False  # finish without fetching, so the held slots really free up
    for job in ctr["held"]:
        await w._finish_completed(job, {"ok": 1})
    await _recover_and_drain(q, w)
    await invariants(q)
    settled = (await q.counts())["waiting-children"] == 0
    assert settled, "a flow never settled -> " + await _settle_diagnostic(q)


# ---- removal without a readable state ----------------------------------------------

# Every collection REMOVE_JOB sweeps when a job's `state` cannot say where it lives.
_SWEPT_ZSETS = (
    "prioritized",
    "delayed",
    "completed",
    "failed",
    "waiting_children",
    "held",
    "cancelled",
)


async def test_removing_a_job_with_no_state_sweeps_every_collection(q):
    """Covers a hash corrupted by hand: with its `state` field gone, removal cannot
    know where the job lives and has to take it out of every collection, or a stray
    id outlives its hash and is handed to a worker or listed with nothing behind it.
    The id is planted in all of them so each sweep step is on the hook."""
    job = await q.add("j", {})
    await q.redis.hdel(q.keys.job(job.id), "state")
    for name in _SWEPT_ZSETS:
        await q.redis.zadd(getattr(q.keys, name), {job.id: 1})
    await q.redis.lpush(q.keys.active, job.id)

    assert await q.remove_job(job.id) is True

    present = {
        name
        for name in _SWEPT_ZSETS
        if await q.redis.zscore(getattr(q.keys, name), job.id) is not None
    }
    if job.id in await q.redis.lrange(q.keys.active, 0, -1):
        present.add("active")
    assert present == set()
