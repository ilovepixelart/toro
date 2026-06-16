"""Property-based invariants: drive the queue through long randomized operation
sequences and assert the global invariants hold after every step, then that every
flow settles once the queue is drained.

A deterministic seeded fuzzer (not Hypothesis): processing is driven by hand via
_acquire + _finish_* so each step is atomic with nothing else mutating - no live
worker, no timing races - and a failing case reproduces exactly from its seed.

Invariants per step:
  1. counts() equals the real set cardinalities.
  2. no job id is in two state sets at once (atomic moves never duplicate).
  3. no orphan aux key (:deps/:results/:cfail/:lock) outlives its job hash.
Final invariant after a full drain:
  4. flows always settle - waiting-children is empty (no parent stranded).
"""

import random

import pytest

from toro import FlowChild as c  # noqa: N813
from toro import Queue, Worker
from toro.job import Job

PREFIX = "torotest"

_STATE_SETS = ("prioritized", "active", "delayed", "completed", "failed", "waiting_children")
_COUNT_TO_SET = {
    "wait": "prioritized",
    "active": "active",
    "delayed": "delayed",
    "completed": "completed",
    "failed": "failed",
    "waiting-children": "waiting_children",
}


async def _members(q: Queue) -> dict[str, set[str]]:
    """Current id membership of every state set (active is a LIST, rest ZSETs)."""
    out: dict[str, set[str]] = {}
    for name in _STATE_SETS:
        key = getattr(q.keys, name)
        ids = (
            await q.redis.lrange(key, 0, -1)
            if name == "active"
            else await q.redis.zrange(key, 0, -1)
        )
        out[name] = set(ids)
    return out


async def _check_invariants(q: Queue) -> None:
    members = await _members(q)

    # 1. counts() agrees with the real cardinalities
    counts = await q.counts()
    for cname, sname in _COUNT_TO_SET.items():
        assert counts[cname] == len(members[sname]), f"{cname} count {counts[cname]} != {sname}"

    # 2. no id lives in two state sets at once
    seen: dict[str, str] = {}
    for sname, ids in members.items():
        for jid in ids:
            assert jid not in seen, f"{jid} in both {seen[jid]} and {sname}"
            seen[jid] = sname

    # 3. no orphan aux keys (a removed job leaves nothing behind)
    base = q.keys.base
    for suffix in (":deps", ":results", ":cfail", ":lock"):
        for key in await q.redis.keys(f"{base}*{suffix}"):
            jid = key[len(base) : -len(suffix)]
            assert await q.redis.exists(q.keys.job(jid)), f"orphan {suffix} for {jid}"


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


async def _rand_member(q: Queue, rng: random.Random, *names: str) -> str | None:
    members = await _members(q)
    pool = sorted(jid for n in names for jid in members[n])
    return rng.choice(pool) if pool else None


# ---- the random operations (module level, so the test body stays simple) -----------


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
    jid = await _rand_member(q, rng, "failed")
    if jid:
        await q.retry_job(jid)


async def _op_remove(q, w, rng, ctr):
    jid = await _rand_member(q, rng, "prioritized", "active", "delayed", "completed", "failed")
    if jid:
        await q.remove_job(jid)


async def _op_clean(q, w, rng, ctr):
    await q.clean(rng.choice(["wait", "delayed", "completed", "failed"]))


async def _op_promote(q, w, rng, ctr):
    jid = await _rand_member(q, rng, "delayed")
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
async def test_invariants_hold_under_random_ops(q, seed):
    rng = random.Random(seed)  # noqa: S311 - a reproducible test fuzzer, not crypto
    w = Worker(q.name, lambda j: None, prefix=PREFIX, connection=q.redis)
    ctr = {"j": 0}

    for _ in range(40):
        await rng.choice(_OPS)(q, w, rng, ctr)
        await _check_invariants(q)

    await _recover_and_drain(q, w)
    await _check_invariants(q)
    settled = (await q.counts())["waiting-children"] == 0
    assert settled, "a flow never settled -> " + await _settle_diagnostic(q)
