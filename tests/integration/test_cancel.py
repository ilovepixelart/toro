"""Integration: cancelling a job, wherever it is (docs/specs/cancel.md).

A job that has not started is ended at once; a running one is told to stop and its
processor is cancelled where it awaits. Either way it lands in `cancelled`, which is
a terminal state of its own: a cancellation is not a failure.
"""

from toro import Queue

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
