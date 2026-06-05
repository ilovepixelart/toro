"""Integration: admin/dashboard actions — remove, promote, retry, clean, trigger.

Each asserts the resulting state AND the negative case (acting on a missing job
returns False rather than silently succeeding).
"""


async def _count(q, state):
    return (await q.counts())[state]


async def test_remove_job_deletes_it_everywhere(q):
    j = await q.add("x", {})
    assert await q.remove_job(j.id) is True
    assert await q.get_job(j.id) is None
    assert await _count(q, "wait") == 0


async def test_remove_missing_job_returns_false(q):
    assert await q.remove_job("nope") is False


async def test_promote_moves_delayed_to_wait(q):
    j = await q.add("x", {}, delay=60_000)
    assert await _count(q, "delayed") == 1

    assert await q.promote_job(j.id) is True
    counts = await q.counts()
    assert counts["delayed"] == 0 and counts["wait"] == 1


async def test_promote_missing_job_returns_false(q):
    assert await q.promote_job("nope") is False


async def test_retry_job_moves_failed_back_to_wait_and_clears_reason(q, run_worker, run_until):
    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        j = await q.add("x", {}, attempts=1)  # one shot → straight to failed
        assert await run_until(lambda: _count(q, "failed"))

    assert await q.retry_job(j.id) is True
    counts = await q.counts()
    assert counts["failed"] == 0 and counts["wait"] == 1
    assert (await q.get_job(j.id)).failed_reason is None  # the prior failure is cleared


async def test_retry_all_failed_requeues_every_one(q, run_worker, run_until):
    async def proc(job):
        raise RuntimeError("boom")

    async with run_worker(q, proc):
        for i in range(3):
            await q.add(f"x{i}", {}, attempts=1)
        assert await run_until(lambda: _count(q, "failed"), timeout=8.0)
        # wait for all three to settle into failed before requeuing
        assert await run_until(lambda: _all_failed(q, 3), timeout=8.0)

    assert await q.retry_all_failed() == 3
    counts = await q.counts()
    assert counts["failed"] == 0 and counts["wait"] == 3


async def test_clean_removes_a_whole_state(q):
    for i in range(4):
        await q.add(f"x{i}", {})
    assert await _count(q, "wait") == 4

    assert await q.clean("wait") == 4
    assert await _count(q, "wait") == 0


async def test_trigger_scheduler_enqueues_one_immediately(q):
    await q.add_scheduler("nightly", cron="0 0 * * *", name="rollup")
    before = await _count(q, "wait")  # the scheduled run is delayed

    assert await q.trigger_scheduler("nightly") is True
    assert await _count(q, "wait") == before + 1  # a manual run is enqueued now


async def test_trigger_missing_scheduler_returns_false(q):
    assert await q.trigger_scheduler("nope") is False


async def _all_failed(q, n):
    return (await q.counts())["failed"] == n
