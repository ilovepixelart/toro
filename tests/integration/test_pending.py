"""Integration: jobs collected during a transaction (docs/specs/enqueue-on-commit.md).

A job enqueued before its transaction commits refers to a row that may never exist.
The buffer collects the adds and sends them only when the caller says the write went
through, which is the half of the dual-write problem people actually hit.
"""

from toro import FlowChild

PREFIX = "torotest"


async def test_nothing_is_enqueued_until_the_flush(q):
    """EC-001: the whole point. Anything sent before the commit is a job about a row
    that might be rolled back out from under it."""
    pending = q.pending()
    pending.add("welcome", {"user": 1})
    pending.add("audit", {"user": 1})

    assert (await q.counts())["wait"] == 0
    assert len(pending) == 2

    jobs = await pending.flush()

    assert (await q.counts())["wait"] == 2
    assert [job.name for job in jobs] == ["welcome", "audit"]  # EC-002: in order
    assert all(job.id for job in jobs), "a flushed job knows its id"


async def test_a_rollback_sends_nothing_and_burns_no_id(q):
    """EC-004: a job that is never sent must not consume an id either: ids are a
    counter, and gaps in it are the sort of thing people debug for an afternoon."""
    await q.add("first", {})
    before = await q.redis.get(q.keys.id)

    pending = q.pending()
    pending.add("welcome", {})
    pending.add("audit", {})
    pending.discard()

    assert await pending.flush() == []
    assert (await q.counts())["wait"] == 1
    assert await q.redis.get(q.keys.id) == before


async def test_a_second_flush_is_a_no_op(q):
    """EC-005: a hook that fires twice, or a retry after a flush that did land, must
    not double every job in the transaction."""
    pending = q.pending()
    pending.add("welcome", {})
    await pending.flush()

    assert await pending.flush() == []
    assert (await q.counts())["wait"] == 1


async def test_a_flush_is_one_round_trip(q, monkeypatch):
    """EC-003: collecting ten jobs costs what one costs. A flush that cost a round
    trip per job would make "flush after commit" the expensive path, and the
    expensive path is the one people skip."""
    executes = 0
    real_pipeline = q.redis.pipeline

    def counting_pipeline(*args, **kwargs):
        pipe = real_pipeline(*args, **kwargs)
        real_execute = pipe.execute

        async def execute(*a, **k):
            nonlocal executes
            executes += 1
            return await real_execute(*a, **k)

        pipe.execute = execute
        return pipe

    monkeypatch.setattr(q.redis, "pipeline", counting_pipeline)

    pending = q.pending()
    for i in range(10):
        pending.add("job", {"i": i})
    await pending.flush()

    assert executes == 1, f"ten jobs cost {executes} round trips"
    assert (await q.counts())["wait"] == 10


async def test_the_options_are_the_same_ones(q):
    """EC-006: the buffer is the same call with the send deferred, so every option
    behaves as it does on add(), including a flow tree."""
    pending = q.pending()
    pending.add("later", {}, delay=60_000)
    pending.add("urgent", {}, priority=10)
    pending.add("mine", {}, job_id="custom-id")
    pending.add_flow("report", {}, children=[FlowChild("fetch", {}), FlowChild("render", {})])

    jobs = await pending.flush()

    counts = await q.counts()
    assert counts["delayed"] == 1
    assert counts["waiting-children"] == 1  # the flow parent is parked
    assert counts["wait"] == 4  # urgent, mine, and the flow's two children
    assert jobs[2].id == "custom-id"
    assert (await q.get_job("custom-id")).name == "mine"


async def test_a_failed_flush_keeps_what_it_could_not_send(q):
    """EC-007: a flush that raises leaves the buffer intact, so the caller can retry
    it rather than reconstruct what was in it."""
    pending = q.pending()
    pending.add("welcome", {})
    pending.add("bad", object())  # not JSON, so staging this one raises

    try:
        await pending.flush()
    except TypeError:
        pass
    else:
        raise AssertionError("a job that cannot be encoded must not pass silently")

    assert len(pending) == 2, "the buffer threw away what it could not send"
    assert (await q.counts())["wait"] == 0
