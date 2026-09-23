"""Integration: jobs collected during a transaction (docs/specs/enqueue-on-commit.md).

A job enqueued before its transaction commits refers to a row that may never exist.
The buffer collects the adds and sends them only when the caller says the write went
through, which is the half of the dual-write problem people actually hit.
"""

import asyncio
from dataclasses import replace

import pytest
from redis.asyncio.connection import Connection

from toro import FlowChild, PartialFlushError

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


async def test_a_flush_costs_the_same_wire_writes_whatever_the_count(q, monkeypatch):
    """EC-003: collecting ten jobs costs what one costs. Counted as writes to the
    socket, which is what a round trip is: counting calls to `pipeline.execute()`
    would pass however many preamble commands the client sent around it."""
    writes = 0
    real_send = Connection.send_packed_command

    async def counting_send(self, *args, **kwargs):
        nonlocal writes
        writes += 1
        return await real_send(self, *args, **kwargs)

    await q.add("warm", {})  # so the scripts are cached server-side, as in a live app
    monkeypatch.setattr(Connection, "send_packed_command", counting_send)

    pending = q.pending()
    for i in range(10):
        pending.add("job", {"i": i})
    await pending.flush()

    # one SCRIPT EXISTS (redis-py checks its own cache before a pipelined EVALSHA),
    # then the whole batch in one write. A cold script cache adds one SCRIPT LOAD.
    assert writes == 2, f"ten jobs cost {writes} wire writes"
    assert (await q.counts())["wait"] == 11


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


async def test_a_flush_that_half_lands_keeps_only_what_did_not(q, monkeypatch):
    """EC-007: Redis has no rollback, so a batch where one add fails leaves the rest
    enqueued. A buffer that kept the whole batch would be retried by the caller the
    docs tell to retry it, and every job that did land would land twice."""
    real_stage = q._stage_add  # simulating a failure inside one script call

    def stage_one_broken(name, data, **kw):
        staged = real_stage(name, data, **kw)
        # too few arguments: this one script call errors, the others do not
        return replace(staged, args=staged.args[:3]) if name == "bad" else staged

    monkeypatch.setattr(q, "_stage_add", stage_one_broken)
    pending = q.pending()
    pending.add("first", {})
    pending.add("bad", {})
    pending.add("third", {})

    with pytest.raises(PartialFlushError) as caught:
        await pending.flush()

    assert [job.name for job in caught.value.sent] == ["first", "third"]
    assert len(pending) == 1, "the batch kept jobs it had already sent"
    assert (await q.counts())["wait"] == 2

    monkeypatch.setattr(q, "_stage_add", real_stage)  # the obstruction is gone
    assert [job.name for job in await pending.flush()] == ["bad"]
    assert (await q.counts())["wait"] == 3


async def test_two_flushes_at_once_do_not_double_the_batch(q):
    """EC-005: the documented hook is fire-and-forget, which is exactly the shape
    that puts two flushes in flight. Taking the batch before the first await is what
    makes the second one find nothing."""
    pending = q.pending()
    pending.add("a", {})
    pending.add("b", {})

    await asyncio.gather(pending.flush(), pending.flush())

    assert (await q.counts())["wait"] == 2


async def test_the_batch_takes_a_copy_of_what_it_was_given(q):
    """Between collecting a job and sending it is exactly where a caller fills in an
    id, or scrubs a secret. `Queue.add` encodes at the call, so it snapshots; a buffer
    that held the caller's dict by reference would send whatever it became."""
    payload = {"user_id": None}
    pending = q.pending()
    pending.add("welcome", payload)

    payload["user_id"] = 42
    payload["token"] = "leaked"  # noqa: S105
    [job] = await pending.flush()

    assert (await q.get_job(job.id)).data == {"user_id": None}


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
