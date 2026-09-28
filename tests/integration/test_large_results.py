"""Integration: a return value is as big and as deep as the processor makes it.

The finish script used to decode the result and re-encode it into the completion
event, inside the script, after it had already written. That is O(size) on Redis's
single thread: a big enough return value crosses the busy threshold, every other
client on a shared server is refused, and `SCRIPT KILL` answers UNKILLABLE because
the script has written. One job could take the server down for everybody.

A deep one broke differently: cjson stops at 1000 levels and the event wraps the
result one deeper, so the encode failed, the publish never happened, and the job
completed with nobody told.
"""

import asyncio
import contextlib
import json
import sys

import pytest

from toro import scripts

BIG = 2 * 1024 * 1024  # bigger than anything that belongs in an event
PREFIX = "torotest"


async def _events(q) -> tuple[asyncio.Task, list[str]]:
    """Everything published on the queue's events channel while a job runs."""
    seen: list[str] = []
    pubsub = q.redis.pubsub()
    await pubsub.subscribe(q.keys.events)

    async def listen() -> None:
        async for message in pubsub.listen():
            # not a comprehension: this stream ends by cancellation, and a
            # comprehension only assigns once it ends
            if message["type"] == "message":
                seen.append(message["data"])  # noqa: PERF401

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.05)  # the subscribe has to land before the job finishes
    return task, seen


async def test_a_huge_result_does_not_travel_in_the_event(q, run_worker, run_until):
    """The result stays in the job's hash, where it was already written, and the
    waiter reads it from there."""
    blob = "x" * BIG
    listening, seen = await _events(q)

    async def proc(job):
        return {"blob": blob}

    try:
        async with run_worker(q, proc):
            job = await q.add("fat", {})
            assert await q.result(job.id, timeout=20) == {"blob": blob}
    finally:
        listening.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listening

    completed = [m for m in seen if '"completed"' in m]
    assert completed, "no completion was announced"
    assert all(len(m) < 4096 for m in completed), f"the event carried {max(map(len, completed)):,}"


async def test_a_deeply_nested_result_still_reaches_its_waiter(q, run_worker, run_until):
    """1000 levels is the limit cjson decodes, and the event adds one. The job used
    to complete in Redis while the waiter timed out and the worker logged a hiccup.

    The recursion limit is raised for the duration: json.dumps recurses per level and
    runs out of stack at this depth before 3.12, so without it the test would only
    exercise the bug on the newest Pythons.
    """
    depth = 1000
    limit = sys.getrecursionlimit()
    sys.setrecursionlimit(depth * 20)
    deep: list = []
    node = deep
    for _ in range(depth - 1):
        child: list = []
        node.append(child)
        node = child

    async def proc(job):
        return deep

    try:
        async with run_worker(q, proc):
            job = await q.add("deep", {})
            assert await q.result(job.id, timeout=20) == deep
    finally:
        sys.setrecursionlimit(limit)


async def test_a_small_result_still_travels_with_its_event(q, run_worker, run_until):
    """The common case keeps its round trip: the value is in the event, so a waiter
    resolves without reading anything back. It is also the only case that works when
    the job removes itself on completion."""
    listening, seen = await _events(q)

    async def proc(job):
        return {"ok": True}

    try:
        async with run_worker(q, proc):
            job = await q.add("small", {}, remove_on_complete=True)
            assert await q.result(job.id, timeout=20) == {"ok": True}
    finally:
        listening.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listening

    assert any('"resultJson"' in m for m in seen), "the value did not travel with the event"


@pytest.mark.parametrize(
    "value",
    [12345678901234567, 0.1 + 0.2, {"items": []}, {"a": [[], {}], "b": 2**63}],
    ids=["17-digit-int", "float", "empty-list", "nested-empties"],
)
async def test_a_result_reaches_its_waiter_exactly_as_returned(q, run_worker, value):
    """The waiter gets what the processor returned, the same value `get_job` reads
    back. Decoded and re-encoded by cjson on the way into the event, a 17-digit int
    came back as a float at 14 significant digits and an empty list as an object."""
    listening, seen = await _events(q)

    async def proc(job):
        return value

    try:
        async with run_worker(q, proc):
            job = await q.add("exact", {})
            got = await q.result(job.id, timeout=20)
    finally:
        listening.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listening

    assert got == value
    assert type(got) is type(value)
    assert (await q.get_job(job.id)).returnvalue == value
    assert any('"completed"' in m and job.id in m for m in seen)


async def test_the_event_carries_the_stored_text_byte_for_byte(q, run_worker):
    """The event's copy of the result is the text the hash stores, escaped into a
    string and never parsed on the way, so no reader sees a re-encoded value."""
    listening, seen = await _events(q)

    async def proc(job):
        return {"id": 12345678901234567, "items": [], "x": 0.1 + 0.2}

    try:
        async with run_worker(q, proc):
            job = await q.add("exact", {})
            await q.result(job.id, timeout=20)
    finally:
        listening.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listening

    stored = await q.redis.hget(q.keys.job(job.id), "returnvalue")
    events = [json.loads(m) for m in seen if '"completed"' in m]
    assert [e["resultJson"] for e in events if e["jobId"] == job.id] == [stored]


async def test_a_large_result_reaches_its_waiter_after_the_job_removed_itself(
    q, run_worker, run_until
):
    """Over the inline limit the waiter reads the value back from the job's hash, but
    remove_on_complete=True deletes that hash in the same script: the waiter got None.
    With nothing to read back, the result travels with the event however large."""
    big = {"blob": "x" * (scripts.MAX_INLINE_RESULT_BYTES + 1)}

    async def proc(job):
        return big

    job = await q.add("big", {}, remove_on_complete=True)
    waiting = asyncio.create_task(q.result(job.id, timeout=20))
    assert await run_until(lambda: job.id in q._result_waiters)  # subscribed before any run
    async with run_worker(q, proc):
        assert await waiting == big
