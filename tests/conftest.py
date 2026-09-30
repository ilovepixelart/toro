"""Shared test harness for the toro pyramid.

Layout: tests/unit (pure, no I/O) · tests/integration (Redis-backed) · tests/load.
Tests are auto-marked by their folder, so `pytest -m unit` runs the fast layer and
`-m integration` the Redis layer. Integration/load tests skip cleanly when no Redis
is reachable on localhost:6379.

The load layer runs CI-sized volumes by default; set TORO_LOAD_SCALE to multiply
the dataset sizes for a real volume run (e.g. `TORO_LOAD_SCALE=10 pytest -m load -s`
sweeps 500k delayed jobs and a 1M-entry active list). Sustained-rate load lives in
tests/load/harness.py (open-loop, arbitrary λ).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from contextlib import asynccontextmanager

import pytest

from toro import Queue, Worker

PREFIX = "torotest"


# ---- pyramid wiring: mark by folder, skip Redis layers when Redis is down --------


def pytest_collection_modifyitems(config, items):
    for item in items:
        path = str(item.fspath)
        if "/unit/" in path:
            item.add_marker(pytest.mark.unit)
        elif "/integration/" in path:
            item.add_marker(pytest.mark.integration)
        elif "/load/" in path:
            item.add_marker(pytest.mark.load)
        elif "/perf/" in path:
            item.add_marker(pytest.mark.perf)


_redis_up: bool | None = None


def _redis_reachable() -> bool:
    global _redis_up
    if _redis_up is None:
        try:
            import redis as _sync

            _sync.from_url("redis://localhost:6379").ping()
            _redis_up = True
        except Exception:
            _redis_up = False
    return _redis_up


@pytest.fixture(autouse=True)
def _require_redis(request):
    needs_redis = request.node.get_closest_marker("integration") or request.node.get_closest_marker(
        "load"
    )
    if needs_redis and not _redis_reachable():
        pytest.skip("needs a Redis on localhost:6379")


@pytest.fixture(scope="session")
def load_scale() -> float:
    """Volume multiplier for the load layer (TORO_LOAD_SCALE, default 1).

    Tests multiply their dataset sizes by this, so the same suite serves as a
    fast CI guardrail and, dialed up, a genuine volume run.
    """
    return max(1.0, float(os.environ.get("TORO_LOAD_SCALE", "1")))


# ---- fixtures & helpers ----------------------------------------------------------


async def _clear(queue: Queue) -> None:
    keys = await queue.redis.keys(queue.keys.base + "*")
    if keys:
        await queue.redis.delete(*keys)


# ---- the queue's global invariants, held by every state a test leaves behind ------

_STATE_SETS = (
    "prioritized",
    "active",
    "delayed",
    "held",
    "completed",
    "failed",
    "cancelled",
    "waiting_children",
)
# counts() key -> the keys.py property backing it. They line up one-to-one except
# `wait`: the waiting set is the priority-ordered zset, so its key is `prioritized`
# (the state is named for what it means; the key for what it is).
_COUNT_TO_SET = {
    "wait": "prioritized",
    "active": "active",
    "delayed": "delayed",
    "held": "held",
    "completed": "completed",
    "failed": "failed",
    "cancelled": "cancelled",
    "waiting-children": "waiting_children",
}


async def snapshot_members(queue: Queue) -> dict[str, set[str]]:
    """Current id membership of every state set (active is a LIST, rest ZSETs).

    One MULTI/EXEC, so the snapshot is an instant: read set by set under a live
    worker, a job moving between two reads shows up in both, or in neither.
    """
    pipe = queue.redis.pipeline(transaction=True)
    for name in _STATE_SETS:
        key = getattr(queue.keys, name)
        if name == "active":
            pipe.lrange(key, 0, -1)
        else:
            pipe.zrange(key, 0, -1)
    replies = await pipe.execute()
    return {name: set(ids) for name, ids in zip(_STATE_SETS, replies, strict=True)}


async def _still_orphan(queue: Queue, key: str, jid: str) -> bool:
    """Whether `key` exists while its job hash does not, read as one instant: a job
    removed between the KEYS scan and this read took both, and is no orphan."""
    pipe = queue.redis.pipeline(transaction=True)
    pipe.exists(key)
    pipe.exists(queue.keys.job(jid))
    still, hashed = await pipe.execute()
    return bool(still) and not hashed


async def check_invariants(queue: Queue, *, live: bool = False) -> None:
    """Assert what every reachable queue state must satisfy:

    1. counts() equals the real set cardinalities.
    2. no job id is in two state sets at once (atomic moves never duplicate).
    3. no orphan aux key (:deps/:results/:cfail/:lock) outlives its job hash.

    Read after each step of the random-operation fuzzers (test_invariants.py) and
    at the teardown of every passing integration test. `live` is for a queue with
    workers running (test_live_invariants.py): counts() is a second, separate read
    that a claim in flight legitimately puts one off, so it is left out there.
    """
    members = await snapshot_members(queue)

    # 1. counts() agrees with the real cardinalities
    if not live:
        counts = await queue.counts()
        for cname, sname in _COUNT_TO_SET.items():
            assert counts[cname] == len(members[sname]), f"{cname} count {counts[cname]} != {sname}"

    # 2. no id lives in two state sets at once
    seen: dict[str, str] = {}
    for sname, ids in members.items():
        for jid in ids:
            assert jid not in seen, f"{jid} in both {seen[jid]} and {sname}"
            seen[jid] = sname

    # 3. no orphan aux keys (a removed job leaves nothing behind)
    base = queue.keys.base
    for suffix in (":deps", ":results", ":cfail", ":lock"):
        for key in await queue.redis.keys(f"{base}*{suffix}"):
            jid = key[len(base) : -len(suffix)]
            assert not await _still_orphan(queue, key, jid), f"orphan {suffix} for {jid}"


@pytest.fixture
def invariants():
    """`await invariants(q)`: assert the queue's invariants hold right now."""
    return check_invariants


@pytest.fixture
def members():
    """`await members(q)`: the id membership of every state set."""
    return snapshot_members


_CALL_PASSED = pytest.StashKey[bool]()


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    report = yield
    if report.when == "call":
        item.stash[_CALL_PASSED] = report.passed  # read by q's teardown
    return report


@pytest.fixture
async def q(request):
    """A clean, isolated queue (own prefix; wiped before and after each test).

    A test that passed is also held to the queue's invariants at teardown, so one
    that leaves a job in two states or an orphan key fails here, naming the
    invariant, rather than a later test that reads the state failing for a reason
    it cannot show. A failed test already reports; a load test is exempt, its sets
    being the size the check would spend minutes reading.
    """
    queue = Queue("torotest", prefix=PREFIX)
    await _clear(queue)
    yield queue
    try:
        passed = request.node.stash.get(_CALL_PASSED, False)
        if passed and not request.node.get_closest_marker("load"):
            await check_invariants(queue)
    finally:
        await _clear(queue)
        await queue.close()


@pytest.fixture(autouse=True)
def _fast_idle_polling(monkeypatch):
    """Idle workers block on BZPOPMIN for `block_timeout` (default 5s) before
    re-polling, so a test that starts a worker pays several of those 5s waits
    plus the shutdown drain - ~20s per worker test, ~6min for the suite. The
    timeout is only the idle poll cadence (the marker still wakes a worker the
    instant work arrives, and the atomic claim is unchanged), so shrinking it in
    tests is purely a speed knob, not a behaviour change. A test that needs a
    specific cadence still passes its own block_timeout (setdefault won't clobber).
    """
    orig = Worker.__init__

    def faster(self, *args, **kw):
        kw.setdefault("block_timeout", 0.05)
        orig(self, *args, **kw)

    monkeypatch.setattr(Worker, "__init__", faster)


@asynccontextmanager
async def _running_worker(queue: Queue, processor, **kw):
    worker = Worker(queue.name, processor, prefix=PREFIX, **kw)
    task = asyncio.create_task(worker.run())
    try:
        yield worker
    finally:
        await worker.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.fixture
def run_worker():
    """`async with run_worker(q, processor, concurrency=2) as w: ...` - starts a
    worker and guarantees a clean shutdown on exit."""
    return _running_worker


@pytest.fixture
def run_until():
    """`await run_until(lambda: cond, timeout=2)` - poll until true or time out.
    Returns True if the condition held, False on timeout (assert on the result)."""

    async def _run_until(predicate, *, timeout: float = 5.0, interval: float = 0.02) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            res = predicate()
            if asyncio.iscoroutine(res):
                res = await res
            if callable(res):
                # `run_until(lambda: _count_is(q, "held", 1))` hands us a closure that
                # RETURNS the predicate. A function object is truthy, so the poll would
                # pass on its first turn having checked nothing. Pass the predicate.
                msg = f"predicate returned {res!r}: pass the predicate, not a lambda round it"
                raise TypeError(msg)
            if res:
                return True
            await asyncio.sleep(interval)
        return False

    return _run_until


# ---- TCP proxies to Redis that lose a reply or a link, for the re-send paths ----


async def _swallow_first_reply_to(sha: str, upstream_port: int = 6379, command: bytes = b""):
    """A TCP proxy to Redis that drops the reply to the first EVALSHA of `sha`:
    the command runs on the server, and the client never hears back.

    Everything the server sends on that connection from then on is dropped, not one
    read: a reply arrives in as many chunks as TCP delivers, and a client handed the
    tail of one reads it as an answer of its own. The client times out on the silence
    and drops the connection, so nothing later is lost with it.
    """
    swallowed = {"done": False}

    async def pipe(reader, writer, state, from_client):
        try:
            while data := await reader.read(65536):
                # `command` narrows the match: a pipeline asks SCRIPT EXISTS <sha> before
                # it sends its EVALSHAs, and that reply is not the one to lose.
                if (
                    from_client
                    and not swallowed["done"]
                    and sha.encode() in data
                    and command in data
                ):
                    swallowed["done"] = True
                    state["swallow"] = True
                elif not from_client and state.get("swallow"):
                    continue  # the reply nobody will hear, whole
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    async def handle(client_reader, client_writer):
        server_reader, server_writer = await asyncio.open_connection("localhost", upstream_port)
        state: dict[str, bool] = {}
        await asyncio.gather(
            pipe(client_reader, server_writer, state, True),
            pipe(server_reader, client_writer, state, False),
        )

    return await asyncio.start_server(handle, "localhost", 0)


async def _drop_link_after_first(sha: str, upstream_port: int = 6379):
    """A TCP proxy to Redis that, for the first EVALSHA of `sha`, lets the command
    through and then cuts the client's connection before the reply reaches it: the
    command ran, and the client sees a dropped connection instead of an answer."""
    dropped = {"done": False}

    async def pipe(reader, writer, state, from_client):
        try:
            while data := await reader.read(65536):
                if from_client and not dropped["done"] and sha.encode() in data:
                    dropped["done"] = True
                    state["cut"] = True
                elif not from_client and state.get("cut"):
                    state["cut"] = False
                    writer.close()  # the reply is on its way: the link goes instead
                    return
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    async def handle(client_reader, client_writer):
        server_reader, server_writer = await asyncio.open_connection("localhost", upstream_port)
        state: dict[str, bool] = {}
        await asyncio.gather(
            pipe(client_reader, server_writer, state, True),
            pipe(server_reader, client_writer, state, False),
        )

    return await asyncio.start_server(handle, "localhost", 0)


@pytest.fixture
def swallow_first_reply():
    """`proxy = await swallow_first_reply(sha)`: a proxy on a free port that lets the
    first command naming `sha` through and drops its reply; `command=b"EVALSHA"` skips
    a pipeline's SCRIPT EXISTS probe. `proxy.sockets[0]` has the port, `proxy.close()`
    ends it."""
    return _swallow_first_reply_to


@pytest.fixture
def drop_link_after_first():
    """`proxy = await drop_link_after_first(sha)`: like swallow_first_reply, but the
    client's connection is cut with the reply on its way, so the client re-sends."""
    return _drop_link_after_first
