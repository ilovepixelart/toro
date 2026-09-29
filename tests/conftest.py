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


@pytest.fixture
async def q():
    """A clean, isolated queue (own prefix; wiped before and after each test)."""
    queue = Queue("torotest", prefix=PREFIX)
    await _clear(queue)
    yield queue
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


async def _swallow_first_reply_to(sha: str, upstream_port: int = 6379):
    """A TCP proxy to Redis that drops the reply to the first EVALSHA of `sha`:
    the command runs on the server, and the client never hears back."""
    swallowed = {"done": False}

    async def pipe(reader, writer, state, from_client):
        try:
            while data := await reader.read(65536):
                if from_client and not swallowed["done"] and sha.encode() in data:
                    swallowed["done"] = True
                    state["swallow"] = True
                elif not from_client and state.get("swallow"):
                    state["swallow"] = False  # this is the reply nobody will hear
                    continue
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
    first EVALSHA of `sha` through and drops its reply; `proxy.sockets[0]` has the port,
    `proxy.close()` ends it."""
    return _swallow_first_reply_to


@pytest.fixture
def drop_link_after_first():
    """`proxy = await drop_link_after_first(sha)`: like swallow_first_reply, but the
    client's connection is cut with the reply on its way, so the client re-sends."""
    return _drop_link_after_first
