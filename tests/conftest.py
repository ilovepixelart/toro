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
    """`async with run_worker(q, processor, concurrency=2) as w: ...` — starts a
    worker and guarantees a clean shutdown on exit."""
    return _running_worker


@pytest.fixture
def run_until():
    """`await run_until(lambda: cond, timeout=2)` — poll until true or time out.
    Returns True if the condition held, False on timeout (assert on the result)."""

    async def _run_until(predicate, *, timeout: float = 5.0, interval: float = 0.02) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            res = predicate()
            if asyncio.iscoroutine(res):
                res = await res
            if res:
                return True
            await asyncio.sleep(interval)
        return False

    return _run_until
