"""The installed-wheel scenarios: toro used exactly as a consumer uses it.

Run by tests/wheel/test_wheel.py with the Python of a fresh virtual environment that
holds nothing but an installed toro (the wheel this repository builds, or a release
from PyPI), started with `-I` from a directory outside the repository, so `import
toro` can only resolve to what was installed. It is not a pytest module: the
interpreter running it has no pytest.

    python -I scenarios.py --out results.json [--only name ...]

Each scenario returns what it observed, as plain JSON, deterministic across runs (no
generated ids, no timestamps): test_wheel.py compares the candidate's observations
against the published release's. A scenario that fails its own expectations records
the error instead.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import itertools
import json
import os
import signal
import sys
import threading
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable
from importlib import metadata
from pathlib import Path
from typing import Any, get_args

import redis
import redis.asyncio as aioredis

import toro
from toro import (
    FlowChild,
    IncompatibleDataModelError,
    JobCancelledError,
    JobFailedError,
    Queue,
    Worker,
)

URL = os.environ.get("REDIS_URL", "")
# Every scenario writes under a prefix of its own, unique to this run, and removes it.
RUN = uuid.uuid4().hex[:8]
SCENARIO_TIMEOUT = 60.0
SCENARIOS: dict[str, Callable[[Ctx], Awaitable[dict[str, Any]]]] = {}


def scenario(fn: Callable[[Ctx], Awaitable[dict[str, Any]]]) -> Callable[..., Any]:
    SCENARIOS[fn.__name__] = fn
    return fn


def expect(condition: object, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def error_of(exc: BaseException) -> dict[str, str]:
    return {"type": type(exc).__name__, "message": str(exc)}


async def refusal(call: Callable[[], Any]) -> dict[str, str] | None:
    """What `call` raised, or None when it was accepted."""
    try:
        res = call()
        if asyncio.iscoroutine(res):
            await res
    except Exception as exc:
        return error_of(exc)
    return None


def installed_files() -> tuple[Path, bool, bool]:
    """Where `toro` was imported from, whether that is inside this environment, and
    whether py.typed sits beside it."""
    installed = Path(toro.__file__).resolve()
    inside = installed.is_relative_to(Path(sys.prefix).resolve())
    return installed, inside, (installed.parent / "py.typed").exists()


class Ctx:
    """One scenario's queue name, prefix and cleanup."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.prefix = f"e2e-{RUN}-{name}"
        self.queues: list[Queue] = []

    def queue(self, **kw: Any) -> Queue:
        kw.setdefault("url", URL)
        q = Queue(self.name, prefix=self.prefix, **kw)
        self.queues.append(q)
        return q

    def worker(self, processor: Any, **kw: Any) -> Worker:
        kw.setdefault("url", URL)
        return Worker(self.name, processor, prefix=self.prefix, **kw)

    async def cleanup(self) -> None:
        for q in self.queues:
            with contextlib.suppress(Exception):
                await q.close()
        client = redis.Redis.from_url(URL)
        try:
            keys = client.keys(f"{self.prefix}:*")
            if keys:
                client.delete(*keys)
        finally:
            client.close()


@contextlib.asynccontextmanager
async def running(worker: Worker, grace: float = 5.0):
    task = asyncio.create_task(worker.run())
    try:
        yield task
    finally:
        await worker.stop(grace_period=grace)
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(task, 10)


async def until(predicate: Callable[[], Any], timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        res = predicate()
        if asyncio.iscoroutine(res):
            res = await res
        if res:
            return True
        await asyncio.sleep(0.02)
    return False


async def state_of(q: Queue, job_id: str) -> str | None:
    job = await q.get_job(job_id)
    return None if job is None else job.state


def closed_port() -> int:
    """A local port with nothing listening on it: Redis is down there."""
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def redis_address() -> tuple[str, int]:
    from urllib.parse import urlsplit

    parts = urlsplit(URL)
    return parts.hostname or "localhost", parts.port or 6379


class Outage:
    """A TCP proxy in front of the real Redis that can go down and come back on the
    same port: what a client sees when Redis restarts under it."""

    def __init__(self) -> None:
        self.port = 0
        self.server: asyncio.Server | None = None
        self.links: set[asyncio.StreamWriter] = set()

    async def up(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", self.port)
        self.port = self.server.sockets[0].getsockname()[1]

    async def down(self) -> None:
        assert self.server is not None
        self.server.close()
        for writer in list(self.links):
            writer.close()
        self.links.clear()
        await self.server.wait_closed()

    @property
    def url(self) -> str:
        return f"redis://127.0.0.1:{self.port}"

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            up_reader, up_writer = await asyncio.open_connection(*redis_address())
        except OSError:
            writer.close()
            return
        self.links.update((writer, up_writer))

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
            try:
                while data := await src.read(65536):
                    dst.write(data)
                    await dst.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                dst.close()

        await asyncio.gather(pipe(reader, up_writer), pipe(up_reader, writer))


# ---- the public surface -------------------------------------------------------------


@scenario
async def import_surface(_: Ctx) -> dict[str, Any]:
    installed, inside, typed = installed_files()
    expect(inside, f"toro imported from {installed}, not from the environment {sys.prefix}")
    expect(typed, "the wheel does not ship py.typed")
    expect(toro.__version__ == metadata.version("toro-queue"), "__version__ disagrees")
    missing = [n for n in toro.__all__ if not hasattr(toro, n)]
    expect(not missing, f"__all__ names what the package lacks: {missing}")
    return {
        "all": sorted(toro.__all__),
        "version": toro.__version__,
        "data_model_version": toro.DATA_MODEL_VERSION,
        "job_states": sorted(get_args(toro.JobState)),
    }


@scenario
async def distribution_metadata(_: Ctx) -> dict[str, Any]:
    dist = metadata.distribution("toro-queue")
    meta = dist.metadata
    top = sorted({Path(str(f)).parts[0] for f in dist.files or []})
    return {
        "name": meta["Name"],
        "requires_python": meta["Requires-Python"],
        "requires_dist": sorted(dist.requires or []),
        "classifiers": sorted(meta.get_all("Classifier") or []),
        # toro ships no command-line entry point; this says so if one ever appears.
        "entry_points": sorted(f"{e.group}:{e.name}" for e in dist.entry_points),
        "top_level": [t for t in top if not t.endswith(".dist-info")],
    }


# ---- producing and processing -------------------------------------------------------


@scenario
async def quick_start(ctx: Ctx) -> dict[str, Any]:
    """The README's quick start: add, process, the completed event, the result."""
    q = ctx.queue()
    job = await q.add("welcome", {"to": "ada@example.com"})
    expect(job.state == "wait", f"a fresh job is {job.state!r}, not 'wait'")
    seen: list[Any] = []

    async def process(j):
        seen.append(j.data)
        return {"ok": True}

    w = ctx.worker(process, concurrency=8)
    events: list[Any] = []
    w.on("completed", lambda j, result: events.append((j.name, result)))
    async with running(w):
        value = await job.result(timeout=10)
        await until(lambda: events)
    done = await q.get_job(job.id)
    expect(value == {"ok": True}, f"result() returned {value!r}")
    expect(done is not None and done.state == "completed", "the job is not completed")
    return {
        "seen": seen,
        "result": value,
        "events": events,
        "returnvalue": done.returnvalue if done else None,
        "attempts_made": done.attempts_made if done else None,
        "counts": await q.counts(),
    }


@scenario
async def sync_processor_runs_off_the_loop(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    loop_thread = threading.get_ident()
    ran_on: list[int] = []

    def process(j):
        ran_on.append(threading.get_ident())
        return j.data * 2

    async with running(ctx.worker(process)):
        value = await (await q.add("double", 21)).result(timeout=10)
    expect(value == 42, f"sync processor result {value!r}")
    expect(ran_on and ran_on[0] != loop_thread, "a sync processor ran on the event loop thread")
    return {"result": value, "off_loop": ran_on[0] != loop_thread}


@scenario
async def priorities_order_and_fifo_within_a_band(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    for name, prio in [("a", 0), ("b", 0), ("c", 10), ("d", 5), ("e", 10)]:
        await q.add(name, priority=prio)
    order: list[str] = []

    async def process(j):
        order.append(j.name)

    async with running(ctx.worker(process, concurrency=1)):
        await until(lambda: len(order) == 5)
    expect(order == ["c", "e", "d", "a", "b"], f"processed in order {order}")
    return {"order": order}


@scenario
async def delayed_jobs_and_promotion(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    t0 = time.monotonic()
    soon = await q.add("soon", delay=400)
    later = await q.add("later", delay=600_000)
    states = [await state_of(q, soon.id), await state_of(q, later.id)]
    ran_at: dict[str, float] = {}

    async def process(j):
        ran_at[j.name] = time.monotonic() - t0

    async with running(ctx.worker(process)):
        await until(lambda: "soon" in ran_at)
        promoted = await q.promote_job(later.id)
        await until(lambda: "later" in ran_at)
        promote_gone = await q.promote_job("no-such-job")
    expect(states == ["delayed", "delayed"], f"delayed jobs start as {states}")
    expect(ran_at.get("soon", 0) >= 0.4, f"a 400 ms delay ran after {ran_at.get('soon')} s")
    expect("later" in ran_at, "a promoted job did not run")
    return {
        "initial_states": states,
        "promoted": promoted,
        "promote_missing": promote_gone,
        "ran": sorted(ran_at),
    }


@scenario
async def retries_with_backoff_then_success(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    tries: list[float] = []

    async def process(_j):
        tries.append(time.monotonic())
        if len(tries) < 3:
            raise ValueError(f"boom {len(tries)}")
        return "third time"

    w = ctx.worker(process)
    retrying: list[str] = []
    w.on("retrying", lambda _j, exc: retrying.append(str(exc)))
    async with running(w):
        job = await q.add("flaky", attempts=3, backoff={"type": "fixed", "delay": 200})
        value = await job.result(timeout=15)
    done = await q.get_job(job.id)
    gaps = [b - a for a, b in itertools.pairwise(tries)]
    expect(value == "third time", f"result {value!r}")
    expect(all(g >= 0.2 for g in gaps), f"a 200 ms backoff retried after {gaps}")
    expect(retrying == ["boom 1", "boom 2"], f"retrying events {retrying}")
    return {
        "result": value,
        "tries": len(tries),
        "retrying_events": retrying,
        "attempts_made": done.attempts_made if done else None,
    }


@scenario
async def retries_exhausted_raise_job_failed(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    fail = {"on": True}

    async def process(_j):
        if fail["on"]:
            raise RuntimeError("card declined")
        return "charged"

    w = ctx.worker(process)
    failed: list[str] = []
    w.on("failed", lambda _j, exc: failed.append(str(exc)))
    async with running(w):
        job = await q.add("charge", attempts=2, backoff=50)
        try:
            await job.result(timeout=15)
            raised: dict[str, str] | None = None
        except JobFailedError as exc:
            raised = {**error_of(exc), "reason": str(exc.reason)}
        snapshot = await q.get_job(job.id)
        fail["on"] = False
        retried = await q.retry_job(job.id)
        value = await q.result(job.id, timeout=15)
    expect(raised is not None, "result() did not raise JobFailedError for a failed job")
    expect(snapshot is not None and snapshot.state == "failed", "the job is not failed")
    expect(value == "charged", f"the retried job answered {value!r}")
    return {
        "raised": raised,
        "failed_events": failed,
        "failed_reason": snapshot.failed_reason if snapshot else None,
        "attempts_made": snapshot.attempts_made if snapshot else None,
        "has_stacktrace": bool(snapshot and snapshot.stacktrace),
        "retried": retried,
        "after_retry": value,
    }


@scenario
async def job_timeout_fails_the_job(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()

    async def process(_j):
        await asyncio.sleep(5)

    async with running(ctx.worker(process)):
        job = await q.add("slow", timeout=200)
        with contextlib.suppress(JobFailedError):
            await job.result(timeout=10)
    done = await q.get_job(job.id)
    expect(done is not None and done.state == "failed", "a timed-out job did not fail")
    return {"state": done.state if done else None, "failed_reason": done and done.failed_reason}


@scenario
async def custom_ids_and_deduplication(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    first = await q.add("charge", {"n": 1}, job_id="order-1234")
    again = await q.add("charge", {"n": 2}, job_id="order-1234")
    d1 = await q.add("ping", deduplication={"id": "ping-window", "ttl": 5000})
    d2 = await q.add("ping", deduplication={"id": "ping-window", "ttl": 5000})
    stored = await q.get_job("order-1234")
    expect(first.id == again.id == "order-1234", "a repeated custom id made a second job")
    expect(d1.id == d2.id, "a second add inside the dedup window made a second job")
    expect(stored is not None and stored.data == {"n": 1}, "the second add overwrote the first")
    return {"ids": [first.id, again.id], "dedup_same": d1.id == d2.id, "counts": await q.counts()}


@scenario
async def invalid_input_is_refused(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    attempts: dict[str, Callable[[], Awaitable[Any]]] = {
        "all_digit_job_id": lambda: q.add("x", job_id="123"),
        "slash_job_id": lambda: q.add("x", job_id="a/b"),
        "zero_attempts": lambda: q.add("x", attempts=0),
        "negative_delay": lambda: q.add("x", delay=-1),
        "unknown_option": lambda: q.add("x", colour="red"),
        "bad_backoff": lambda: q.add("x", backoff={"type": "sideways", "delay": 1}),
        "empty_dedup_id": lambda: q.add("x", deduplication={"id": "", "ttl": 10}),
        "both_every_and_cron": lambda: q.add_scheduler("s", every=1000, cron="* * * * *"),
        "bad_cron": lambda: q.add_scheduler("s", cron="not a cron"),
        "bad_rate_limit": lambda: q.set_limits(rate_limit={"max": 0, "duration": 1000}),
    }
    out = {label: await refusal(call) for label, call in attempts.items()}
    accepted = [label for label, err in out.items() if err is None]
    expect(not accepted, f"accepted invalid input: {accepted}")
    wrong = [label for label, err in out.items() if err and err["type"] != "ValueError"]
    expect(not wrong, f"refused without ValueError: {wrong}")
    expect((await q.counts())["wait"] == 0, "a refused add enqueued something")
    return out


# ---- schedules and flows ------------------------------------------------------------


@scenario
async def schedulers_every_trigger_remove(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    ran: list[str] = []

    async def process(j):
        ran.append(j.name)

    sid = await q.add_scheduler("tick", every=300, name="tick-job", data={"k": 1})
    listed = [s.get("id") for s in await q.schedulers()]
    async with running(ctx.worker(process)):
        await until(lambda: len(ran) >= 2)
        triggered = await q.trigger_scheduler("tick")
        await q.remove_scheduler("tick")
        after = await q.schedulers()
        gone = await q.trigger_scheduler("tick")
    expect(len(ran) >= 2, f"an every=300 schedule ran {len(ran)} times")
    expect(triggered is True and gone is False, "trigger_scheduler answered wrongly")
    return {
        "id": sid,
        "listed": listed,
        "job_names": sorted(set(ran)),
        "triggered": triggered,
        "after_remove": after,
        "trigger_removed": gone,
    }


@scenario
async def flows_fan_out_fan_in(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()

    async def process(j):
        if j.name == "fetch":
            return j.data["shard"] * 2
        results = await j.children_results()
        return sum(results.values())

    parent = await q.add_flow(
        "report", {"q": 3}, children=[FlowChild("fetch", {"shard": i}) for i in range(3)]
    )
    parked = await state_of(q, parent.id)
    async with running(ctx.worker(process, concurrency=3)):
        value = await parent.result(timeout=15)
    view = await q.flow_view(parent.id)
    expect(parked == "waiting-children", f"a flow parent starts as {parked!r}")
    expect(value == 6, f"the parent read its children as {value!r}")
    # The counts are over the root's direct children (FlowView's docstring).
    expect(view is not None and view.total == 3 and view.done == 3, "the flow view is wrong")
    return {
        "parked": parked,
        "result": value,
        "view": view and {"total": view.total, "done": view.done, "failed": view.failed},
        "missing_view": await q.flow_view("no-such-job"),
    }


@scenario
async def flow_failure_policies_and_retry_flow(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    broken = {"on": True}

    async def process(j):
        if j.name == "flaky" and broken["on"]:
            raise RuntimeError("shard down")
        if j.name in ("tolerant", "strict"):
            failed = await j.failed_children()
            return {"ok": len(await j.children_results()), "failed": len(failed)}
        return j.data

    tolerant = await q.add_flow(
        "tolerant",
        children=[
            FlowChild("part", 1),
            FlowChild("part", 2),
            FlowChild("flaky", on_fail="continue"),
        ],
    )
    strict = await q.add_flow("strict", children=[FlowChild("part", 3), FlowChild("flaky")])
    async with running(ctx.worker(process, concurrency=4)):
        tolerant_value = await tolerant.result(timeout=15)
        try:
            await strict.result(timeout=15)
            strict_raised = None
        except JobFailedError as exc:
            strict_raised = type(exc).__name__
        progress = await q.flow_progress([tolerant.id])
        tree = await q.get_flow(tolerant.id)
        roots_total, roots = await q.get_jobs_roots("completed")
        roots_counts = await q.roots_counts()
        failed_children = await q.failed_children(tolerant.id)
        broken["on"] = False
        retried = await q.retry_flow(strict.id)
        strict_value = await q.result(strict.id, timeout=15)
    expect(tolerant_value == {"ok": 2, "failed": 1}, f"the tolerant parent saw {tolerant_value}")
    expect(strict_raised == "JobFailedError", "a fail_parent child did not fail the parent")
    expect(retried >= 2, f"retry_flow() retried {retried} jobs")
    expect(strict_value == {"ok": 2, "failed": 0}, f"the retried flow answered {strict_value}")
    return {
        "tolerant": tolerant_value,
        "strict_raised": strict_raised,
        "progress": list(progress[tolerant.id]),
        "tree_children": sorted(n["job"].name for n in tree["children"]) if tree else None,
        "completed_roots": [roots_total, sorted(j.name for j in roots)],
        "roots_counts": roots_counts,
        "failed_children": sorted(failed_children.values()),
        "retried": retried,
        "strict_after_retry": strict_value,
        "missing_flow": await q.get_flow("no-such-job"),
    }


# ---- concurrency and limits ---------------------------------------------------------


class InFlight:
    def __init__(self) -> None:
        self.now = 0
        self.peak = 0
        self.order: list[str] = []

    def processor(self, seconds: float):
        async def process(j):
            self.now += 1
            self.peak = max(self.peak, self.now)
            self.order.append(j.name)
            try:
                await asyncio.sleep(seconds)
            finally:
                self.now -= 1

        return process


@scenario
async def concurrency_runs_jobs_in_parallel(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    flight = InFlight()
    for i in range(8):
        await q.add(f"j{i}")
    t0 = time.monotonic()
    async with running(ctx.worker(flight.processor(0.4), concurrency=4)):
        await until(_completed(q, 8), 15)
    took = time.monotonic() - t0
    expect(flight.peak == 4, f"concurrency=4 ran {flight.peak} at once")
    expect(took < 2.5, f"8 jobs of 0.4 s on 4 slots took {took:.2f} s")
    return {"peak": flight.peak}


def _completed(q: Queue, n: int) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        return (await q.counts())["completed"] >= n

    return check


@scenario
async def global_concurrency_caps_every_worker(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    await q.set_limits(global_concurrency=2)
    limits = await q.limits()
    flight = InFlight()
    for i in range(6):
        await q.add(f"j{i}")
    w1 = ctx.worker(flight.processor(0.3), concurrency=3)
    w2 = ctx.worker(flight.processor(0.3), concurrency=3)
    async with running(w1), running(w2):
        await until(_completed(q, 6), 15)
    expect(flight.peak == 2, f"a global cap of 2 ran {flight.peak} at once over two workers")
    return {"limits": limits, "peak": flight.peak}


@scenario
async def concurrency_key_serializes_in_order(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    flight = InFlight()
    for i in range(4):
        await q.add(f"k{i}", concurrency_key="account-7")
    async with running(ctx.worker(flight.processor(0.1), concurrency=4)):
        await until(_completed(q, 4), 15)
    expect(flight.peak == 1, f"jobs sharing a key ran {flight.peak} at once")
    expect(flight.order == ["k0", "k1", "k2", "k3"], f"key order {flight.order}")
    return {"peak": flight.peak, "order": flight.order}


@scenario
async def rate_limit_spaces_job_starts(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    await q.set_limits(rate_limit={"max": 2, "duration": 1000})
    limits = await q.limits()
    starts: list[float] = []

    async def process(_j):
        starts.append(time.monotonic())

    for i in range(4):
        await q.add(f"r{i}")
    async with running(ctx.worker(process, concurrency=4)):
        await until(lambda: len(starts) == 4, 10)
    spread = starts[-1] - starts[0] if len(starts) == 4 else None
    expect(len(starts) == 4, f"{len(starts)} of 4 rate-limited jobs ran")
    expect(spread is not None and spread >= 0.8, f"4 jobs at 2/s started within {spread} s")
    return {"limits": limits, "ran": len(starts)}


# ---- control: pause, cancel, pending, progress, admin -------------------------------


@scenario
async def pause_and_resume(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    ran: list[str] = []

    async def process(j):
        ran.append(j.name)

    await q.pause()
    paused = await q.is_paused()
    await q.add("held-back")
    async with running(ctx.worker(process)):
        await asyncio.sleep(0.7)
        while_paused = list(ran)
        await q.resume()
        await until(lambda: ran)
    expect(paused is True, "is_paused() is False after pause()")
    expect(while_paused == [], "a paused queue ran a job")
    expect(ran == ["held-back"], "the job did not run after resume()")
    return {"paused": paused, "while_paused": while_paused, "after_resume": ran}


@scenario
async def cancel_a_running_job(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    started = asyncio.Event()

    async def process(_j):
        started.set()
        await asyncio.sleep(30)

    w = ctx.worker(process)
    cancelled: list[str] = []
    w.on("cancelled", lambda j: cancelled.append(j.name))
    async with running(w):
        job = await q.add("long")
        await asyncio.wait_for(started.wait(), 10)
        ok = await q.cancel_job(job.id, reason="no longer needed")
        try:
            await job.result(timeout=10)
            raised = None
        except JobCancelledError as exc:
            raised = error_of(exc) | {"reason": exc.reason}
        await until(lambda: cancelled, 5)
    done = await q.get_job(job.id)
    expect(ok is True, "cancel_job() answered False for a running job")
    expect(raised is not None, "result() did not raise JobCancelledError")
    expect(done is not None and done.state == "cancelled", "the job is not cancelled")
    return {
        "cancel": ok,
        "raised": raised and {"type": raised["type"], "reason": raised["reason"]},
        "cancelled_events": cancelled,
        "cancel_missing": await q.cancel_job("no-such-job"),
    }


@scenario
async def pending_batch_flush_and_discard(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    batch = q.pending()
    batch.add("a", 1)
    batch.add("b", 2)
    batch.add("c", 3)
    size = len(batch)
    before = (await q.counts())["wait"]
    jobs = await batch.flush()
    dropped = q.pending()
    dropped.add("never", 0)
    dropped.discard()
    discarded = await dropped.flush()
    expect(before == 0, "pending() wrote before flush()")
    expect([j.name for j in jobs] == ["a", "b", "c"], "flush() did not send the batch")
    return {
        "size": size,
        "sent": [j.name for j in jobs],
        "after_discard": discarded,
        "wait": (await q.counts())["wait"],
    }


@scenario
async def progress_and_logs(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()

    async def process(j):
        await j.update_progress(50)
        await j.log("halfway")
        await j.update_progress({"step": "done"})
        await j.log("finished")
        return "ok"

    async with running(ctx.worker(process)):
        job = await q.add("tracked")
        await job.result(timeout=10)
    done = await q.get_job(job.id)
    logs = await q.get_logs(job.id)
    expect(done is not None and done.progress == {"step": "done"}, "progress was not stored")
    expect(logs == ["halfway", "finished"], f"logs {logs}")
    outside: dict[str, str] | None = None
    try:
        await job.update_progress(1)
    except RuntimeError as exc:
        outside = error_of(exc)
    return {"progress": done.progress if done else None, "logs": logs, "outside": outside}


@scenario
async def inspect_and_administer_jobs(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()

    async def process(j):
        if j.data.get("fail"):
            raise ValueError("bad row")
        return j.data["n"]

    async with running(ctx.worker(process)):
        for n in range(3):
            await q.add("row", {"n": n})
        bad = await q.add("row", {"n": 99, "fail": True})
        await until(_completed(q, 3))
        await until(_is(q, bad.id, "failed"))
        completed = sorted(j.data["n"] for j in await q.get_jobs("completed"))
        found = [j.data["n"] for j in await q.search("failed", "99")]
        totals = await q.lifetime_totals()
        cleaned = await q.clean("completed")
    retried = await q.retry_all_failed()
    removed = await q.remove_job(bad.id)
    expect(completed == [0, 1, 2], f"get_jobs('completed') -> {completed}")
    expect(found == [99], f"search('failed', '99') -> {found}")
    expect(cleaned == 3, f"clean('completed') removed {cleaned}")
    return {
        "completed": completed,
        "search": found,
        # "ms" is the summed processing time: measured, so only its presence is compared
        "lifetime_totals": {k: (v if k != "ms" else "ms" in totals) for k, v in totals.items()},
        "cleaned": cleaned,
        "retry_all_failed": retried,
        "removed": removed,
        "remove_again": await q.remove_job(bad.id),
        "missing_job": await q.get_job("no-such-job"),
        "counts": await q.counts(),
    }


def _is(q: Queue, job_id: str, state: str) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        return await state_of(q, job_id) == state

    return check


@scenario
async def metrics_text_is_openmetrics(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()

    async def process(_j):
        return 1

    async with running(ctx.worker(process)):
        await (await q.add("m")).result(timeout=10)
    text = await q.metrics_text()
    families = sorted({line.split()[2] for line in text.splitlines() if line.startswith("# TYPE ")})
    expect(text.endswith("# EOF\n"), "the exposition does not end with # EOF")
    expect(families, "the exposition has no metric families")
    return {"families": families}


@scenario
async def dashboard_reads(ctx: Ctx) -> dict[str, Any]:
    """The read side a dashboard is built on, after real work has gone through."""
    q = ctx.queue()

    async def process(j):
        if j.name == "bad":
            raise ValueError("nope")
        await asyncio.sleep(0.01)
        return j.name

    async with running(ctx.worker(process, concurrency=2)):
        for name in ("a", "a", "b", "bad"):
            await q.add(name)
        parent = await q.add_flow("p", children=[FlowChild("c")])
        await parent.result(timeout=10)
        await until(_completed(q, 5), 10)
        latency = await q.latency()
    points = await q.metrics(minutes=5)
    by_name = await q.metrics_by_name(minutes=5)
    departed = await q.departed_workers()
    cleared = await q.clear_departed()
    text = await q.metrics_text()
    rendered = toro.render_all({q.name: (await q.lifetime_totals(), await q.counts())})
    expect(len(points) == 5, f"metrics(minutes=5) returned {len(points)} points")
    expect(sum(p["completed"] for p in points) == 5, "the minute buckets lost a completion")
    expect(rendered == text, "render_all over one queue disagrees with metrics_text()")
    expect(departed and departed[0].get("reason") == "stopped", f"departures: {departed}")
    return {
        "point_fields": sorted(points[0]),
        "completed_in_window": sum(p["completed"] for p in points),
        "failed_in_window": sum(p["failed"] for p in points),
        # durations (ms, percentiles) are measured: only the counts are compared
        "by_name": [{k: n[k] for k in ("name", "completed", "failed")} for n in by_name],
        "percentiles": sorted(await q.percentiles(minutes=5)),
        "flow_points": len(await q.flow_metrics(minutes=5)),
        "flow_percentiles": sorted(await q.flow_percentiles(minutes=5)),
        "latency_is_ms": isinstance(latency, int) and latency >= 0,
        "departed": [sorted(departed[0]), departed[0].get("reason")],
        "cleared": cleared,
        "after_clear": await q.departed_workers(),
    }


@scenario
async def worker_presence(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()

    async def process(_j):
        return None

    w = ctx.worker(process, concurrency=2)
    async with running(w):
        await until(q.workers, 5)
        live = await q.workers()
    after = await q.workers()
    expect(len(live) == 1, f"one running worker, {len(live)} listed")
    expect(after == [], "a stopped worker is still listed")
    return {"live": len(live), "fields": sorted(live[0]) if live else [], "after_stop": after}


# ---- startup, shutdown, crash, outage -----------------------------------------------


@scenario
async def graceful_stop_drains_in_flight(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    started = asyncio.Event()

    async def process(_j):
        started.set()
        await asyncio.sleep(0.5)
        return "drained"

    w = ctx.worker(process)
    task = asyncio.create_task(w.run())
    job = await q.add("in-flight")
    await asyncio.wait_for(started.wait(), 10)
    t0 = time.monotonic()
    await w.stop(grace_period=5)
    await asyncio.wait_for(task, 5)
    took = time.monotonic() - t0
    done = await q.get_job(job.id)
    expect(done is not None and done.state == "completed", "stop() did not let the job finish")
    expect(took < 3, f"a graceful stop of a 0.5 s job took {took:.2f} s")
    return {"state": done.state if done else None, "returnvalue": done and done.returnvalue}


@scenario
async def stop_past_grace_cancels_and_run_returns(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    started = asyncio.Event()

    async def process(_j):
        started.set()
        await asyncio.sleep(30)

    w = ctx.worker(process)
    task = asyncio.create_task(w.run())
    job = await q.add("stuck")
    await asyncio.wait_for(started.wait(), 10)
    t0 = time.monotonic()
    await w.stop(grace_period=0.2)
    await asyncio.wait_for(task, 5)
    took = time.monotonic() - t0
    expect(took < 3, f"stop(grace_period=0.2) took {took:.2f} s")
    # A worker that was stopped can run again.
    again = asyncio.create_task(w.run())
    await asyncio.sleep(0.3)
    rerun_alive = not again.done()
    await w.stop(grace_period=1)
    with contextlib.suppress(Exception):
        await asyncio.wait_for(again, 5)
    return {"state_after_stop": await state_of(q, job.id), "rerun_alive": rerun_alive}


@scenario
async def worker_crash_and_restart_recovers_the_job(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    job = await q.add("survivor", {"n": 7})
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        __file__,
        "crash-worker",
        ctx.name,
        ctx.prefix,
        stdout=asyncio.subprocess.PIPE,
        env={**os.environ, "REDIS_URL": URL},
    )
    assert proc.stdout is not None
    line = await asyncio.wait_for(proc.stdout.readline(), 20)
    expect(line.decode().strip() == f"claimed {job.id}", f"crash worker said {line!r}")
    # Our own child, never pid 0 or 1: those signal a whole process group or everything.
    expect(proc.pid > 1, f"refusing to signal pid {proc.pid}")
    os.kill(proc.pid, signal.SIGKILL)
    await proc.wait()
    state_after_crash = await state_of(q, job.id)

    async def process(j):
        return j.data["n"] * 6

    w = ctx.worker(process, stalled_interval=300)
    stalled: list[str] = []
    w.on("stalled", stalled.append)
    async with running(w):
        value = await q.result(job.id, timeout=20)
    expect(proc.returncode == -signal.SIGKILL, f"crash worker exited {proc.returncode}")
    expect(value == 42, f"the recovered job answered {value!r}")
    return {
        "state_after_crash": state_after_crash,
        "result": value,
        "stalled_events": len(stalled),
    }


async def crash_worker(name: str, prefix: str) -> None:
    """The worker that is killed: claims one job, says so, then hangs inside it."""

    async def process(j):
        print(f"claimed {j.id}", flush=True)
        await asyncio.sleep(600)

    w = Worker(name, process, url=URL, prefix=prefix, lock_duration=600, stalled_interval=0)
    await w.run()


@scenario
async def redis_down_at_start(ctx: Ctx) -> dict[str, Any]:
    url = f"redis://127.0.0.1:{closed_port()}"
    q = Queue(ctx.name, url=url, prefix=ctx.prefix)
    out: dict[str, Any] = {}
    t0 = time.monotonic()
    try:
        await asyncio.wait_for(q.add("x"), 15)
        out["add"] = None
    except Exception as exc:
        out["add"] = type(exc).__name__
    out["add_bounded"] = time.monotonic() - t0 < 15

    async def process(_j):
        return None

    w = Worker(ctx.name, process, url=url, prefix=ctx.prefix)
    t0 = time.monotonic()
    try:
        await asyncio.wait_for(w.run(), 15)
        out["run"] = None
    except Exception as exc:
        out["run"] = type(exc).__name__
    out["run_bounded"] = time.monotonic() - t0 < 15
    await asyncio.wait_for(w.stop(grace_period=0), 10)
    await q.close()
    expect(out["add"] == "ConnectionError", f"add() with Redis down raised {out['add']}")
    expect(out["run"] == "ConnectionError", f"run() with Redis down raised {out['run']}")
    return out


@scenario
async def redis_down_mid_run_and_back(ctx: Ctx) -> dict[str, Any]:
    outage = Outage()
    await outage.up()
    q = ctx.queue()  # direct: the producer is not the one losing Redis
    ran: list[str] = []

    async def process(j):
        ran.append(j.name)

    w = ctx.worker(process, url=outage.url)
    task = asyncio.create_task(w.run())
    try:
        await q.add("before")
        expect(await until(lambda: "before" in ran, 10), "the worker never ran a job")
        await outage.down()
        proxied = Queue(ctx.name, url=outage.url, prefix=ctx.prefix)
        try:
            await asyncio.wait_for(proxied.add("lost"), 15)
            add_during = None
        except Exception as exc:
            add_during = type(exc).__name__
        await proxied.close()
        await asyncio.sleep(1.5)
        alive_during = not task.done()
        await outage.up()
        await q.add("after")
        recovered = await until(lambda: "after" in ran, 30)
    finally:
        await w.stop(grace_period=1)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, 10)
        with contextlib.suppress(Exception):
            await outage.down()
    expect(add_during == "ConnectionError", f"add() during the outage raised {add_during}")
    expect(alive_during, "run() ended while Redis was down")
    expect(recovered, "the worker did not process a job within 30 s of Redis coming back")
    return {"add_during": add_during, "alive_during": alive_during, "recovered": recovered}


@scenario
async def newer_data_model_is_refused(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    await q.add("stamp")  # the first write stamps the model this toro understands
    stamped = await q.redis.hget(q.keys.meta, "model")
    await q.redis.hset(q.keys.meta, "model", toro.DATA_MODEL_VERSION + 1)
    fresh = ctx.queue()
    try:
        await fresh.add("refused")
        add = None
    except IncompatibleDataModelError as exc:
        add = {"type": type(exc).__name__, "found": exc.found, "understood": exc.understood}

    async def process(_j):
        return None

    try:
        await asyncio.wait_for(ctx.worker(process).run(), 10)
        run = None
    except IncompatibleDataModelError as exc:
        run = type(exc).__name__
    expect(add is not None, "a producer wrote into a newer data model")
    expect(run == "IncompatibleDataModelError", "a worker ran against a newer data model")
    return {"stamped": stamped, "add": add, "run": run}


@scenario
async def connection_without_decode_responses_is_refused(ctx: Ctx) -> dict[str, Any]:
    raw = aioredis.Redis.from_url(URL)
    out = {
        "queue": await refusal(lambda: Queue(ctx.name, connection=raw, prefix=ctx.prefix)),
        "worker": await refusal(
            lambda: Worker(ctx.name, lambda _j: None, connection=raw, prefix=ctx.prefix)
        ),
    }
    await raw.aclose()
    expect(out["queue"] is not None, "Queue accepted a client that returns bytes")
    expect(out["worker"] is not None, "Worker accepted a client that returns bytes")
    return out


@scenario
async def shared_connection_and_close(ctx: Ctx) -> dict[str, Any]:
    shared = aioredis.Redis.from_url(URL, decode_responses=True)
    q = Queue(ctx.name, connection=shared, prefix=ctx.prefix)
    await q.add("x")
    await q.close()
    still_usable = await shared.ping()
    own = ctx.queue()
    job = await own.add("y")
    await own.close()
    try:
        await own.result(job.id, timeout=1)
        after_close = None
    except Exception as exc:
        after_close = error_of(exc)
    await shared.aclose()
    expect(still_usable, "close() closed a connection the caller owns")
    expect(after_close is not None, "result() on a closed queue did not raise")
    return {"caller_connection_usable": still_usable, "result_after_close": after_close}


@scenario
async def result_times_out(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()
    job = await q.add("nobody-runs-this")
    t0 = time.monotonic()
    try:
        await job.result(timeout=0.3)
        raised = None
    except Exception as exc:
        raised = type(exc).__name__
    took = time.monotonic() - t0
    expect(raised is not None, "result() returned for a job nobody ran")
    expect(took < 3, f"result(timeout=0.3) took {took:.2f} s")
    return {"raised": raised}


@scenario
async def a_failing_event_handler_does_not_hurt_the_worker(ctx: Ctx) -> dict[str, Any]:
    q = ctx.queue()

    def explode(*_args):
        raise RuntimeError("handler bug")

    async def process(j):
        return j.data

    w = ctx.worker(process)
    w.on("completed", explode)
    async with running(w):
        first = await (await q.add("a", 1)).result(timeout=10)
        second = await (await q.add("b", 2)).result(timeout=10)
    expect([first, second] == [1, 2], "a raising handler stopped the worker")
    return {"results": [first, second]}


# ---- runner -------------------------------------------------------------------------


async def run_one(name: str) -> dict[str, Any]:
    ctx = Ctx(name)
    try:
        observed = await asyncio.wait_for(SCENARIOS[name](ctx), SCENARIO_TIMEOUT)
        result = {"ok": True, "observed": json.loads(json.dumps(observed, default=str))}
    except Exception as exc:
        result = {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "trace": traceback.format_exc(),
        }
    finally:
        await ctx.cleanup()
    return result


async def run_all(names: list[str]) -> dict[str, Any]:
    results = {}
    for name in names:
        t0 = time.monotonic()
        results[name] = await run_one(name)
        results[name]["seconds"] = round(time.monotonic() - t0, 2)
        status = "ok" if results[name]["ok"] else "FAIL " + results[name]["error"]
        print(f"{name}: {status}", file=sys.stderr, flush=True)
    return results


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "crash-worker":
        asyncio.run(crash_worker(sys.argv[2], sys.argv[3]))
        return 0
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--only", nargs="*", default=None)
    args = parser.parse_args()
    if not URL:
        print("set REDIS_URL to a Redis you own", file=sys.stderr)
        return 2
    names = args.only or list(SCENARIOS)
    results = asyncio.run(run_all(names))
    Path(args.out).write_text(json.dumps(results, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
