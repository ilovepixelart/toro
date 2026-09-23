"""The perf matrix: what actually moves toro's throughput (docs/specs/sync-and-scale.md).

Three axes, because these are the three levers people reach for and only some of
them pay:

    loop      asyncio        | uvloop        (when installed)
    tasks     default        | eager         (3.12+)
    enqueue   one at a time  | pipelined     (`Queue.pending`)

Run it:

    uv run python tests/perf/harness.py                 # the whole matrix
    uv run python tests/perf/harness.py --check         # against the baselines
    uv run python tests/perf/harness.py --write         # record new baselines

A cell is jobs/s, wall clock, against a local Redis. Machines differ by more than the
differences being measured, so `--check` compares the SHAPE of a run (each cell
against the plain asyncio cell of that same run) rather than absolute numbers against
another machine's. That is the only comparison a checked-in baseline can honestly
make.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import pathlib
import sys
import time
from typing import Any

from redis.asyncio import Redis

from toro import Queue, Worker

URL = "redis://localhost:6379"
PREFIX = "perf"
BASELINES = pathlib.Path(__file__).with_name("baselines.json")
# Enough that startup is not the measurement, small enough to run in a break.
JOBS = 2000
CONCURRENCY = 20
# How far a ratio may drift before it means something. A wall-clock benchmark on a
# working machine is noisy; a shape that moves more than this is a real change.
TOLERANCE = 0.25
# A cell that is not finishing is a bug in the harness or the queue, not a slow
# machine: say so rather than hanging the run.
STUCK_AFTER = 120.0
BASE_CELL = "asyncio"


async def _clear(redis: Redis) -> None:
    keys = await redis.keys(f"{PREFIX}:*")
    if keys:
        await redis.delete(*keys)


async def _noop(job: Any) -> int:
    return 1


async def _enqueue(queue: Queue, *, pipelined: bool) -> float:
    start = time.perf_counter()
    if pipelined:
        pending = queue.pending()
        for i in range(JOBS):
            pending.add("perf", {"i": i})
        await pending.flush()
    else:
        for i in range(JOBS):
            await queue.add("perf", {"i": i})
    return time.perf_counter() - start


async def _drain(queue: Queue) -> float:
    worker = Worker(
        queue.name,
        _noop,
        url=URL,
        prefix=PREFIX,
        concurrency=CONCURRENCY,
        stalled_interval=0,
        blocked_warning=0,  # a benchmark blocks its own loop; that is not news
    )
    start = time.perf_counter()
    running = asyncio.create_task(worker.run())
    # The lifetime counter, not `counts()`: retention bounds the completed set, so a
    # run larger than the bound would wait for a number the set can never reach.
    deadline = start + STUCK_AFTER
    while (await queue.lifetime_totals())["completed"] < JOBS:
        if time.perf_counter() > deadline:
            done = await queue.lifetime_totals()
            raise RuntimeError(f"drain stalled at {done} after {STUCK_AFTER}s")
        await asyncio.sleep(0.01)
    elapsed = time.perf_counter() - start
    await worker.stop(grace_period=0)
    running.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await running
    return elapsed


async def _cell(*, pipelined: bool) -> dict[str, float]:
    queue = Queue("bench", url=URL, prefix=PREFIX)
    try:
        await _clear(queue.redis)
        enqueue = await _enqueue(queue, pipelined=pipelined)
        process = await _drain(queue)
        await _clear(queue.redis)
    finally:
        await queue.close()
    return {"enqueue_per_s": JOBS / enqueue, "process_per_s": JOBS / process}


def _loops() -> dict[str, Any]:
    """The loops this interpreter can actually run. uvloop is a dev dependency and
    not every platform has it, so its absence skips its cells rather than failing."""
    factories: dict[str, Any] = {"asyncio": asyncio.new_event_loop}
    try:
        import uvloop  # imported here: optional, and only for this axis
    except ImportError:
        return factories
    factories["uvloop"] = uvloop.new_event_loop
    return factories


def run_cell(*, loop_factory: Any, eager: bool, pipelined: bool) -> dict[str, float]:
    """One cell, in its own loop.

    Not `asyncio.run`: the loop is the axis, so it is built here, and the task
    factory is set on it before anything runs.
    """
    loop = loop_factory()
    try:
        if eager:
            loop.set_task_factory(asyncio.eager_task_factory)
        return loop.run_until_complete(_cell(pipelined=pipelined))
    finally:
        loop.close()


def matrix() -> dict[str, dict[str, float]]:
    """Every runnable cell, named `<loop>[+eager][+pipelined]`."""
    eager_available = hasattr(asyncio, "eager_task_factory")
    out: dict[str, dict[str, float]] = {}
    for loop_name, factory in _loops().items():
        for eager in (False, True) if eager_available else (False,):
            for pipelined in (False, True):
                name = loop_name + ("+eager" if eager else "") + ("+pipelined" if pipelined else "")
                out[name] = run_cell(loop_factory=factory, eager=eager, pipelined=pipelined)
    return out


def _shape(results: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    """Each cell against the plain asyncio cell of the same run.

    The ratio is what survives a change of machine; the absolute numbers are not
    comparable between two of them, and a baseline that pretended otherwise would
    fail on every laptop but the one it was recorded on.
    """
    base = results[BASE_CELL]
    return {
        name: {metric: value / base[metric] for metric, value in cell.items()}
        for name, cell in results.items()
    }


def _report(results: dict[str, dict[str, float]]) -> None:
    shape = _shape(results)
    print(f"{'cell':24} {'enqueue/s':>12} {'process/s':>12} {'vs asyncio':>12}")
    for name, cell in results.items():
        ratio = shape[name]["process_per_s"]
        print(
            f"{name:24} {cell['enqueue_per_s']:12,.0f} {cell['process_per_s']:12,.0f} "
            f"{ratio:11.2f}x"
        )


def _check(results: dict[str, dict[str, float]]) -> int:
    if not BASELINES.exists():
        print(f"no baselines at {BASELINES}; run with --write", file=sys.stderr)
        return 1
    recorded = json.loads(BASELINES.read_text())["shape"]
    shape = _shape(results)
    drifted = []
    for name, cell in recorded.items():
        if name not in shape:  # a loop this machine does not have
            continue
        for metric, expected in cell.items():
            actual = shape[name][metric]
            if abs(actual - expected) > TOLERANCE * max(expected, 1e-9):
                drifted.append(f"{name}.{metric}: {expected:.2f}x recorded, {actual:.2f}x now")
    for line in drifted:
        print("DRIFT", line)
    print("shape holds" if not drifted else f"{len(drifted)} cell(s) drifted")
    return 1 if drifted else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="compare against the baselines")
    parser.add_argument("--write", action="store_true", help="record these as the baselines")
    args = parser.parse_args()

    results = matrix()
    _report(results)
    if args.write:
        BASELINES.write_text(
            json.dumps(
                {
                    "jobs": JOBS,
                    "concurrency": CONCURRENCY,
                    "note": "ratios against the plain asyncio cell of the same run",
                    "shape": _shape(results),
                },
                indent=2,
            )
            + "\n"
        )
        print(f"wrote {BASELINES}")
    if args.check:
        return _check(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
