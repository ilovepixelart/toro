"""Open-loop load harness for toro — built to the benchmarking methodology.

Why it's shaped this way (the methodology, enforced in code):
  * OPEN-LOOP load: the producer enqueues on a fixed wall-clock schedule (rate λ),
    never gating on completions — what real producers do, and it avoids Coordinated
    Omission (a closed loop self-throttles and hides saturation).
  * Latency recorded with HdrHistogram and **CO-corrected** against the intended
    interval (1/λ), so a stall doesn't silently omit the slow samples it caused.
  * The wait clock starts at ENQUEUE, not dequeue (dequeue-start re-introduces CO).
  * A WARM-UP window is discarded; only the steady-state window is measured.
  * Reports PERCENTILES (p50..p99.9, max) — never an average — plus achieved
    throughput, backlog/drops, a Little's-Law cross-check, and Redis cost.

Each worker records {enq, proc, fin} per job into a Redis list (reliable across
processes, unlike pub/sub timing); the harness drains it once at the end.

Run, e.g.:
    uv run python tests/load/harness.py --rate 2000 --workers 4 --concurrency 50 \
        --duration 20 --warmup 5 --work-ms 0
Sweep λ across runs to plot the throughput-vs-latency curve and find the knee.
"""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import time

import redis.asyncio as aioredis
from hdrh.histogram import HdrHistogram

from toro import Queue, Worker

QUEUE = "loadtest"
PREFIX = "torobench"
_STOP_KEY = f"{PREFIX}:{QUEUE}:bench-stop"
_RESULTS_KEY = f"{PREFIX}:{QUEUE}:bench-results"


def _now_us() -> float:
    return time.time() * 1e6  # system wall clock — comparable across processes


# ---- worker subprocess --------------------------------------------------------


def _worker_entry(url: str, concurrency: int, work_ms: float) -> None:
    asyncio.run(_run_worker(url, concurrency, work_ms))


async def _run_worker(url: str, concurrency: int, work_ms: float) -> None:
    sink = aioredis.from_url(url, decode_responses=True)  # own conn for result records

    async def processor(job):
        if work_ms:
            await asyncio.sleep(work_ms / 1000.0)  # simulate I/O-bound work
        proc_us = (job.processed_on or 0) * 1000.0
        await sink.rpush(_RESULTS_KEY, f"{job.data['_enq']} {proc_us} {_now_us()}")

    worker = Worker(
        QUEUE, processor, url=url, prefix=PREFIX, concurrency=concurrency, stalled_interval=0
    )
    task = asyncio.create_task(worker.run())
    # Poll a Redis stop flag — a cross-process signal an asyncio.Event can't carry.
    while not await worker.redis.exists(_STOP_KEY):  # noqa: ASYNC110
        await asyncio.sleep(0.05)
    await worker.stop(grace_period=2.0)
    task.cancel()
    await sink.aclose()


# ---- producer + measurement (parent) -----------------------------------------


async def _drive(args) -> dict:
    q = Queue(QUEUE, url=args.url, prefix=PREFIX)
    for k in await q.redis.keys(q.keys.base + "*"):
        await q.redis.delete(k)
    await q.redis.delete(_STOP_KEY, _RESULTS_KEY)

    interval_us = 1e6 / args.rate
    payload = "x" * args.payload_bytes
    t0 = time.time()
    warm_until_us = (t0 + args.warmup) * 1e6
    end_at = t0 + args.warmup + args.duration
    enqueued = enqueued_steady = 0

    cmd_before = await _commandstats(q.redis)
    next_send = _now_us()
    while time.time() < end_at:
        now = _now_us()
        while next_send <= now and time.time() < end_at:  # catch up to the schedule
            await q.add("bench", {"_enq": now, "p": payload}, remove_on_complete=True)
            enqueued += 1
            if now >= warm_until_us:
                enqueued_steady += 1
            next_send += interval_us
        await asyncio.sleep(min(interval_us, 2000) / 1e6)

    await asyncio.sleep(1.0)  # let in-flight jobs drain
    await q.redis.set(_STOP_KEY, "1")
    cmd_after = await _commandstats(q.redis)

    # Drain the result records and build histograms over the STEADY window only.
    records = await q.redis.lrange(_RESULTS_KEY, 0, -1)
    hist = {m: HdrHistogram(1, 120_000_000, 3) for m in ("wait", "run", "e2e")}
    measured = 0
    for rec in records:
        enq, proc, fin = (float(x) for x in rec.split())
        if enq < warm_until_us:  # discard warm-up
            continue
        measured += 1
        hist["e2e"].record_corrected_value(max(1, int(fin - enq)), int(interval_us))
        if proc:
            hist["wait"].record_corrected_value(max(1, int(proc - enq)), int(interval_us))
            hist["run"].record_value(max(1, int(fin - proc)))

    info = {
        k: v
        for k, v in (await q.redis.info()).items()
        if k in ("used_memory_human", "connected_clients", "blocked_clients")
    }
    await q.close()
    return {
        "enqueued": enqueued,
        "enqueued_steady": enqueued_steady,
        "completed": len(records),
        "measured": measured,
        "hist": hist,
        "cmd": _cmd_delta(cmd_before, cmd_after),
        "info": info,
        "duration": args.duration,
    }


async def _commandstats(redis) -> dict:
    raw = await redis.info("commandstats")
    return {k: int(v.get("calls", 0)) if isinstance(v, dict) else 0 for k, v in raw.items()}


def _cmd_delta(before: dict, after: dict) -> dict:
    return {k: after[k] - before.get(k, 0) for k in after if after[k] - before.get(k, 0) > 0}


# ---- reporting ----------------------------------------------------------------


def _report(args, res: dict) -> None:
    measured, dur = res["measured"], res["duration"]
    throughput = measured / dur if dur else 0
    backlog = res["enqueued_steady"] - measured
    pcts = [50, 90, 95, 99, 99.9]

    print("\n" + "=" * 70)
    print(
        f" toro load — OPEN-LOOP   λ={args.rate:g}/s  workers={args.workers}  "
        f"concurrency={args.concurrency}  work={args.work_ms}ms"
    )
    print("=" * 70)
    print(
        f" enqueued(steady)={res['enqueued_steady']}  completed(steady)={measured}  "
        f"total_completed={res['completed']}"
    )
    sat = "⚠️  PAST SATURATION (backlog growing)" if backlog > args.rate * 0.1 else "OK — kept up"
    print(f" achieved throughput ≈ {throughput:,.0f} jobs/s    backlog = {backlog}   {sat}")

    print(f"\n {'metric':<6}" + "".join(f"{f'p{p}':>10}" for p in pcts) + f"{'max':>10}   (ms)")
    for m in ("wait", "run", "e2e"):
        if hist_count(res["hist"][m]) == 0:
            continue
        cells = "".join(f"{res['hist'][m].get_value_at_percentile(p) / 1000:>10.2f}" for p in pcts)
        cells += f"{res['hist'][m].get_max_value() / 1000:>10.2f}"
        print(f" {m:<6}{cells}")

    if hist_count(res["hist"]["e2e"]):
        w = res["hist"]["e2e"].get_mean_value() / 1e6
        print(
            f"\n Little's Law:  L = λ·W ≈ {args.rate:g} × {w * 1000:.1f}ms "
            f"≈ {args.rate * w:,.0f} jobs in flight"
        )
    if res["info"]:
        print(f" Redis: {res['info']}")
    if res["cmd"]:
        top = sorted(res["cmd"].items(), key=lambda kv: -kv[1])[:6]
        print(
            " Redis cmd calls (Δ): "
            + "  ".join(f"{k.replace('cmdstat_', '')}={v:,}" for k, v in top)
        )
    print(" (note: the harness adds 1 RPUSH/job for measurement — visible above)")
    print("=" * 70 + "\n")


def hist_count(h) -> int:
    return h.get_total_count()


def main() -> None:
    ap = argparse.ArgumentParser(description="toro open-loop load harness")
    ap.add_argument("--rate", type=float, default=1000, help="target arrival rate λ (jobs/s)")
    ap.add_argument("--workers", type=int, default=2, help="worker processes (~1 per core)")
    ap.add_argument("--concurrency", type=int, default=20, help="async jobs in flight per worker")
    ap.add_argument("--duration", type=float, default=15, help="steady-state window (s)")
    ap.add_argument("--warmup", type=float, default=5, help="warm-up window to discard (s)")
    ap.add_argument("--work-ms", type=float, default=0, help="simulated per-job I/O work (ms)")
    ap.add_argument("--payload-bytes", type=int, default=64)
    ap.add_argument("--url", default="redis://localhost:6379")
    args = ap.parse_args()

    procs = [
        mp.Process(target=_worker_entry, args=(args.url, args.concurrency, args.work_ms))
        for _ in range(args.workers)
    ]
    for p in procs:
        p.start()
    try:
        _report(args, asyncio.run(_drive(args)))
    finally:
        for p in procs:
            p.join(timeout=4)
            if p.is_alive():
                p.terminate()


if __name__ == "__main__":
    main()
