# Scaling

How toro uses a machine, what to turn up first, and what the levers are actually
worth. Every number here comes from `tests/perf/harness.py` or `bench/bench.py`
against a local Redis; run them yourself, because the ratios travel and the absolute
numbers do not.

## The process model

One process runs one event loop. A worker with `concurrency=N` runs N **slots**, and
a slot is an `asyncio` task: it claims a job, awaits the processor, commits the
outcome, and goes back for the next one. Slots are not threads and not processes, so
N of them share one core for Python bytecode.

An idle slot parks a connection inside a blocking pop, so a worker needs at least as
many connections as it has slots. toro sizes the pool it opens at
`max(50, concurrency + 10)`; a connection you hand it is yours to size.

Everything that coordinates workers lives in Redis: locks, the stalled sweep, the
rate-limit bucket, the concurrency cap, the key queues. There is no leader and no
peer-to-peer anything, so a replica is just another process that connects.

## Slots, or replicas?

Turn up `concurrency` first. A slot costs a task and a connection, and jobs that are
mostly waiting on I/O overlap almost perfectly.

Add replicas when the loop is the bottleneck, which shows up as one of:

- The **blocked-loop warning** (see [Processing](processing.md)): something is
  starving the loop, and more slots on that loop will not help.
- Throughput flat while `toro_queue_depth` climbs: the loop is saturated with work it
  can do, and another core is what is missing.
- CPU pinned at one core's worth.

Replicas also fail independently: a process that dies takes its in-flight jobs to the
stalled sweep, and the others keep going.

## Threads help for blocking I/O, and only that

A sync processor runs in the worker's own thread pool, sized by `concurrency`. That
is the right answer for a library with no async version: the thread blocks, the loop
does not.

It is the wrong answer for CPU-bound work. Threads share the GIL, so four threads
computing do not compute four times faster, and a thread cannot be cancelled: a
`cancel_job()` ends the job while the work runs on, and `stop()` cannot interrupt it
either. For CPU-bound work, either hand it out of the process yourself:

```python
POOL = ProcessPoolExecutor()                  # once, at import: building one per job
                                              # re-imports your app on every job
def handle(job):                              # a sync processor, in a thread
    return POOL.submit(crunch, job.data).result()   # that waits on a process
```

or use a queue built for it. toro is an async queue, and it says so.

## What the levers are worth

The matrix, on a developer laptop against a local Redis, 2,000 jobs, `concurrency=20`
(`uv run python tests/perf/harness.py`):

| Cell | Enqueue/s | Process/s | vs plain asyncio |
|---|---|---|---|
| asyncio | 3,086 | 9,257 | 1.00x |
| asyncio + pipelined enqueue | 39,465 | 8,869 | 0.96x |
| asyncio + eager tasks | 3,143 | 8,212 | 0.89x |
| uvloop | 3,901 | 10,165 | 1.10x |
| uvloop + eager tasks | 3,367 | 10,444 | 1.13x |

Read in order of what it buys:

- **Pipelining the enqueue is worth about 13x** and is the only order-of-magnitude
  lever here. `Queue.pending()` sends a batch in one round trip; see
  [Producing](producing.md). If you enqueue in a loop, this is the change to make.
- **uvloop is worth about 1.1x** on the processing side. Real, and not the difference
  between a system that works and one that does not. toro bundles it for nobody: it
  is a dependency you choose, and the recipe is three lines.
- **An eager task factory costs about 6%** on plain asyncio and adds nothing on
  uvloop, which is the opposite of the folklore. Measure before adopting it.

To run under another loop, build it yourself; the stdlib has the API and toro has no
opinion:

```python
import asyncio, uvloop
with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
    runner.run(worker.run())
```

## Sizing Redis

A job costs about **43 Redis commands end to end** on the default path: ~11 to
enqueue it and ~32 to claim, renew, and settle it (`uv run python bench/bench.py`).
All of it is scripted, so those commands are not round trips: an enqueue is one, and
a job's whole lifecycle is a handful.

Connections: one per slot parked on the blocking pop, plus one per worker for the
cancellation channel, plus what your producers use. A dashboard adds its own.

## Caps hold across every replica

`global_concurrency`, `rate_limit` and `concurrency_key` are enforced inside the
Lua that claims a job, so they hold across every worker and every replica, not per
process. Set the same value in each replica: they are queue-wide settings that each
worker reports, and the dashboard warns when workers disagree.

## What to watch

| Signal | Where | Means |
|---|---|---|
| `toro_queue_depth{state="wait"}` climbing | [Operating](operating.md) | Arrivals outpacing throughput: add slots, then replicas. |
| `toro_queue_depth{state="held"}` climbing | same | Work serialized behind a `concurrency_key`, not behind a worker. |
| Blocked-loop warnings | logs, `blocked` event | Something is starving the loop; more slots will not help. |
| `rate(toro_jobs_total{outcome="failed"})` | same | The failure rate; cancellations are counted separately and are not in it. |
| Stalled recoveries | worker logs | Locks expiring: jobs longer than `lock_duration`, or a blocked loop. |
