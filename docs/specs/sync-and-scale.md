# Sync processors, and the loop they must not block

## Problem and outcome

A processor is awaited: `await self.processor(job)` in `Worker._handle`. Hand toro a
plain `def` and the await raises `TypeError: object dict can't be used in 'await'
expression`, after the function has already run to completion inside the event loop,
which is the worst of both answers. People arriving from the sync queues bring sync
handlers, and the first thing toro does with one is break in a way that does not name
the problem.

The larger version has no error at all: an async processor that calls a blocking
library starves the loop. Lock renewal is a coroutine, so a starved loop stops
renewing, the stalled sweep takes the job back, and a queue that looks healthy runs
work twice. Nothing in toro notices.

Outcome: a sync processor runs in a thread and does not block the loop. A processor
that blocks the loop anyway is reported, by name, before the lock it holds expires.

## Design

- **Decide by inspection, once, at construction.** `asyncio.iscoroutinefunction`
  picks the arm, plus the same question of `__call__` for a callable object, which is
  the shape a class-based processor takes. A `functools.partial` is unwrapped by hand
  first: `iscoroutinefunction` sees through one, but the `__call__` fallback would
  then ask the partial about its own.
  Calling the processor to find out what it returns would run a sync one inside the
  loop, which is the thing being avoided.
- **An awaitable result is awaited anyway.** If the inspection is wrong (a decorator
  that hides a coroutine function), the thread returns an un-started coroutine, and
  the job's "result" would be a coroutine object that fails to serialize, with a
  `coroutine was never awaited` warning as the only clue. Awaiting what comes back
  costs a line and turns a silent corruption into correct behavior.
- **A thread per slot, owned by the worker.** `asyncio.to_thread` uses the loop's
  default executor: shared process-wide and sized `min(32, cpu + 4)`, so a worker with
  `concurrency=64` queues sync jobs behind a pool it does not control, and a job
  waiting for a thread holds its lock while waiting. The worker owns a
  `ThreadPoolExecutor(max_workers=concurrency)`, created on the first sync job so an
  all-async worker pays nothing, and shut down in `stop()`.
- **A sync processor cannot be cancelled, so cancelling one waits for it.** A thread
  is not a task: cancelling the await would raise in the coroutine and leave the
  thread running. That frees the worker's slot while the pool has one thread fewer
  than it has slots (the next job is then claimed, locked and renewed while sitting
  unstarted), and commits a terminal state that hands on the job's concurrency key
  while the work that holds it carries on. A sync cancellation is therefore recorded
  and not interrupted: the job ends `cancelled` when its thread returns, which is the
  only moment at which capacity and keys are honestly free. A thread still running at
  interpreter exit holds the process open (executor threads are joined at exit), which
  is inherent and documented where people meet it.
- **The detector measures the loop, not the processor.** A watchdog task sleeps a
  known interval and compares elapsed wall clock against it: the difference is how
  long the loop could not run anything. Nothing needs instrumenting, and it sees every
  cause, including ones inside dependencies.
- **Warn against the lock, not against a round number.** The threshold that costs a
  job is the renewal: lag near `lock_renew_time` means a renewal is already late. The
  warning names the jobs in flight, because the processor that blocked the loop is one
  of them, and fires once per episode rather than once per tick.
- **No bundled loop dependency.** `asyncio.Runner(loop_factory=...)` is the stdlib
  API, and toro has no CLI to hang a flag on: this is a documented recipe and a
  measurement, not a dependency.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| SS-001 | A sync processor runs to completion and its return value is the job's result, with the loop free throughout: a concurrent heartbeat keeps its cadence while a sync job sleeps. | `tests/integration/test_sync_processors.py::test_a_sync_processor_does_not_block_the_loop` |
| SS-002 | Sync and async processors are the same everywhere else: retries, failure, cancellation state, flow settling, metrics. | `::test_a_sync_processor_is_a_processor` |
| SS-003 | The threads are bounded by `concurrency` and owned by the worker: `stop()` leaves none running, and an all-async worker never creates a pool. | `::test_a_sync_worker_reports_its_concurrency`, `::test_sync_jobs_run_side_by_side`, `::test_the_pool_is_given_back_when_the_worker_stops`, `::test_an_async_worker_starts_no_threads` |
| SS-004 | Cancelling a sync job records the request and ends the job `cancelled` when its thread returns, keeping its slot and its concurrency key until then. | `::test_cancelling_a_sync_job_ends_it_when_its_thread_does`, `::test_a_cancelled_sync_job_keeps_the_slot_its_thread_still_holds`, `::test_cancelling_a_sync_job_does_not_hand_its_key_on_early` |
| SS-005 | A processor that blocks the loop past the threshold warns once, naming the jobs in flight; a merely busy loop, and an idle one, do not. | `tests/integration/test_blocked_loop.py::test_a_blocking_processor_is_named`, `::test_a_run_of_blocking_jobs_still_warns_once`, `::test_a_busy_loop_is_not_a_blocked_one`, `::test_an_idle_worker_never_warns` |
| SS-006 | The detector is one timer per worker and costs nothing measurable when nothing blocks. | measured: one `asyncio.sleep` per tick, at most one per second, and no Redis command at all. The same 2,000-job cell, median of three runs each: 8,703 jobs/s with it off, 8,710 with it on (1.001x), against a run-to-run spread of 8%. |
| SS-007 | A processor that is neither (a callable object with an async `__call__`, a partial, a partial around one) is detected correctly, and a misdetected one still produces the right result. | `tests/unit/test_processor_kind.py`, `tests/integration/test_sync_processors.py::test_a_processor_whose_kind_was_misread_still_returns_its_value` |
| SS-008 | A perf suite axis with checked-in baselines: loop choice against task factory against pipelining, each cell the median of three runs with its spread beside it. | `tests/perf/harness.py`, `tests/perf/baselines.json`, guarded by `tests/perf/test_harness.py` |

## Out of scope

- A process pool. CPU-heavy users pick the sync queues; the detector covers the trap
  that nobody notices, which is the dangerous one.
- Cancelling a running thread. Python cannot, and pretending otherwise is worse than
  saying so.
- `enqueue_on_commit()`, which is the same milestone but a different concern: it has
  its own spec.

## Risks

- **A sync processor holds a thread for its whole run**, so `concurrency=64` with
  sync handlers means 64 threads. The pool is sized by `concurrency` for exactly that
  reason, and the scaling page says what a thread costs.
- **The detector's own timer can be the thing that is late**, which is the point: it
  measures its own lateness. False positives under a heavily loaded machine are
  possible; the threshold is tied to the lock so the warning still means something.
- **A warning per episode can still be noisy** under a processor that blocks
  repeatedly. One per episode with the jobs named is the compromise; a rate limit can
  follow if it is not enough.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | SS-007 | Detect the processor's kind | `toro/worker.py` | unit, red first |
| 2 | SS-001, SS-002 | Run a sync processor in the worker's pool | `toro/worker.py` | integration, red first |
| 3 | SS-003 | The pool's lifecycle: lazy, bounded, shut down | `toro/worker.py` | integration |
| 4 | SS-004 | Cancellation, and what it cannot do | `toro/worker.py` | integration |
| 5 | SS-005 | The blocked-loop detector | `toro/worker.py` | integration, red first |
| 6 | SS-006, SS-008 | The perf axis and baselines | `tests/perf/` | measured |
| 7 | | Docs: a scaling page, processing, upgrading | `docs/` | review |
| 8 | | Prove: full suite, mutation audit, adversarial review | | evidence |
