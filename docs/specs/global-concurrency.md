# Global concurrency cap

## Problem and outcome

A queue's in-flight work is unbounded across processes: each worker's
`concurrency` multiplies by the number of worker processes, and the rate limiter
bounds job *starts*, not *occupancy*. Long jobs against a connection-limited
downstream (a database pool, an API with a max-concurrent-requests limit) need a
hard cap on jobs active at once, queue-wide.

Outcome: a `global_concurrency` Worker option, enforced atomically in the claim
script. Slots free themselves on every path a job leaves `active`, including a
worker crash.

## Design

- **Option.** `Worker(..., global_concurrency: int | None = None)`. Mirrors
  `rate_limit`: a constructor option sent to Lua via ARGV, and all workers on a
  queue should pass the same value. A changed cap takes effect as workers
  restart.
- **Enforcement.** The shared `acquireNext` routine returns `false` when the cap
  is set and `LLEN active >= cap`. All three scripts that claim pass it the cap:
  `MOVE_TO_ACTIVE` and both finish scripts.
- **Only one script can be refused.** The only write that grows `active` is the
  `LPUSH` in `acquireNext`. In the finish scripts that call always follows the
  finisher's own successful `LREM` (or the script has already returned), so a
  fetch after a finish is a swap: it cannot raise occupancy. Only
  `MOVE_TO_ACTIVE` can, so the safety of the cap rests on that one path. The
  finish scripts carry the cap so `acquireNext` knows when the queue is full.
- **Fails closed, before any write.** A caller that omits the cap argument gets
  a script error, not a claim that ignores the limit. Every script validates
  the cap before its first write: Redis does not roll a script back, so an
  error raised after a finish had committed would leave that commit standing. The option is normalized with `int()`, so
  an int subclass cannot reach Lua as an unreadable repr.
- **No counter.** Occupancy is read from the `active` list itself, so there is
  nothing to leak: every existing exit from `active` (complete, fail, stalled
  sweep, removal) frees the slot by construction.
- **A capped claim touches nothing.** The guard runs before the pop: no
  pop-and-put-back, no rate-limit token spent, no attempt consumed.
- **Wakeup.** A capped worker parks on the marker exactly like a paused one.
  `acquireNext` re-arms the marker after a claim only while a slot is still
  free, so a claim that fills the last slot does not wake a worker just to
  turn it away. A finish that re-enqueues before it fetches (an immediate
  retry, a flow child releasing its parent) still arms the marker through
  `enqueue`, as every `add` does: one refused wake each. Two paths
  free a slot without claiming and arm the marker when jobs are waiting: a
  finish with `fetch=0` (a draining worker), and a stalled job that fails
  terminally. A draining worker's own parked loop can pop that wake first, so
  a loop that pops a marker while shutting down hands it on before it exits.
  Removal of an active job deliberately does not wake: its processor may still
  be running, so an eager wake would exceed the real cap. When the processor
  ends, its finish comes back lock-lost, and the worker arms the marker then:
  the one moment it knows the slot is really free.
- **Visibility.** The heartbeat record and `Queue.workers()` gain
  `global_concurrency` (0 when unset), which is what the dashboard needs to show
  the cap and derive "waiting on cap" (`active >= cap` with jobs waiting).
- **Cost.** Up to two `LLEN` calls (O(1)) per claim, only when the cap is set.
  Unset, the claim path issues no extra commands. Measured across 3 processes
  with 6 jobs in flight on zero-work jobs: unset, set-but-unreached, and
  saturated with 24 loops parked all run about 9,000 jobs/s at 1.0 script calls
  per job. With 100 ms jobs a saturated cap of 3 drained 60 jobs in 2.09 s
  against a 2.0 s optimum. An `add` while the queue is full still wakes one
  parked loop that is turned away: the producer does not know the cap.
- **Slots stick.** A finisher swaps its own slot, so under a full queue the
  workers holding slots keep them and others can sit idle until a holder
  drains, stops, or crashes.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| GC-001 | With `global_concurrency=N`, no more than N jobs are active at any instant across several workers whose summed `concurrency` exceeds N, the high-water mark reaches N, and every job still completes. Covers the initial claim and the fetch after both complete and fail. | `tests/integration/test_global_concurrency.py::test_cap_holds_across_workers`, plus the seeded fuzzer in `test_invariants.py` asserting `active <= cap` after every step |
| GC-002 | A capped claim leaves the queue untouched: the job keeps its `prioritized` score, no attempt is consumed, no rate-limit token is spent. | `::test_capped_claim_touches_nothing` |
| GC-003 | A worker that dies holding slots frees them through the stalled sweep; the queue drains afterwards and never wedges. | `::test_crashed_worker_slots_are_recovered` (mutation-verified: fails with the sweep disabled) |
| GC-004 | A freed slot wakes a parked worker well under `block_timeout` on both non-claiming release paths: a finish with `fetch=0`, and a stalled job failing terminally. A draining worker's own parked loop hands the wake on instead of swallowing it. | `::test_freed_slot_wakes_parked_worker` (parametrized per path), `::test_draining_worker_passes_the_wake_on` |
| GC-005 | Unset (the default) leaves claim behavior and return shapes unchanged, and workers run up to their summed `concurrency`. The two release paths of GC-004 arm the marker whether or not a cap is set, which costs an uncapped queue at most one harmless wake. | `::test_unset_cap_is_unbounded` plus the existing integration suite green |
| GC-006 | A non-positive or non-integer `global_concurrency` raises `ValueError` at construction. An int subclass (an `IntEnum`) is stored as a plain int and caps like one. | `tests/unit/test_worker_options.py::test_global_concurrency_validation`, `::test_global_concurrency_is_stored_as_a_plain_int`, `tests/integration/test_global_concurrency.py::test_int_subclass_cap_is_enforced` |
| GC-007 | The heartbeat record and `Queue.workers()` expose `global_concurrency`. | `tests/integration/test_workers.py::test_presence_reports_global_concurrency` |
| GC-008 | The re-arm after a claim is skipped when that claim filled the last slot, on the initial claim and on the fetch after both complete and fail. While a slot is free and jobs wait, the marker is armed. A finish that re-enqueues before it fetches arms it regardless. | `tests/integration/test_global_concurrency.py::test_full_cap_does_not_wake_a_parked_worker` |
| GC-009 | A claim that omits the cap argument is a script error and claims nothing. A finish that fetches without it errors before its first write: nothing is committed and the lock stands. | `::test_missing_cap_argument_is_an_error`, `::test_finish_with_a_missing_cap_commits_nothing` |
| GC-010 | The slot of a removed active job is reused as soon as its processor ends, not at the next idle re-poll, and not before. | `::test_slot_of_a_removed_job_is_reused_once_its_processor_ends` |

## Out of scope

- A per-key limit (`concurrency_key`): its own spec, next.
- A worker event when capped. It needs a new script sentinel to tell "capped"
  from "empty"; the dashboard derives the condition from Redis instead.
- Changing the cap at runtime. A value stored in Redis that overrides the
  option can be added later without breaking this API.
- The matador "waiting on cap" UI: a paired follow-up once GC-007 lands.

## Risks

- **The swap invariant.** The cap is safe only while `acquireNext` stays the
  sole writer to `active` and the finish scripts keep removing before they
  fetch. The fuzzer check in GC-001 is the tripwire.
- **Mixed config.** Workers passing different caps each enforce their own, as
  happens during any rollout that introduces or changes the cap. A finisher
  refused by its own lower cap frees a slot without a wake, so a worker with
  room can wait up to `block_timeout` for it. The delay is bounded by the idle
  re-poll and ends with the rollout. Documented the same way `rate_limit` is.
- **False stalls.** The cap bounds claimed jobs. A job reclaimed after a false
  stall can run twice, exactly as it can today, which briefly exceeds the cap
  in real work while `active` stays within it.
- **Silent skips.** Integration tests skip when no Redis answers on
  `localhost:6379`. Every check above must run against a live Redis and the run
  must show zero skips.

## Decisions

1. The cap is a Worker option, not a queue-level value in Redis: the smallest
   change, no new keys, and the same contract `rate_limit` already has.
2. No "capped" worker event.
3. A missing cap argument is a script error. A limit must not fail open.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | GC-006 | Add the option, validate it, store it (not yet enforced) | `toro/worker.py` | unit, red first |
| 2 | GC-001, GC-002, GC-005 | Write the cap tests (red for the right reason: the option exists but nothing enforces it), then the `LLEN` guard and the cap ARGV | `toro/scripts.py`, `toro/worker.py`, tests | high-water mark via a shared counter; direct script calls for GC-002 |
| 3 | GC-001 | Cap invariant in the seeded fuzzer | `tests/integration/test_invariants.py` | `active <= cap` after every step |
| 4 | GC-003 | Crash recovery test | tests only | zombie worker, expired locks, sweep; mutation check with the sweep off |
| 5 | GC-004 | Arm the marker on the two non-claiming release paths | `toro/scripts.py` | large `block_timeout`, assert prompt start, red first per path |
| 6 | GC-007 | Presence field and `workers()` | `toro/worker.py`, `toro/queue.py` | integration |
| 7 | | Docs: `processing.md`, `concepts.md`, README feature row | docs | review |
| 8 | | Prove: lint, types, full suite with zero skips, a saturated-cap load run, a multi-process run of a capped fleet | | evidence captured |
| 9 | GC-006 | Normalize the option with `int()` | `toro/worker.py` | unit and integration, red first |
| 10 | GC-004 | A draining loop hands on a marker it popped | `toro/worker.py` | build the blocked-client ordering that swallows the wake, red first |
| 11 | GC-008, GC-009 | Move the cap into `acquireNext`, re-arm only while a slot is free, pass the cap from both finish scripts | `toro/scripts.py`, `toro/worker.py` | assert on the marker key, red first; re-measure the saturated load run |
