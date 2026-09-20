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
- **Enforcement in one script.** `MOVE_TO_ACTIVE` returns `false` when the cap
  is set and `LLEN active >= cap`, before it calls `acquireNext`. The shared
  `acquireNext` routine and both finish scripts are untouched.
- **Why one script is enough.** The only write that grows `active` is the
  `LPUSH` in `acquireNext`. In the finish scripts that call always follows the
  finisher's own successful `LREM` (or the script has already returned), so a
  fetch after a finish is a swap: it cannot raise occupancy. Only
  `MOVE_TO_ACTIVE` can, so only it checks.
- **No counter.** Occupancy is read from the `active` list itself, so there is
  nothing to leak: every existing exit from `active` (complete, fail, stalled
  sweep, removal) frees the slot by construction.
- **A capped claim touches nothing.** The guard runs before the pop: no
  pop-and-put-back, no rate-limit token spent, no attempt consumed.
- **Wakeup.** A capped worker parks on the marker exactly like a paused one.
  Two paths free a slot without claiming and arm the marker when jobs are
  waiting: a finish with `fetch=0` (a draining worker), and a stalled job that
  fails terminally. Removal of an active job deliberately does not: its
  processor may still be running, so an eager wake would exceed the real cap;
  a parked worker notices within `block_timeout`.
- **Visibility.** The heartbeat record and `Queue.workers()` gain
  `global_concurrency` (0 when unset), which is what the dashboard needs to show
  the cap and derive "waiting on cap" (`active >= cap` with jobs waiting).
- **Cost.** One `LLEN` (O(1)) per claim attempt, only when the cap is set.
  Unset, the claim path issues no extra commands. While saturated, each finish
  or add wakes one parked loop that is turned away by the guard: one cheap
  script call, no spin (the bounced loop does not re-arm the marker).

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| GC-001 | With `global_concurrency=N`, no more than N jobs are active at any instant across several workers whose summed `concurrency` exceeds N, the high-water mark reaches N, and every job still completes. Covers the initial claim and the fetch after both complete and fail. | `tests/integration/test_global_concurrency.py::test_cap_holds_across_workers`, plus the seeded fuzzer in `test_invariants.py` asserting `active <= cap` after every step |
| GC-002 | A capped claim leaves the queue untouched: the job keeps its `prioritized` score, no attempt is consumed, no rate-limit token is spent. | `::test_capped_claim_touches_nothing` |
| GC-003 | A worker that dies holding slots frees them through the stalled sweep; the queue drains afterwards and never wedges. | `::test_crashed_worker_slots_are_recovered` (mutation-verified: fails with the sweep disabled) |
| GC-004 | A freed slot wakes a parked worker well under `block_timeout` on both non-claiming release paths: a finish with `fetch=0`, and a stalled job failing terminally. | `::test_freed_slot_wakes_parked_worker` (parametrized per path) |
| GC-005 | Unset (the default) changes nothing: return shapes and claim behavior are identical, and workers run up to their summed `concurrency`. | `::test_unset_cap_is_unbounded` plus the existing integration suite green |
| GC-006 | A non-positive or non-integer `global_concurrency` raises `ValueError` at construction. | `tests/unit/test_worker_options.py::test_global_concurrency_validation` |
| GC-007 | The heartbeat record and `Queue.workers()` expose `global_concurrency`. | `tests/integration/test_workers.py::test_presence_reports_global_concurrency` |

## Out of scope

- A per-key limit (`concurrency_key`): its own spec, next.
- A worker event when capped. It needs a new script sentinel to tell "capped"
  from "empty"; the dashboard derives the condition from Redis instead.
- Changing the cap at runtime. A value stored in Redis that overrides the
  option can be added later without breaking this API.
- Suppressing the saturated-queue wake (threading the cap into `acquireNext`
  so it stops re-arming the marker at the cap). Only if a load test shows the
  bounce matters.
- The matador "waiting on cap" UI: a paired follow-up once GC-007 lands.

## Risks

- **The swap invariant.** Enforcement in one script is sound only while
  `acquireNext` stays the sole writer to `active` and the finish scripts keep
  removing before they fetch. The fuzzer check in GC-001 is the tripwire.
- **Mixed config.** Workers passing different caps each enforce their own.
  Documented the same way `rate_limit` is.
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

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | GC-006 | Add the option, validate it, store it (not yet enforced) | `toro/worker.py` | unit, red first |
| 2 | GC-001, GC-002, GC-005 | Write the cap tests (red for the right reason: the option exists but nothing enforces it), then the `LLEN` guard in `MOVE_TO_ACTIVE` and the ARGV at its one call site | `toro/scripts.py`, `toro/worker.py`, tests | high-water mark via a shared counter; direct script calls for GC-002 |
| 3 | GC-001 | Cap invariant in the seeded fuzzer | `tests/integration/test_invariants.py` | `active <= cap` after every step |
| 4 | GC-003 | Crash recovery test | tests only | zombie worker, expired locks, sweep; mutation check with the sweep off |
| 5 | GC-004 | Arm the marker on the two non-claiming release paths | `toro/scripts.py` | large `block_timeout`, assert prompt start, red first per path |
| 6 | GC-007 | Presence field and `workers()` | `toro/worker.py`, `toro/queue.py` | integration |
| 7 | | Docs: `processing.md`, `concepts.md`, README feature row | docs | review |
| 8 | | Prove: lint, types, full suite with zero skips, a saturated-cap load run to size the bounce, an end-to-end run of a capped example | | evidence captured |
