# Cancel a job, including one that is running

## Problem and outcome

A job can be removed but not stopped. `remove_job()` on an active job takes it out
of `active` and its worker's finish then commits nothing, but the processor runs to
completion: the work carries on with nowhere to report, and the caller waiting on
`result()` waits out its timeout. There is no way to say "stop this" to a job that
has already started, and no way to tell a job that was deliberately stopped from one
that failed.

Outcome: `cancel_job(job_id)` ends a job wherever it is. One that has not started is
ended at once, with no worker involved. One that is running is told to stop, and its
processor is cancelled where it awaits, so its `finally` blocks run. Either way the
job lands in a new terminal state, `cancelled`, and anyone waiting on `result()` is
told so rather than left waiting.

## Design

- **A terminal state of its own, `cancelled`.** Folding into `failed` would be less
  machinery, and is rejected: a cancellation is not a failure, and counting it as one
  corrupts the failure rate, the failure metric and the dashboard's failed tab, which
  are the operational signals people page on. `_state_zset` is now the one
  state-to-ZSET mapping, so the listings cost one line.
- **One terminal path, as ever.** Cancellation commits through `recordFinished`, so
  it inherits everything a finish already does: the concurrency key passes on,
  retention applies, a flow parent settles, the live index is maintained. No second
  way for a job to end.
- **No retry.** A cancelled job has been told not to run. `attempts` does not apply,
  and `retry_job()` on one returns False, as it does for a job that never failed.
- **Two deliveries, one meaning.** Promptness comes from a message on `<base>cancel`,
  a channel carrying nothing but cancellations: `events` carries one message per job,
  and a worker listening there parses the whole firehose to catch something rare.
  The terminal `cancelled` event still goes to `events`, where `result()` and the
  dashboards read it: the split is by audience. Correctness comes from `EXTEND_LOCK`, which every running job already
  calls every `lock_renew_time` and which already returns a signal the worker acts
  on. It gains a "cancel requested" answer. A dropped message therefore costs
  latency, never the cancellation: the backstop cannot be missed, because a worker
  that stops renewing loses the job to the stalled sweep anyway.
- **The processor runs in its own task.** Today it is awaited inline in the process
  loop, so there is nothing to cancel without killing the loop. Cancelling the task
  raises `CancelledError` where the processor awaits, which is Python's own
  cooperative cancellation: `finally` blocks and context managers run. Work that must
  not be interrupted belongs in that unwinding, not behind `asyncio.shield`, which
  does not hold a cancellation back (see `processing.md`). The worker decides the
  outcome by what it asked for, so a processor that swallows `CancelledError`, or a
  cleanup that raises, still ends the job `cancelled`.
- **A job that is not running needs no worker.** `wait`, `delayed`, `held` and
  `waiting-children` are cancelled inside the script that moves them, atomically.
  A held job leaves its key's queue; a holder hands the key on.
- **Flows cancel as a unit**, like removal: cancelling a parent cancels its subtree,
  and a cancellation cascades UPWARD as a cancellation. A `fail_parent` ancestor of a
  cancelled child is stopped, not failed: the stop was deliberate, so counting it as a
  failure is the one thing the separate state exists to prevent, and a flow is not a
  way back in. Under `continue` nothing changes: the parent still runs, and reads
  "cancelled" as the reason in its `failed_children()` record.
- **`result()` raises `JobCancelledError`**, a sibling of `JobFailedError`, rather
  than waiting out its timeout.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| CN-001 | `cancel_job()` on a job that has not started ends it at once, with no worker running: it is `cancelled`, in no other collection, and never runs. Covers `wait`, `delayed` and `held`. | `tests/integration/test_cancel.py::test_a_job_that_has_not_started_is_cancelled_at_once` (parametrized) |
| CN-002 | `cancel_job()` on an ACTIVE job stops its processor where it awaits, and the processor's `finally` runs. The job lands in `cancelled`. | `::test_cancelling_a_running_job_stops_its_processor` |
| CN-003 | The cancellation reaches the worker over `<base>cancel` within a fraction of the lock-renew interval, and reaches it through `EXTEND_LOCK` even when the message never arrives. | `::test_a_cancel_arrives_promptly`, `::test_a_cancel_with_no_message_still_lands` |
| CN-004 | A cancelled job is terminal: it does not retry however many `attempts` remain, `retry_job()` returns False, and no further attempt is recorded. | `::test_a_cancelled_job_does_not_retry` |
| CN-005 | `cancelled` is a state everywhere: `counts()`, `roots_counts()`, `get_jobs`, `get_jobs_roots`, `search`, `remove_job`, `clean`. Retention applies to it under `remove_on_fail`. | `::test_cancelled_is_a_state` |
| CN-006 | Cancelling frees what the job held: a concurrency-key holder hands the key on, a held job leaves its key's queue, and a cancelled child settles its parent by the parent's own `on_fail` policy, stopping a `fail_parent` ancestor rather than failing it. | `::test_cancelling_a_held_job_leaves_its_keys_queue`, `::test_cancelling_a_key_holder_hands_the_key_on`, `::test_a_cancelled_child_settles_its_parent_by_policy`, `::test_a_cancellation_cascades_upward_as_a_cancellation` |
| CN-007 | Cancelling a flow parent cancels its whole subtree, running children included. | `::test_cancelling_a_flow_takes_its_subtree` |
| CN-008 | `result()` on a cancelled job raises `JobCancelledError`, whether it was waiting when the cancel landed or asked afterwards. | `::test_result_reports_a_cancellation` |
| CN-009 | `cancel_job()` on a job that is already terminal, or absent, returns False and changes nothing. | `::test_cancelling_what_cannot_be_cancelled` |
| CN-013 | The cancel state machine survives fault injection on every crash path: a commit dropped mid-cancel, a request landing during the claim, and one landing after the job settled. | `tests/integration/test_fault_injection.py::test_dropped_cancel_commit_recovers_and_cancels_once`, `::test_a_cancellation_during_the_claim_is_not_lost`, `::test_a_cancellation_after_the_job_settled_changes_nothing` |
| CN-011 | Removing a RUNNING job stops its processor, so the slot it holds frees at once. The job is removed, not cancelled. | `::test_removing_a_running_job_stops_its_processor`, `tests/integration/test_global_concurrency.py::test_removing_a_running_job_frees_its_cap_slot_at_once` |
| CN-012 | A worker's cancellations reach the API beside its completions and failures. | `::test_a_worker_reports_its_cancellations` |
| CN-010 | A worker with no cancellations pays nothing measurable: the claim and finish paths are unchanged, and the subscription is one per worker, on a channel carrying nothing but cancellations. | measured. The claim this supports is that nothing is added to the hot path: a cancellation publishes only when one is asked for, and the lock renewal's extra read happens on a call that was already being made. The earlier throughput comparison here (9,650 against 9,340 jobs/s) is NOT evidence of that: alternating the order later showed whichever version ran second measured faster, which is the size of the whole difference. What the method could resolve: subscribing workers to the general `events` channel read 8,560, a 7% regression, because each worker parsed one message per job. Pinned by `::test_a_worker_does_not_listen_to_the_job_firehose` |

## Out of scope

- Cancelling work that is not awaiting (a processor in a tight CPU loop). asyncio
  cancellation lands at an await point; a processor that never yields cannot be
  interrupted, and the lock is what eventually reports it.
- A grace period or a two-phase "ask then kill". Cancellation is one signal.
- Cancelling a whole queue or a whole state. `clean()` already removes in bulk;
  bulk cancel can follow if asked for.

## Risks

- **A processor that swallows `CancelledError`** would otherwise commit a completion
  that stands: cancelling an active job only flags it, so its lock is still held and
  it is still in `active`. The worker therefore decides the outcome by what it asked
  for, not by how the processor unwound: a job it asked to stop ends `cancelled`
  whether the processor returned a value or raised from its cleanup. The work itself
  can still be running afterwards, which is why the processing page says not to.
- **A worker subscribes before its first claim**, so a job it is running is always
  one it can hear about. Subscribed afterwards, the claim path (which wakes on the
  marker) starts a processor before the subscribe round trip lands, and every
  cancellation of a just-started job waits out a lock renewal.
- **The worker gains a subscription**, so it gains a connection and a reconnect path.
  It follows the shape `Queue._ensure_dispatcher` already uses, confirmation of the
  subscribe included, and a stream that dies falls back to the lock backstop rather
  than losing cancellations.
- **`cancelled` is an eighth state**, so anything enumerating states sees one more.
  The same rolling-upgrade rule as `held`: upgrade workers before cancelling.

## Open questions

- Whether `cancel_job` should accept a reason string stored as `failedReason`'s
  sibling. Leaning yes, since "who cancelled this and why" is the first question
  asked of a cancelled job, but it can be added later without a breaking change.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | CN-005 | `cancelled` as a JobState: the key, the listings, counts | `toro/job.py`, `toro/keys.py`, `toro/queue.py`, `toro/scripts.py` | red first |
| 2 | CN-001, CN-006 | `CANCEL_JOB` for a job that has not started, through `recordFinished` | `toro/scripts.py`, `toro/queue.py` | integration, one test per state |
| 3 | CN-002, CN-003 | The processor in its own task; the events-channel signal and the `EXTEND_LOCK` backstop | `toro/worker.py`, `toro/scripts.py` | integration, red first |
| 4 | CN-004, CN-009 | Terminality: no retry, no double cancel | `toro/scripts.py` | integration |
| 5 | CN-007 | Flow subtree | `toro/scripts.py` | integration |
| 6 | CN-008 | `JobCancelledError` and `result()` | `toro/errors.py`, `toro/queue.py` | integration |
| 7 | CN-010 | Throughput with and without a subscription | bench | before and after |
| 8 | | Docs: consuming, concepts, data model, upgrading, producing | `docs/` | review |
| 9 | | Prove: full suite, mutation audit, adversarial review, matador suite | | evidence |
