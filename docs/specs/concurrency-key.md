# Serialize jobs that share a key

## Problem and outcome

Jobs that touch the same thing must not run at once: one order's payment steps,
one tenant's sync, one document's edits. Today that needs a queue per thing or a
lock inside the processor, and a lock inside the processor still burns a worker
slot while it waits. Every real ask for "groups" found in the survey behind the
roadmap was this: run at most one job per key at a time, and let other keys flow.

Outcome: `add(..., concurrency_key="order-42")` runs at most one job per key at a
time, in the order they were added, without holding a worker slot or a place in
the queue while they wait. Other keys are unaffected, and so is the claim path.

## Design

- **A key is held from enqueue to terminal.** The first job added under a free
  key takes it (`<base>ck:<key>` = its id, `SET NX`) and is enqueued as any job.
  A job added under a taken key is **held**: it sits in `<base>held:<key>`, a
  ZSET scored by the priority score it would have had in `prioritized`, and in
  `<base>held`, the ZSET of every held id, with `state` `held`. It is in no other
  collection. When the holder reaches a terminal state, is removed, or is trimmed
  at once, the key passes to the lowest-scored held job: it moves to
  `prioritized` at its original score, so its place among jobs added at the same
  time is kept, and the marker is armed. With nothing held the key's marker is
  deleted. Both per-key structures vanish when empty: nothing is left per key.
- **Holding spans delays and retries.** A delayed holder holds its key while it
  waits; a retrying holder holds it through its backoff. This is strict order,
  which is what a key asks for, and it keeps every hot path untouched: the claim
  path, promotion of delayed jobs and the stalled sweep never look at keys.
- **One routine takes a key: `acquireKey(base, jobId, key, score)`**, called at
  every point a job enters the pipeline: `ADD_JOB`, each leaf of `ADD_FLOW`,
  `releaseParent` (a parent that becomes runnable), `RETRY_JOB` (a failed job
  re-entering) and `ADD_SCHEDULED` (an occurrence). It returns whether the job
  may be enqueued now. **One routine releases it: `releaseKey(base, jobId)`**,
  called from `recordFinished` (every terminal path: a worker's finish, an eager
  parent failure, a stall-out, a remove-at-once) and from `REMOVE_JOB`'s
  `removeTree` for a holder removed before it finished. A held job that is
  removed leaves both held sets and holds nothing.
- **`held` is a job state.** `JobState` gains `held`; `counts()` reports it;
  `get_jobs("held")` lists held jobs by their score; `remove_job` works on them;
  `retry`, `promote` and `clean` treat them as what they are. The dashboard shows
  the count beside the waiting tab and a held job's key in its row (matador,
  separate change).
- **Keys are key segments.** The same rule as scheduler and dedup ids: a
  non-empty string with no `:` and no control characters, so two keys cannot
  collide into one Redis key.
- **Flows.** A flow node may carry a key; a parent takes its key when it becomes
  runnable, a leaf at enqueue. A held child is not settled, so its parent waits
  for it as for any child.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| CK-001 | Two jobs added under one key: the second is `held`, in no other collection, and runs only after the first reaches a terminal state, whatever the worker concurrency. Jobs under other keys and jobs with no key run meanwhile. | `tests/integration/test_concurrency_key.py::test_one_job_per_key_at_a_time` |
| CK-002 | Held jobs run in the order they were added, and a higher-priority job added under the key later runs before lower-priority ones still held, at its original score. | `::test_held_jobs_keep_their_order_and_priority` |
| CK-003 | The key passes on every terminal path of the holder: completion, terminal failure, eager parent failure, stall-out, removal, remove-at-once retention. A retry with attempts left keeps the key. | `::test_the_key_passes_on_every_terminal_path` (parametrized) |
| CK-004 | Nothing is left per key: after the last job under a key settles, no key of the queue names it. | `::test_a_key_leaves_nothing_behind` |
| CK-005 | A delayed holder holds its key; a scheduled occurrence and a retried job take or wait for the key like an added job. | `::test_holding_spans_delays_schedules_and_retries` |
| CK-006 | Flow nodes: a held leaf keeps its parent parked; a parent takes its key when released; a flow's keys pass on eager failure. | `::test_flow_nodes_hold_keys` |
| CK-007 | A key is validated as a key segment; the option round-trips through `JobOptions` and is visible on the job. | `tests/unit/test_job_options.py` |
| CK-008 | A held job is a `held` job everywhere: `counts()`, `get_jobs("held")`, `remove_job`, `clean("held")`; `retry_job` and `promote_job` on it are no-ops that return False. | `::test_held_is_a_state` |
| CK-009 | The claim path and the finish scripts are unchanged for jobs with no key: throughput within noise of main. | measured, as in `flow-retention.md` |

## Out of scope

- A per-key limit above one. The structures allow it (the holder marker becomes
  a set with a cardinality check); every ask found was for one.
- Fairness across keys (round-robin between tenants). Keys serialize; they do
  not schedule.
- Cancelling a held job other than by removal (0.8 cancel covers it).

## Risks

- **A stuck holder blocks its key for good.** A holder that is stalled past the
  limit is failed by the sweep and releases; a holder removed releases. A holder
  parked forever (a flow parent whose child never settles) blocks the key as it
  blocks its flow. `remove_job` is the way out, as for the flow.
- **A crash between `SET NX` and the enqueue cannot happen**: both are in one
  script.
- **Mixed fleets.** An old worker never releases a key; `releaseKey` runs in the
  finish scripts the WORKER registers. Keys must be introduced only once every
  worker runs this version. The upgrading page says so.
- **Held jobs are invisible to `prioritized`**, so `latency()` (age of the next
  waiting job) does not see them. A held job waits on its key, not on workers;
  documented.

## Decisions

1. Hold from enqueue, not from claim: the claim path stays untouched and the
   semantics are strict order, which is what a key means.
2. Limit fixed at one in this release.
3. A new visible state, `held`, rather than folding into `wait`: a job that is
   waiting on a key, not on a worker, must be distinguishable in a dashboard.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | CK-007 | The option: validation, `JobOptions`, the job's view | `toro/job.py`, `toro/queue.py` | unit, red first |
| 2 | CK-001, CK-002 | `acquireKey` in `ADD_JOB`; `releaseKey` in `recordFinished`; `held` in `counts` | `toro/scripts.py`, `toro/queue.py`, `toro/keys.py` | integration, red first |
| 3 | CK-003, CK-004 | Every terminal path releases; removal of a holder and of a held job | `toro/scripts.py` | one test per path |
| 4 | CK-005, CK-006 | Delays, schedules, retries, flow nodes | `toro/scripts.py` | integration |
| 5 | CK-008 | `held` as a state in every admin path | `toro/queue.py` | integration |
| 6 | CK-009 | Throughput, no keys and all keys | bench | before and after |
| 7 | | Docs: producing (a section), concepts (the states), data model, upgrading | docs | review |
| 8 | | Prove: full suite, mutation audit, adversarial review, matador suite | | evidence |
