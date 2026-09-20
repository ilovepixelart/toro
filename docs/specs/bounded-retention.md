# Bounded retention by default

## Problem and outcome

`remove_on_complete` and `remove_on_fail` default to `None`, which means keep
forever. Every finished job leaves its hash and a member of `completed` or
`failed` behind, so a queue left on its defaults grows until Redis runs out of
memory. That is the classic production incident for a Redis queue, and nothing
in the defaults warns of it.

Outcome: a queue on its defaults holds a bounded number of finished jobs.
Keep-forever stays available, as an explicit choice.

## Design

- **Unset means bounded.** `None` (the option was not given) maps to a default
  bound: the newest **1000** completed jobs and the newest **5000** failed jobs.
  Failed jobs are kept in larger numbers because they are what gets debugged.
  The bound is a count, not an age: a count caps memory whatever the throughput,
  while an age keeps more the busier the queue is.
- **`False` is the escape hatch.** It already means keep forever, and keeps that
  meaning. `True`, an int, and `{"count", "age"}` are unchanged. Today `None` and
  `False` behave alike; this change is what separates them.
- **One source for the numbers.** `DEFAULT_KEEP_COMPLETED` and
  `DEFAULT_KEEP_FAILED` live in `toro/job.py`. `JobOptions.keep_args` takes the
  default that applies. Its Lua twin `keepArgsFromOpts`, used when a flow parent
  fails with no Python caller, gets the same number interpolated into the script
  text when it is built, so the two cannot drift.
- **The count trim becomes bounded per finish. This comes first.** The age trim
  deletes at most 1000 jobs per finish; the count trim deletes everything past
  the bound in one pass. Flipping the default on a queue holding millions of
  finished jobs would make the first finish after the upgrade delete them all
  in a single Redis-blocking script. The count trim gets the same 1000-per-finish
  limit, oldest first, and the backlog drains over the following finishes.
- **Flows are unaffected.** A child's result is copied into its parent's
  `:results` when it settles, so a trimmed child costs the parent nothing. This
  is already the documented behavior for an explicit `remove_on_complete`.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| BR-001 | With the option unset, `completed` holds at most the newest 1000 jobs and `failed` at most the newest 5000, and the trimmed jobs' hashes and aux keys are gone. | `tests/integration/test_finished_retention.py::test_unset_option_bounds_the_finished_sets` |
| BR-002 | `False` keeps every finished job, given per job and given through `default_job_options`. | `::test_false_keeps_everything` |
| BR-003 | `True`, an int, and `{"count", "age"}` map exactly as before. | `tests/unit/test_job_options.py` (the existing table, with the unset row changed) |
| BR-004 | The Lua twin agrees with `keep_args` for every option shape, unset included: a flow parent failed eagerly with `removeOnFail` unset is retained under the failed default. | `tests/integration/test_finished_retention.py::test_lua_twin_matches_python` |
| BR-005 | A count bound met by a deep backlog deletes at most 1000 jobs in one finish, oldest first, and the set reaches the bound over later finishes. | `::test_count_trim_is_bounded_per_finish` |
| BR-006 | A flow whose children were trimmed by the default still settles, and its parent still reads every child's result. | `tests/integration/test_flows.py::test_default_retention_keeps_children_results` |

## Out of scope

- Retention on the stall-failure path. `MOVE_STALLED` records a job that stalled
  out without a trim, as `docs/flows-design.md` documents. The next ordinary
  failure's trim covers the set.
- An age-based default.
- Per-job protection from another job's trim (see Risks).

## Risks

- **This deletes data on upgrade.** A queue that relied on the old default
  starts dropping finished jobs past the bound at its first finish after the
  upgrade. The migration note has to say so plainly and lead with the fix:
  `default_job_options={"remove_on_complete": False, "remove_on_fail": False}`.
- **Retention belongs to the set, not the job.** A trim is triggered by the job
  that is finishing, under that job's option, and trims the whole set. A job
  given `remove_on_complete=False` is not protected from a later job that
  finishes under a bound. That is how it works today with mixed options; the
  new default makes it common. Keeping everything means setting `False` for the
  queue, through `default_job_options` on every producer. The docs must say this.
- **`Queue.result()` on an old job.** A job trimmed from `completed` can no
  longer be read back. Awaiting a result while the job runs is unaffected.
- **The dashboard.** matador's completed and failed tabs show at most the
  retained jobs. Metrics are separate counters and are unaffected.
- **Silent skips.** Integration tests skip with no Redis on `localhost:6379`;
  every gate must show zero skips.

## Open questions

1. **The numbers.** Recommended: 1000 completed, 5000 failed.
2. **Where the migration note lives.** There is no changelog and GitHub releases
   carry no notes. Recommended: a new `docs/upgrading.md`, linked from the docs
   index and the README, with an entry per breaking release.
3. **Set-wide trimming.** Recommended: document it, as above. Alternative:
   change the semantics so a keep-forever job is exempt, which needs a second
   index and is a larger change than this one.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | BR-005 | Bound the count trim per finish, oldest first | `toro/scripts.py` | seed a deep backlog, one finish, count the deletions; red first |
| 2 | BR-003 | `keep_args` takes the applicable default; unset maps to it | `toro/job.py`, `toro/worker.py` | the unit table, red first |
| 3 | BR-001, BR-002 | End to end through a worker | tests | finish past both bounds; `False` per job and per queue |
| 4 | BR-004 | The Lua twin takes the same constant | `toro/scripts.py` | eager parent failure with the option unset; parity table |
| 5 | BR-006 | Flows under the default | tests only | children trimmed before the parent runs |
| 6 | | Docs: `producing.md`, README row, `docs/upgrading.md`, the docs index | docs | review |
| 7 | | Prove: full suite with zero skips, mutation audit, memory stays flat over a long run on defaults | | evidence captured |
