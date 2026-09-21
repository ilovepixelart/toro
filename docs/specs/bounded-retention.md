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
- **One place reads the option.** `keepFor`, in the shared Lua, maps a job's
  stored option to what its finished set keeps, and `recordFinished` calls it for
  every job it records. It is in Lua because two of the ways a job finishes have
  no worker behind them (a parent failed with its child, a job the stalled sweep
  gives up on); a second copy in Python for the worker's own finishes would be a
  copy to keep in step. The defaults, `DEFAULT_KEEP_COMPLETED` and
  `DEFAULT_KEEP_FAILED`, live in `toro/job.py` and are interpolated into the
  script text. No caller passes retention, so none can pass the wrong one or
  skip it.
- **The count trim becomes bounded per finish. This comes first.** The age trim
  deletes at most 1000 jobs per finish; the count trim deletes everything past
  the bound in one pass. Flipping the default on a queue holding millions of
  finished jobs would make the first finish after the upgrade delete them all
  in a single Redis-blocking script. The limit is one budget of 1000 for the
  whole script, oldest first: one script can record many jobs (a failing flow
  child fails its ancestors with it) and one job can carry both bounds. The
  backlog drains over the following finishes.
- **Schedulers carry the queue's defaults.** Workers mint every occurrence from
  the scheduler's stored template and never see a producer's
  `default_job_options`, so `add_scheduler` merges them into the template. A
  manual trigger leaves retention the template stores unset to the queue.
  Without this the keep-everything setting would not cover scheduled jobs.
- **A flow's results and progress survive a trimmed child.** A child's result is
  copied into its parent's `:results` when it settles, so a trimmed child costs
  the parent nothing, and `FlowView` counts `done` and `failed` from the parent's
  copies as well as from the tree. The child's node does leave the tree.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| BR-001 | With the option unset, `completed` holds at most the newest 1000 jobs and `failed` at most the newest 5000, and the trimmed jobs' hashes and aux keys are gone. | `tests/integration/test_finished_retention.py::test_unset_option_bounds_the_finished_sets` |
| BR-002 | `False` keeps every finished job, given per job and given through `default_job_options`. | `::test_false_keeps_everything` |
| BR-003 | `False`, `True`, a count and `{"count", "age"}` map exactly as before, for both finished sets; unset, null and unreadable opts map to the set's default; the other set's option never leaks in. | `tests/integration/test_finished_retention.py::test_the_one_place_a_remove_option_is_read`, `::test_unreadable_opts_count_as_unset` |
| BR-004 | Every way a job finishes applies that job's own option through the same routine: a flow parent failed eagerly with `removeOnFail` unset is retained under the failed default. | `tests/integration/test_finished_retention.py::test_eagerly_failed_parent_is_kept_under_the_default` (with BR-001 for a worker's finish and BR-008 for the stalled sweep) |
| BR-005 | A count bound met by a deep backlog deletes at most 1000 jobs in one finish, oldest first, and the set reaches the bound over later finishes. The 1000 bounds the script: both bounds on one job, or a chain of ancestors failed with a leaf, still delete at most 1000. | `::test_count_trim_is_bounded_per_finish`, `::test_count_and_age_trims_share_one_budget`, `::test_failing_a_chain_of_ancestors_shares_one_budget` |
| BR-006 | A flow whose children were trimmed by the default still settles, its parent still reads every child's result, and its progress counts do not go backwards. | `tests/integration/test_flows.py::test_default_retention_keeps_children_results`, `::test_flow_view_still_counts_children_the_default_trimmed` |
| BR-007 | A scheduler's stored template carries the queue's `default_job_options` under its own options, so jobs a worker mints honor a queue that keeps everything. A manual trigger of a template that stores retention unset takes the queue's default. | `tests/integration/test_scheduler.py::test_scheduler_template_carries_the_queue_defaults`, `::test_scheduler_options_win_over_the_queue_defaults`, `tests/integration/test_finished_retention.py::test_scheduled_jobs_honor_a_queue_that_keeps_everything`, `tests/integration/test_admin.py::test_trigger_scheduler_leaves_unset_retention_to_the_queue` |
| BR-008 | A job the stalled sweep fails for good is recorded under its own `remove_on_fail`, the default included: a queue whose only failures are crashed workers stays within the failed bound. | `tests/integration/test_finished_retention.py::test_a_crash_loop_cannot_outgrow_the_failed_bound`, `::test_a_stalled_out_job_honors_its_remove_on_fail` |

## Out of scope

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
- **A custom `job_id` frees up sooner.** Adding an existing id is a no-op only
  while the job's hash exists. On the old default that was forever; now it is
  until 1000 newer jobs complete. The migration note says so.
- **A trimmed child leaves its flow's tree.** On a busy queue that can happen
  while the flow is still running. Results and counts are unaffected (BR-006);
  retaining a flow as one unit would need children kept out of the rank-based
  trim, which is a data-model change and not part of this one.
- **`Queue.result()` on an old job.** A job trimmed from `completed` can no
  longer be read back. Awaiting a result while the job runs is unaffected.
- **The dashboard.** matador's completed and failed tabs show at most the
  retained jobs. Metrics are separate counters and are unaffected.
- **Silent skips.** Integration tests skip with no Redis on `localhost:6379`;
  every gate must show zero skips.

## Decisions

1. **The numbers.** 1000 completed, 5000 failed.
2. **Where the migration note lives.** `docs/upgrading.md`, linked from the docs
   index and the README, with an entry per breaking release.
3. **Set-wide trimming.** Documented, not changed. Exempting a keep-forever job
   needs a second index and is a larger change than this one.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | BR-005 | Bound the count trim per finish, oldest first | `toro/scripts.py` | seed a deep backlog, one finish, count the deletions; red first |
| 2 | BR-003 | `keepFor` reads the option for either set; the worker stops passing retention | `toro/scripts.py`, `toro/worker.py`, `toro/job.py` | the option table run inside Redis, red first |
| 3 | BR-001, BR-002 | End to end through a worker | tests | finish past both bounds; `False` per job and per queue |
| 4 | BR-004 | `recordFinished` calls `keepFor` itself; the finish scripts' ARGV is built in one place | `toro/scripts.py` | eager parent failure with the option unset |
| 5 | BR-006 | Flows under the default | tests only | children trimmed before the parent runs |
| 6 | | Docs: `producing.md`, README row, `docs/upgrading.md`, the docs index | docs | review |
| 7 | | Prove: full suite with zero skips, mutation audit, memory stays flat over a long run on defaults | | evidence captured |
