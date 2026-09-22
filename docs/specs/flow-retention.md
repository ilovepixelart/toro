# A flow is retained as one unit

## Problem and outcome

Retention trims the finished sets by rank, oldest first, and a flow's children are
entries like any other. Children finish before their parent, so they reach the
trim first: on a queue doing more than 1000 completions in a flow's lifetime, a
finished child's hash, logs and node in the tree are deleted while the flow is
still running, and every finished flow near the retention edge shows a partial
tree. The parent's collected results and the progress counts are safe
(`docs/specs/bounded-retention.md`, BR-006); the tree and the children's own
records are not. Before the bounded default this needed an explicit
`remove_on_complete`; now it is what a busy queue does out of the box.

Manual removal already treats a flow as a unit: `REMOVE_JOB` and `clean()` delete
a parent with its subtree. Automatic removal is the odd one out.

Outcome: a finished child is kept exactly as long as its flow's root. While the
flow runs, nothing of it is trimmed; when the root is trimmed, the subtree goes
with it.

## Design

- **A finished child of a running flow sits outside the trim's reach.** A job that
  finishes while it has a parent that has not settled is scored `LIVE + now`
  in its finished set, where `LIVE` is 2^52: above every real timestamp, below
  2^53 so scores stay exact. The count and age trims look only below `LIVE`
  (`ZCOUNT -inf LIVE` replaces `ZCARD`, same cost class), so live children are
  neither counted against the bound nor candidates for it. The trim path gains
  no per-candidate work.
- **What keeps a finished job is its root, not its parent.** A parent can settle
  mid-flow while the root carries on, and its children finishing afterwards still
  belong to a running flow.
- **Every live node is indexed under its root.** A finished child scored live
  is added to `<root>:live` (every flow node stores its `rootId` at enqueue).
  When the root settles (every terminal path goes through `recordFinished`) or
  is removed at once by its own option, `settleLive` drains the index and
  re-scores each entry one above the root's finish time, so the root is the
  oldest of its flow and reaches the trim first. The index, not a walk over
  hashes, is what places them: a mid-level parent that disappears in between
  (trimmed by an older worker, removed at once) cannot strand its leaves, and
  deleting a job drains its index, so removing a flow takes its live jobs too. When
  a failed root is retried, `reviveSubtree` walks its children and scores every
  finished descendant `LIVE + now`, indexed again. An orphan (a child finishing
  after its root settled) is scored one above its own time, so a tie in the
  same millisecond cannot rank it before its root.
- **Trimming a job with children trims its finished descendants** (the cascade
  `REMOVE_JOB` already has), counted against the script's trim budget. A
  descendant that is still running is left alone; it finishes as a job with no
  parent and is scored and trimmed like any other. A descendant whose own
  option keeps everything stays.
- **One aux key per running flow, no migration.** `<root>:live` exists while a
  flow has live finished nodes and is deleted with the root. A flow enqueued
  before `rootId` was stored finds its root by walking up. A fleet with old and
  new workers is safe from loss: an old worker's rank trim can still take a
  running flow's children, as before the change, and counts live entries
  against its own bound; nothing is stranded. Keys are introduced once every
  worker runs this version.
- **What does not change.** A job is still in exactly one collection, the one its
  `state` names, which removal, counts and listing rely on. `counts()` and
  `get_jobs()` return what they return today. The alternative that keeps children
  out of the finished sets altogether is simpler to state, but it breaks that
  invariant for one class of jobs and changes what `counts()` means.
- **What changes for readers of the finished sets:** a flow child's score is a
  retention position, not its finish time. `finishedOn` in the job's hash is the
  finish time. Newest-first readers (`get_jobs`, `search`, `retry_all_failed`)
  page settled jobs before a running flow's children; `clean` removes settled
  history only; `counts()` is unchanged. The dashboard lists roots only.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| FR-001 | While a flow runs, its finished children survive any number of unrelated finishes under the default bound, hashes and logs included, and the tree stays whole. | `tests/integration/test_flow_retention.py::test_a_running_flow_keeps_its_finished_children` |
| FR-002 | Live children do not count against the bound: a running flow with 999 finished children does not push retained history out. | `::test_live_children_do_not_eat_the_bound` |
| FR-003 | When a root settles (completed, failed by a worker, failed eagerly by the script, or stalled out) or is removed at once by its own option, its finished descendants are re-scored one above it, at every depth, even when a mid-level parent between them is gone. | `::test_settled_flow_rides_with_its_root` (parametrized by path), `::test_a_root_the_sweep_fails_places_its_subtree`, `::test_a_root_removed_at_once_leaves_settled_children`, `::test_a_mid_gone_while_its_leaves_are_live_does_not_strand_them` |
| FR-004 | Trimming a root deletes its finished subtree in the same script; nothing of the flow remains in either finished set, and the deletions count against the trim budget. | `::test_trimming_a_root_takes_its_subtree` |
| FR-005 | Within its root's finished set a retained flow is never partial: across a long run of flows of uneven size past the bound, every root still kept has all of its children, and no child outlives its root. | `::test_no_retained_flow_is_partial` |
| FR-006 | A child that finishes after its parent was failed eagerly or trimmed is recorded and trimmed as a job with no parent, scored one above its root's time; a sibling still running or delayed when its root is trimmed is left alone, and the finish that trimmed the root lands as any finish. | `::test_orphans_are_ordinary_jobs`, `::test_a_sibling_outliving_a_trimmed_root_is_left_alone`, `::test_an_orphan_is_never_older_than_its_root` |
| FR-008 | Retrying a failed root puts its finished children back out of the trim's reach until the flow settles again. | `::test_a_retried_flow_is_running_again` |
| FR-009 | The cascade leaves a descendant whose own option keeps everything; the live boundary the trims send to Redis is the exact integer; newest-first readers page settled jobs first and `clean` removes settled history only. | `::test_the_cascade_keeps_what_is_kept_forever`, `::test_the_live_boundary_is_exact`, `::test_readers_of_the_finished_sets_see_settled_jobs` |
| FR-007 | The steady-state cost of a finish is unchanged for queues without flows, and within 5% for an all-flows workload. | measured against main, median of 5 interleaved runs of 20,000 jobs at concurrency 20: plain 9,594 against 9,608 jobs/s, flows of four 9,131 against 9,195 |

## Out of scope

- Counting the bound in flows instead of jobs.

## Risks

- **The hottest scripts change.** `recordFinished` runs on every finish. The
  guard is FR-007 plus the existing fuzzers.
- **A score that is not a timestamp.** Anything that reads scores from
  `completed` or `failed` as finish times would be wrong for flow children.
  The newest-first readers in `queue.py` were: they page settled jobs first now.
  The data-model doc says so.
- **The unit rule stops at the root's set.** A flow's jobs in the other finished
  set follow that set's bound; a flow larger than the bound goes at its root's
  own finish. Both are documented in `flows.md`.
- **A flow that never settles keeps its finished children forever.** That is the
  definition of a running flow, and `remove()` or `clean("waiting-children")`
  ends it.

## Decisions

1. **This design** (live score, settle walk, cascade) over the two alternatives:
   a second chance at the trim (re-score protected children when the trim meets
   them: per-candidate reads on the hot path, unbounded work while large flows
   run), or a roots-only index per finished set (clean semantics, but two new
   keys, a backfill on upgrade, and drift in a fleet with old workers).
2. **Sequencing.** Its own PR on top of bounded retention; 0.7.0 is tagged only
   after both are in.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | FR-001, FR-002 | Live score for a child whose parent has not settled; trims look below `LIVE` | `toro/scripts.py` | integration, red first |
| 2 | FR-003, FR-008 | `settleLive` when a root settles or is removed at once; `reviveSubtree` when a failed root is retried | `toro/scripts.py` | one test per terminal path, and a retry |
| 3 | FR-004, FR-006 | Cascade in the trim; orphans | `toro/scripts.py` | integration |
| 4 | FR-005 | Long run of flows past the bound | tests only | property over the whole run |
| 5 | FR-007 | Throughput, flows and no flows | bench | before and after |
| 6 | | Docs: data model, flows, producing, upgrading | docs | review |
| 7 | | Prove: full suite, mutation audit, matador suite against the branch | | evidence captured |
