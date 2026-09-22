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
- **One helper places a flow's finished nodes: `placeSubtree(rootId, score)`.**
  It walks the subtree once and re-scores every finished descendant to
  `score + depth`, in whichever finished set holds it. It has exactly two callers.
  When a root settles (every terminal path already goes through
  `recordFinished`; a root is a job with `children` whose parent is not parked),
  the score is the root's finish time, so the root is always older than its
  descendants and reaches the trim first. When a failed root is retried and the
  flow is running again, the score is `LIVE + now`, which puts its finished
  children back out of the trim's reach. At most `MAX_FLOW_NODES` (1000) nodes,
  once per settle or retry.
- **Trimming a job with children trims its finished descendants** (the cascade
  `REMOVE_JOB` already has), counted against the script's trim budget. A
  descendant that is still running is left alone; it finishes as a job with no
  parent and is scored and trimmed like any other.
- **No new keys, no migration.** A fleet with old and new workers is safe in both
  directions: old workers score children at `now` and trim them by rank, as today.
- **What does not change.** A job is still in exactly one collection, the one its
  `state` names, which removal, counts and listing rely on. `counts()` and
  `get_jobs()` return what they return today. The alternative that keeps children
  out of the finished sets altogether is simpler to state, but it breaks that
  invariant for one class of jobs and changes what `counts()` means.
- **What changes for readers of the finished sets:** a flow child's score is a
  retention position, not its finish time. `finishedOn` in the job's hash is the
  finish time. The dashboard lists roots only and is unaffected; a raw
  `get_jobs("completed")` lists the children of running flows first.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| FR-001 | While a flow runs, its finished children survive any number of unrelated finishes under the default bound, hashes and logs included, and the tree stays whole. | `tests/integration/test_flow_retention.py::test_a_running_flow_keeps_its_finished_children` |
| FR-002 | Live children do not count against the bound: a running flow with 999 finished children does not push retained history out. | `::test_live_children_do_not_eat_the_bound` |
| FR-003 | When a root settles (completed, failed by a worker, failed eagerly by the script, or stalled out), its finished descendants are re-scored above it, at every depth. | `::test_settled_flow_rides_with_its_root` (parametrized by path) |
| FR-004 | Trimming a root deletes its finished subtree in the same script; nothing of the flow remains in either finished set, and the deletions count against the trim budget. | `::test_trimming_a_root_takes_its_subtree` |
| FR-005 | A finished flow is never shown partial: across a long run of flows past the bound, every root that still exists has all of its children. | `::test_no_retained_flow_is_partial` |
| FR-006 | A child that finishes after its parent was failed eagerly or removed is recorded and trimmed as a job with no parent; nothing leaks. | `::test_orphans_are_ordinary_jobs` |
| FR-008 | Retrying a failed root puts its finished children back out of the trim's reach until the flow settles again. | `::test_a_retried_flow_is_running_again` |
| FR-007 | The steady-state cost of a finish is unchanged for queues without flows, and within 5% for an all-flows workload. | measured, as in `bounded-retention.md` |

## Out of scope

- Counting the bound in flows instead of jobs.

## Risks

- **The hottest scripts change.** `recordFinished` runs on every finish. The
  guard is FR-007 plus the existing fuzzers.
- **A score that is not a timestamp.** Anything that reads scores from
  `completed` or `failed` as finish times would be wrong for flow children.
  In the repository that is nothing; the data-model doc has to say so.
- **A flow that never settles keeps its finished children forever.** That is the
  definition of a running flow, and `remove()` or `clean("waiting-children")`
  ends it.

## Decisions to confirm

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
| 2 | FR-003, FR-008 | `placeSubtree`, called when a root settles and when a failed root is retried | `toro/scripts.py` | one test per terminal path, and a retry |
| 3 | FR-004, FR-006 | Cascade in the trim; orphans | `toro/scripts.py` | integration |
| 4 | FR-005 | Long run of flows past the bound | tests only | property over the whole run |
| 5 | FR-007 | Throughput, flows and no flows | bench | before and after |
| 6 | | Docs: data model, flows, producing, upgrading | docs | review |
| 7 | | Prove: full suite, mutation audit, matador suite against the branch | | evidence captured |
