# Flows

A flow is a parent job enqueued atomically with its children. The children run
first (in parallel, nested arbitrarily within the 1000-node cap); the parent
is parked in the `waiting-children` state and runs only once every child has
settled. One
primitive covers fan-out/fan-in ("fetch 10 shards, then merge") and chains
("build, then deploy").

> Why the API and the failure defaults look the way they do - including the
> failure modes other queues' dependency features taught us to avoid - is
> documented in [the design notes](flows-design.md).

## Enqueue a flow

```python
from toro import FlowChild as c

parent = await queue.add_flow(
    "report", {"period": "2026-06"},
    children=[
        c("fetch", {"shard": 1}),
        c("fetch", {"shard": 2}),
        c("summarize", {}, children=[c("fetch", {"shard": 3})]),  # nesting
    ],
    attempts=3,                # parent options: add()'s, minus job_id/deduplication
)
result = await parent.result()        # resolves when the WHOLE flow does
```

If the flow fails, `result()` raises `JobFailedError` naming the failing
child (`"child 7 failed: ..."`). The default `timeout` is 30s - size it for
the whole flow, not one job.

`add_flow` inserts the entire tree in one atomic script - either the whole
flow exists or none of it. Leaves go straight to `wait` (or `delayed`, if they
carry a `delay`); every node with children parks in `waiting-children`.

`FlowChild` takes the same options as `Queue.add()` (`priority` - clamped to
the same range as `add()` - `attempts`, `backoff`, `delay`, auto-removal),
plus `on_fail` (below). A few options are
deliberately not valid on flow nodes: custom `job_id`s and `deduplication`
(both invite id-reuse hazards inside trees), and `delay` on a node that has
children (it runs when its children settle, not on a clock).

## Reading children results

The parent pulls its children's results explicitly - there is no implicit
argument injection to reason backwards from:

```python
async def process(job):
    if job.name == "report":
        results = await job.children_results()   # {child_id: returnvalue}
        failures = await job.failed_children()    # {child_id: reason}
        return summarize(results, failures)
```

A child's result is copied into the parent at the moment the child completes,
so children are free to use `remove_on_complete` - the parent's copy survives,
and so does routine history cleanup (`clean("completed")`).

## When a child fails

Each child's `on_fail` says what its *terminal* failure (after its own
retries) does to the parent:

| `on_fail` | behavior |
|---|---|
| `"fail_parent"` (default) | The parent fails **immediately** - in the same atomic step, no worker needed - and the failure walks up through ancestors that also default. |
| `"continue"` | The failure is recorded; the parent still runs once every child has settled and inspects `failed_children()`. |

There is deliberately no "wait forever" option: a flow always settles. The
crash path counts too - a child whose worker died and that exhausts the
stalled-recovery limit settles its parent the same way.

Eager parent failure does **not** cancel siblings: in-flight and still-queued
children keep running, and their results are still collected into the parent
(useful if you later retry it). To actually stop the remaining work, remove
the parent - removal cascades the subtree.

## Retrying a failed flow

Retry is flow-aware:

- Retrying a failed **parent** re-arms its barrier: it goes back to
  `waiting-children` until its unsettled children resolve (it will not run on
  partial results).
- Retrying a failed **child** re-joins its parked parent's barrier and clears
  the stale entry from the parent's failure report.

So `retry_all_failed()` - or the dashboard's *retry all* - recovers an entire
failed flow in one shot, in any order. One pinned v1 edge: if the parent has
already failed and you retry *only* the child, the child's later success does
not resurrect the parent; retry the parent too (or use retry-all).

To recover *one* flow without touching the rest of the queue, use
`retry_flow(parent_id)`: it retries every failed job in that subtree, root
first, so a re-parked parent is ready before its children re-join the barrier -
order-independent, like retry-all but scoped. It returns the number of jobs
retried. The dashboard's *retry flow* button on a failed parent calls it.

## Removing flow jobs

- Removing a **parent** removes its whole subtree - children included, even
  ones currently running. This is how you cancel a flow. (A running child's
  processor coroutine is not interrupted; it finishes and its commit is then
  discarded by the lock-token guard.)
- Removing a pending **child** releases the parent if it was the last thing
  being waited on. A removed completed child does *not* take its
  already-collected result with it.
- `clean("waiting-children")` therefore cancels every parked flow outright.

All removal paths (manual, bulk, auto-removal) clean up the flow bookkeeping
keys with the job - nothing is left behind to leak.

## Introspection

```python
flow = await queue.get_flow(parent_id)        # {"job": Job, "children": [...]}
results = await queue.children_results(parent_id)
failures = await queue.failed_children(parent_id)
```

`get_flow` hydrates the tree breadth-first, one pipelined round trip per
level, down to `depth` levels (default 10; `depth=0` returns just the root
node) - pass a larger `depth` for deeper trees. `Job.parent_id` and
`Job.children_ids` expose flow membership on any loaded job.

For a dashboard detail view, `flow_view(parent_id)` folds those three reads
into one `FlowView`:

```python
view = await queue.flow_view(parent_id)
view.tree                       # same {"job": Job, "children": [...]} shape
view.results, view.failures     # collected results + tolerated failures
view.total, view.done, view.failed   # fan-in counts (completions only)
view.live                       # True while any node is still non-terminal
```

It costs the tree's O(depth) round trips plus one pipelined read of the
parent's result/failure hashes (not three separate calls), and `done`/`failed`
count completions only - a failed flow never reads as done. Returns `None` if
the job doesn't exist. [matador](https://github.com/ilovepixelart/matador)
renders all of this as a **flows** tab (one row per parked parent) and a tree
on the job detail with fan-in progress.

## Metrics and events

Enqueueing a flow increments the queue's `added` counter by the node count
and publishes a single `added` event carrying the root's id. On failures,
every terminally-failed child increments the per-job `failed` counter
(tolerated `continue` failures included), and each eagerly-failed ancestor
increments it again - so one leaf failure in a deep `fail_parent` chain
produces several `failed` increments and events. That counter is about *jobs*.

Whole flows are counted in their own right, one unit per **root** flow as it
settles (nested sub-flows don't double count):

```python
points = await queue.flow_metrics(minutes=60)   # per-minute completed/failed
pcts = await queue.flow_percentiles(minutes=60)  # end-to-end p50/p95/p99 (ms)
```

`flow_percentiles` measures the whole flow's wall clock - enqueue to the root
finishing - which the per-job duration never captures (a flow that fans out
wide finishes long after any single job's runtime). The dashboard charts this
as a throughput strip on the flows tab. Both reads zero-fill and share the
8h metrics retention.

## Limits

- One queue per flow: steps are job names on the same queue, dispatched by
  your processor. (Cross-queue flows are a possible future, not a v1 feature.)
- Trees, not DAGs: a child has exactly one parent.
- The shape is declared at enqueue time; processors can't append children to a
  running flow.
- At most 1000 nodes per flow (the atomic insert bounds how long Redis is
  held, same idea as the delayed-promotion batch).
