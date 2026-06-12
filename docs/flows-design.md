# Flows: design notes

> Status: SHIPPED. This documents the design decisions and the research behind
> them; the user guide is [flows.md](flows.md).

A flow is a parent job enqueued together with its children. Children run first
(in parallel, nesting allowed); the parent becomes runnable only when every
child has settled. This gives fan-out/fan-in ("fetch 10 parts, then summarize")
and chains ("A, then B") with one primitive.

## Why this design: lessons from the landscape

Every mature queue eventually grew a dependency story, and each one left a
public trail of what went wrong. Before building, we surveyed the dependency /
workflow features of the established queues across the Node.js, Python and
Postgres ecosystems - both how they model things and what their issue trackers
say users keep hitting. The design below is shaped by that trail.

What the survey says, distilled:

1. **Fan-in barriers die at the crash boundary** unless the bookkeeping is
   atomic with the child's settle. Counter- and callback-based barriers
   coordinated from the client side have produced a decade of "the callback
   never fired" bugs: a worker killed mid-job, a nested group counted wrong,
   a result expiring before the barrier read it. toro already funnels every
   settle through one Lua script - the barrier belongs inside it, crash path
   included.
2. **Implicit result injection ages badly.** Passing the previous job's return
   value as a magic argument (first or last position, depending on the
   framework) always grows an opt-out flag and confuses arity forever.
   Explicit pull by handle is the shape that aged well - provided the
   ergonomics are good; "fetch the dependency object and read `.result` off
   it" is the unergonomic version users complain about.
3. **"Wait forever" as a failure default is universally hated.** Two major
   queues default to leaving the dependent/parent parked indefinitely when a
   dependency fails terminally, silently; in both, it's the single most
   complained-about behavior of the feature. Failure must propagate by
   default.
4. **Graph state lives in the datastore, not the message.** Serializing the
   remaining workflow into message headers means bloat, no nesting, and no
   visibility (one framework's own redesign RFC concedes exactly this). Keys
   in the datastore are also the only way a dashboard can show a flow.
5. **Cleanup must be structural.** The most mature implementation leaks its
   children-results hashes (no TTL, surviving auto-removal - tens of
   thousands of stale keys reported in production) because aux keys are
   cleaned on a separate path from job removal. Cleanup has to ride the
   removal paths that already exist.
6. **Eager beats lazy on parent failure.** Failing the parent "lazily" -
   marking it and waiting for a worker on the parent's queue to actually
   transition it - surprises people whenever no such worker exists. The
   parent should fail in the same atomic step as the child.
7. **Failure policy frozen at enqueue time** (denormalized into the stored
   job) is a recurring complaint: teams want to change their minds after the
   flow exists. Storing the policy as a plain mutable field costs nothing.

## Design

### Scope and shape

- **Trees, not DAGs.** A child has exactly one parent. Multi-parent is rare,
  expensive, and even the most mature implementation declined it;
  fan-out/fan-in plus nesting covers the real use cases.
- **Single queue per flow (v1).** toro workers dispatch on `job.name` inside
  one processor, so flow steps are just different names on one queue. This
  removes the cross-queue hazard class outright (scripts reaching into other
  queues' keys; a purged children's queue stranding parents elsewhere).
  Cross-queue can be revisited later without breaking the model.
- **Static shape.** The tree is declared at enqueue time. Children spawned
  dynamically from inside a processor is the messiest corner of the systems
  that have it, and is out of scope.

### API

```python
from toro import FlowChild as c   # name, data, opts, children, on_fail

parent: Job = await queue.add_flow(name, data, children=[...], **opts)
```

- `add_flow` enqueues the whole tree **atomically** (one Lua call) and returns
  the parent `Job`. `await parent.result()` resolves when the flow does.
- Children accept the same options as `Queue.add()` (priority, attempts,
  backoff, delay, auto-removal), plus `on_fail` (below). Scheduler-style
  options, custom ids and deduplication are not valid on flow nodes.
- Inside the parent's processor, results are **pulled explicitly**:

```python
async def process(job):
    if job.name == "report":
        results = await job.children_results()   # {child_id: returnvalue}
        failures = await job.failed_children()   # {child_id: failed_reason}
```

- Introspection: `await queue.get_flow(parent_id)` returns the tree
  (`{job, children: [...]}`), for the dashboard and for users.

### Failure semantics: two policies, no third

Per-child `on_fail`, stored as a plain mutable field on the child's hash (so
tooling *can* change it after enqueue - see lesson 7):

| `on_fail`               | when this child terminally fails                       |
| ----------------------- | ------------------------------------------------------ |
| `"fail_parent"` (default) | the parent fails **immediately and eagerly**, in the same Lua script, recursively up ancestors that also default; no worker on the parent required |
| `"continue"`            | the failure is recorded in the parent's failures hash, the dependency cleared; the parent runs once all children settle and inspects `failed_children()` |

There is deliberately **no "wait indefinitely" option** (lesson 3). Retries
still happen first: "terminally fails" means after the child's own `attempts`
are exhausted (or it stalls past `max_stalled_count`). A child retried to
success *after* its parent already failed does not resurrect the parent
(documented, v1); retrying the parent re-arms its barrier instead.

### Data model

New job state `waiting-children` (a sixth `JobState`), backed by a per-queue
ZSET (timestamp-scored, like `completed`/`failed`), surfaced in `counts()`,
`get_jobs()`, `clean()` and the dashboard.

Per parent job, three aux keys (joining `:lock` / `:logs`):

| key             | type | content                                            |
| --------------- | ---- | -------------------------------------------------- |
| `{id}:deps`     | SET  | ids of children not yet settled (the barrier)      |
| `{id}:results`  | HASH | child id → returnvalue JSON (completed children)   |
| `{id}:cfail`    | HASH | child id → failed reason (`on_fail="continue"` children) |

Plus two hash fields: `parentId` on every child; `children` (static JSON id
list) on every parent - the deps set shrinks, the dashboard tree needs the
full picture.

A SET rather than a counter: it's idempotent under re-delivery, inspectable
("which children is this parent still waiting on?"), and the empty-check
(`SCARD == 0`) is the release condition. The counter-corruption bug class
(lesson 1) is the argument against counters.

### Mechanics

- **`ADD_FLOW`** (new script): tree as JSON in ARGV, `cjson.decode`, walk
  depth-first; leaves enqueue into `prioritized` (or `delayed`), interior
  nodes land in `waiting-children` with their `:deps` set populated. Capped
  (~1000 nodes per flow) to bound script time, like `PROMOTE_BATCH`. One
  metrics increment and one announce for the whole tree.
- **Release** lives inside the existing finish scripts, the extension point
  `_LIB` reserved ("to add markers-with-delay or grouping later, we change
  only these functions"):
  - `MOVE_TO_COMPLETED` of a child: `HSET parent:results`, `SREM parent:deps`;
    on empty, move the parent from `waiting-children` through the shared
    `enqueue()` at its stored priority.
  - `MOVE_TO_FAILED` terminal branch: apply `on_fail` - either record into
    `:cfail` + `SREM` (+ release if last), or fail the parent now through
    `recordFinished` (so `remove_on_fail` retention applies), publish the
    event, and **loop upward** while the ancestor itself has a parent with
    `fail_parent`.
  - **The stalled path gets identical bookkeeping**: `MOVE_STALLED`'s
    fail-branch runs the same parent logic (lesson 1: the crash path is where
    barriers historically break).
- **Cleanup is structural** (lesson 5): `delJobs` and `REMOVE_JOB` know the
  three aux keys, so every existing removal path (auto-removal
  keepCount/keepAge, manual remove, `clean()`) deletes them for free.
  Removing a parent removes its subtree; removing a child SREMs it from its
  parent's deps (and releases the parent if it was the last). Settle writes
  are guarded by a parent-exists check so a retention-trimmed parent can't
  get orphan keys recreated by late siblings.
- **Retry is flow-aware**: a failed parent with unsettled deps re-parks in
  `waiting-children`; a retried child re-joins a parked parent's barrier and
  clears its stale `:cfail` entry. `retry_all_failed()` therefore recovers a
  whole flow in any order.

### Edge cases pinned down (each has a test)

- **Parent retries**: a released parent is a normal job; its own
  `attempts`/`backoff` apply. Children are not re-run on parent retry -
  results are already in `:results`.
- **`remove_on_complete` on children**: allowed - the result is copied into
  the parent's `:results` at completion, so the child hash is free to go.
  Routine `clean("completed")` likewise never touches a pending parent's
  collected results.
- **Empty `children=[]`**: rejected (`ValueError`) - that's `add()`.
- **Id rules**: flow nodes get server-side ids; custom ids on flow nodes are
  not supported in v1 (id reuse inside trees is a documented zombie-job
  hazard elsewhere).
- **Progress display counts completions only** - a failed flow must never
  read as 100% done.

## Out of scope (v1)

Cross-queue flows, DAGs/multi-parent, dynamic children from inside a
processor, and child-retry-resurrects-failed-parent.
