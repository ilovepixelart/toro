# Producing jobs

Everything the producer side can do: `Queue.add()` and its options, waiting for
results, inspecting the queue, and the admin operations.

## add()

```python
queue = Queue("emails")
job = await queue.add("welcome", {"user_id": 42})
```

`add(name, data=None, *, job_id=None, deduplication=None, **options)` writes the
job hash and enqueues (or delays) it in one atomic script - the `added` event is
published from inside that script, so an enqueue is a single round trip and the
event can't be lost between the two.

`name` is a free-form label for your processor to dispatch on; `data` is any
JSON-serializable payload.

## Options

| Option | Default | Meaning |
|---|---|---|
| `priority` | 0 | Higher = more urgent, one global order (0 to 2^20; clamped). Default 0 is the least-urgent band, FIFO among itself. |
| `delay` | 0 | ms before the job becomes runnable; it sits in `delayed` until due. |
| `attempts` | 1 | Total tries before the job is terminally failed. |
| `backoff` | `None` | Delay before each retry: an int (fixed ms) or `{"type": "fixed"\|"exponential", "delay": ms}`. Exponential doubles per attempt. |
| `remove_on_complete` | unset | Which successes to keep: unset keeps the newest 1000, `False` keeps all, `True` removes at once, `N` keeps the newest N, `{"count": N, "age": seconds}` bounds both. |
| `remove_on_fail` | unset | Same, for terminal failures; unset keeps the newest 5000. |
| `concurrency_key` | `None` | Jobs sharing a key run one at a time, in the order they were added. See [Serializing on a key](#serializing-on-a-key). |

Per-queue defaults go on the constructor and merge under per-call options:

```python
queue = Queue("emails", default_job_options={"remove_on_complete": 100, "attempts": 3})
```

## Retention

A queue on its defaults holds a bounded history: the newest 1000 completed jobs
and the newest 5000 failed ones (failures are what gets debugged). The bound is a
count, so it caps memory whatever the throughput. The numbers are
`DEFAULT_KEEP_COMPLETED` and `DEFAULT_KEEP_FAILED` in `toro/job.py`.

To keep every finished job, say so for the queue, on every producer:

```python
queue = Queue(
    "emails",
    default_job_options={"remove_on_complete": False, "remove_on_fail": False},
)
```

Retention is enforced inside the finish script itself - there is no separate
cleanup process to run or forget. It follows that:

- **Retention belongs to the set, not to the job.** A trim runs when a job
  finishes, under *that job's* option, and trims the whole `completed` (or
  `failed`) set. A job added with `remove_on_complete=False` is not protected
  from a later job that finishes under a bound, which is why keep-everything is
  a per-queue setting above.
- **The worker applies it.** The option is stored with the job and read as the job
  finishes, so the default in force is that of the worker's toro version.
- **A flow is one unit.** While a flow runs, nothing of it is trimmed: its
  finished children sit outside every bound until the root settles. Then the
  whole flow is as old as its root, so a trim reaches the root first and takes
  the subtree with it. Within its root's finished set a retained flow is never
  partial; the exceptions are listed under [Flows](flows.md).
- **A trim is bounded.** One finish deletes at most 1000 jobs, oldest first,
  whichever bounds apply and however many flow ancestors fail with it, plus the
  rest of a flow it started to remove. A bound that meets a deep backlog drains
  it over the following finishes instead of blocking Redis in one pass.
- **A scheduler stores its options when it is registered**, the queue's defaults
  included, because workers mint each occurrence from the stored template. Call
  `add_scheduler()` again to change them.

A trimmed job is gone: its hash, logs and flow bookkeeping are deleted,
`get_job()` returns `None`, and `result()` called after the trim times out. A
trimmed flow root takes its finished subtree with it.
Awaiting `result()` while the job runs is unaffected, and so are the metrics,
which are separate counters. Retention covers every way a job can finish: a
worker's finish, a flow parent failed by the script, and a job the stalled sweep
fails after its worker died.

## Serializing on a key

Work that touches the same thing must not run at once: one order's payment steps,
one tenant's sync, one document's edits.

```python
await queue.add("charge", {"order": 42}, concurrency_key="order-42")
await queue.add("invoice", {"order": 42}, concurrency_key="order-42")  # runs after
```

At most one job per key is claimed at a time, however many workers are running.
At-least-once still applies: a job whose worker stops reporting is recovered by the
stalled sweep and runs again, beside a processor that may still be going, so a key
orders work rather than making a second run impossible. A job
added under a taken key is **held**: it sits in no other collection, holds no
worker slot and no place in the queue, and takes the key when the holder reaches
a terminal state. Held jobs run in the order they were added, and a more urgent
one added later goes first, at the position it would have had in the queue.

- **The key is held from enqueue to a terminal state**, so a delayed job holds it
  while it waits and a retrying job holds it through its backoff. That is what
  "in order" means for a key; a job released with time still on its delay goes
  back to `delayed` and waits that out.
- **Every way of finishing hands the key on**: a completion, a terminal failure,
  a parent failed by a child, a job the stalled sweep gives up on, one removed at
  once by its retention, and one removed with `remove_job()`.
- **Flow nodes may carry a key.** A leaf takes it at enqueue; a parent takes it
  when its children settle and it becomes runnable. A held child has not settled,
  so its flow waits for it as for any child.
- **Nothing is left per key**: the bookkeeping exists only while jobs are using it.
- A key is a Redis key segment, so it must be a non-empty string with no `:` or
  control characters, like a scheduler or deduplication id.

A held job is a job in the `held` state: `counts()`, `get_jobs("held")`,
`search`, `remove_job()` and `clean("held")` all see it. `retry_job()` and
`promote_job()` return `False` for one: it has not failed, and it is not waiting
on a clock.

## Custom ids and deduplication

Two distinct tools, usable independently:

- **`job_id="order-123"`** - id-based dedup. Adding a job whose id already
  exists is idempotent: nothing is enqueued and the existing job's id comes
  back. The id frees up when the job is removed, [retention](#retention)
  included: on defaults, once 1000 newer jobs have completed. A queue that
  relies on an id for longer keeps more history.
  Must be a non-empty, non-all-digits string - all-digit ids would collide with
  auto-generated ones.
- **`deduplication={"id": "sync-user-42", "ttl": 60_000}`** - a throttle window.
  While the ttl lives, repeat adds with the same dedup id are ignored and the
  already-queued job's id is returned. Self-expiring; nothing to clean up at
  finish time.

`job_id` answers "this exact piece of work must exist at most once";
`deduplication` answers "don't enqueue this more often than every X".

A custom id becomes the job's Redis key, beside the queue's own keys, so `add()`
refuses one that would land on another key: a queue key's name (`completed`,
`marker`, ...), a queue namespace or its bare name (`repeat:`, `worker:`,
`metrics:`, `de:`, and so `de` itself, whose lock would be `de:lock`), or another
job's aux key (`...:lock`, `:logs`, `:deps`, `:results`, `:cfail`). Colons are
otherwise fine: `order:123`.

To enqueue a parent job together with children that must run first
(fan-out/fan-in, chains), use `add_flow()` - see [Flows](flows.md).

## Waiting for a result

```python
job = await queue.add("welcome", {"user_id": 42})
value = await job.result(timeout=30)        # or queue.result(job.id)
```

`result()` resolves with the processor's return value, raises `JobFailedError`
on terminal failure, or `TimeoutError` after `timeout`. It registers for the
job's events *before* checking state, so a job that finishes while you wait is
never missed - and it works even when the job hash was auto-removed, as long as
`result()` was awaited before the job finished. A retrying job keeps you
waiting; only the terminal outcome resolves the call.

## Inspecting the queue

| Call | Returns |
|---|---|
| `await queue.counts()` | One count per `JobState`: `wait`, `active`, `delayed`, `held`, `waiting-children`, `completed`, `failed`, `cancelled`. |
| `await queue.get_job(job_id)` | A `Job` snapshot, or `None`. |
| `await queue.get_jobs(state, start, end)` | A page of jobs; `wait` comes back in global priority order, finished states newest-first. |
| `await queue.get_logs(job_id)` | Log lines appended by the processor. |
| `await queue.search(state, query, scan_limit=500)` | Substring match over `name`/`data` within the most recent `scan_limit` jobs of a state. A bounded scan, not an index - surface the bound honestly in UIs. |
| `await queue.workers()` | Live workers from their heartbeats; stale entries are pruned (and logged as `lost`) on read. |
| `await queue.departed_workers()` | Recent departures, newest first: graceful `stopped` or crashed `lost`. |
| `await queue.metrics(minutes=60)` | Per-minute `{timestamp, added, completed, failed, ms}` points, oldest first, zero-filled for charting. Counters are written inside the same atomic scripts as the transitions (a count can never disagree with the state change it counts); `added` counts real inserts (dedup hits and id replays don't count), `failed` means terminal failures - retries don't count, stall-failures do. Buckets expire after 8 hours. |
| `await queue.metrics_by_name(minutes=60)` | Per-job-name `{name, completed, failed, ms}` totals over the window, failures first - the triage order ("which job is responsible"), not the volume order. |
| `await queue.latency()` | Age (ms) of the next-to-run waiting job, `0` when nothing waits. Depth says how much is queued; latency says how far behind the workers are. |

## Admin operations

| Call | Does |
|---|---|
| `await queue.retry_job(job_id)` | Move one failed job back to the queue. Flow-aware: retrying a flow parent re-drives its whole failed subtree (failed children pulled along, completed ones kept); a retried child re-joins its parked parent's barrier ([Flows](flows.md)). |
| `await queue.retry_all_failed(limit=1000)` | Re-queue every failed job (pipelined, one round trip per batch); returns how many were retried. |
| `await queue.promote_job(job_id)` | Run a delayed job now. |
| `await queue.cancel_job(job_id)` | Stop a job wherever it is, leaving it in `cancelled`. One that has not started ends at once; a RUNNING one is asked, and its worker stops the processor where it awaits ([Processing](processing.md#cancellation)). A flow parent takes its subtree. False when there was nothing to stop. |
| `await queue.remove_job(job_id)` | Delete a job from every state, with its lock, logs and flow keys. A RUNNING job's processor is stopped too, so its worker slot frees at once rather than when the work happens to end; the job is removed, not `cancelled`. Removing a flow parent removes its whole subtree - children included, even running ones. |
| `await queue.clean(state, limit=1000)` | Remove every job in a state (pipelined). |
| `await queue.pause()` / `resume()` / `is_paused()` | Stop workers claiming new jobs (in-flight jobs finish); resume wakes idle workers. |

These are the operations a dashboard such as
[matador](https://github.com/ilovepixelart/matador) calls under its buttons -
they're ordinary public API.

## Lifecycle

A `Queue` opens its Redis connection eagerly and starts no background work; the
first `result()` call starts a small shared event listener. Call
`await queue.close()` when you're done with it (anyone still inside `result()`
fails fast rather than waiting out their timeout).

Repeatable and cron schedules are their own page: [Scheduling](scheduling.md).
What happens to a job after a worker picks it up: [Processing](processing.md).
