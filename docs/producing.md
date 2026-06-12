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
| `remove_on_complete` | `None` | Auto-removal for successes: `None`/`False` keep all, `True` remove at once, `N` keep the newest N, `{"count": N, "age": seconds}` bound both. |
| `remove_on_fail` | `None` | Same, for terminal failures. |

Per-queue defaults go on the constructor and merge under per-call options:

```python
queue = Queue("emails", default_job_options={"remove_on_complete": 1000, "attempts": 3})
```

Auto-removal is enforced inside the finish script itself - there is no separate
cleanup process to run or forget.

## Custom ids and deduplication

Two distinct tools, usable independently:

- **`job_id="order-123"`** - id-based dedup. Adding a job whose id already
  exists is idempotent: nothing is enqueued and the existing job's id comes
  back. The id frees up when the job is removed (including by auto-removal).
  Must be a non-empty, non-all-digits string - all-digit ids would collide with
  auto-generated ones.
- **`deduplication={"id": "sync-user-42", "ttl": 60_000}`** - a throttle window.
  While the ttl lives, repeat adds with the same dedup id are ignored and the
  already-queued job's id is returned. Self-expiring; nothing to clean up at
  finish time.

`job_id` answers "this exact piece of work must exist at most once";
`deduplication` answers "don't enqueue this more often than every X".

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
| `await queue.counts()` | `{"wait": n, "active": n, "delayed": n, "completed": n, "failed": n}` |
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
| `await queue.retry_job(job_id)` | Move one failed job back to the queue. |
| `await queue.retry_all_failed(limit=1000)` | Re-queue every failed job (pipelined, one round trip per batch); returns how many were retried. |
| `await queue.promote_job(job_id)` | Run a delayed job now. |
| `await queue.remove_job(job_id)` | Delete a job from every state, with its lock and logs. |
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
