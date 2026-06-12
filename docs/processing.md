# Processing jobs

The consumer side: the `Worker`, its processor function, concurrency, lifecycle
events, rate limiting, and shutdown.

## A worker

```python
async def send_welcome(job):
    await mailer.send(job.data["user_id"])
    return {"sent": True}            # JSON-serializable -> job.returnvalue

worker = Worker("emails", send_welcome, concurrency=20)
await worker.run()                   # awaits until stop()
```

The processor is an `async` function of one argument, the `Job`. Returning
commits the job as `completed` (the return value, JSON-serialized, becomes
`returnvalue`); raising routes it through the retry policy - back to the queue
(or to `delayed` under [backoff](producing.md)) while attempts remain, then
terminally `failed` with the exception text and a `stacktrace` field.

## Inside the processor

The worker injects a runtime context into the job while it runs:

```python
async def handle(job):
    await job.log("starting")               # appends to <jobId>:logs
    await job.update_progress(42)           # publishes a `progress` event
    if job.attempts_made > 1:
        ...                                 # this is a retry
```

`update_progress` takes a number or any JSON value; dashboards render it live.

## Concurrency

`concurrency=N` runs N processing loops ("slots") as `asyncio` tasks on one
event loop - see [workers vs. slots](concepts.md). Two practical consequences:

- **Stay `await`-y.** Slots are not threads: CPU-bound or blocking code stalls
  every sibling slot *and* the lock renewers that keep your jobs from being
  treated as stalled ([Reliability](reliability.md)).
- **Connections scale with concurrency.** Each idle slot parks a (blocking-pop)
  connection, so the worker sizes its own pool to `concurrency + headroom`. If
  you pass your own `connection`, size its pool accordingly.

A busy slot doesn't return to the blocking wait between jobs: the finish call
also claims the next job in the same round trip (fetch-next - see
[Architecture](architecture.md)), so a saturated worker runs at one round trip
per job.

## Options

| Option | Default | Meaning |
|---|---|---|
| `concurrency` | 1 | Parallel slots in this worker. |
| `rate_limit` | `None` | `{"max": N, "duration": ms}` - queue-wide token bucket (below). |
| `block_timeout` | 5.0 s | How long an idle slot blocks waiting for a wakeup before re-checking. |
| `lock_duration` / `lock_renew_time` / `renew_locks` | 30000 / half / `True` | The at-least-once lease - see [Reliability](reliability.md). |
| `stalled_interval` / `max_stalled_count` | 30000 / 1 | The recovery sweep - same page. |
| `grace_period` | 30.0 s | Default drain window for `stop()`. |
| `heartbeat_interval` | 5000 ms | Presence cadence for the workers view. |

## Rate limiting

```python
worker = Worker("emails", handle, rate_limit={"max": 100, "duration": 60_000})
```

At most `max` jobs start per `duration`, across **all** workers on the queue -
the token bucket lives in Redis, shared, so adding workers doesn't multiply the
limit (give every worker the same config). When a claim hits the limit the job
goes back untouched: no attempt is consumed, and the worker sleeps until a token
frees (emitting a `rate-limited` event with the wait).

## Lifecycle events

`worker.on(event, fn)` registers plain in-process callbacks (sync, fire-and-forget):

| Event | Args | When |
|---|---|---|
| `completed` | `job, result` | A job committed successfully. |
| `failed` | `job, exc` | A job failed terminally. The sweep fires it too for stall-failed jobs - there with the job *id* (not a `Job`) and a `RuntimeError("job stalled too many times")`, while the job hash's `failedReason` reads `"job stalled more than allowable limit"`. |
| `retrying` | `job, exc` | A failure with attempts left was re-queued. |
| `stalled` | `job_id` | The sweep recovered one of this queue's jobs. |
| `lock-lost` | `job_id` | This worker's lock was taken over; its result was dropped. |
| `rate-limited` | `retry_ms` | A claim hit the rate limit. |

These are this worker's own hooks. Cross-process consumers (dashboards,
`result()`) use the pub/sub events channel instead - see [Concepts](concepts.md).

## Presence

Every `heartbeat_interval` the worker flushes a presence record (host, pid,
concurrency, what it's running, processed/failed counts, state). That powers the
dashboard's workers view; a worker that misses heartbeats long enough is pruned
and logged as a `lost` departure, while `stop()` flips it to a visible
`stopping` state first and logs `stopped` - so the dashboard can tell a drain
from a crash.

## Shutdown

```python
await worker.stop()        # or stop(grace_period=10)
```

`stop()` stops claiming new jobs, lets in-flight jobs finish for up to the grace
period, cancels whatever remains (those jobs' locks expire and the sweep
recovers them - nothing is lost), deregisters presence, and closes the
connection. Pair `run()`/`stop()` with your framework's startup/shutdown hooks.
