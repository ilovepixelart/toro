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

A flow parent's processor pulls what its children produced (both helpers are
processor-only and raise `RuntimeError` elsewhere - use the `Queue`-side
equivalents outside a worker):

```python
results = await job.children_results()   # {child_id: returnvalue}
failures = await job.failed_children()   # {child_id: reason} (on_fail="continue")
```

See [Flows](flows.md) for the full model.

## A sync processor

A plain `def` is a processor too. It runs in a thread, so the loop stays free to
renew locks and answer heartbeats:

```python
def handle(job):                 # no async, no await
    return requests.get(...).json()   # a blocking library is fine here
```

The threads are the worker's own, `concurrency` of them, created on the first sync
job and given back when the worker stops. That is deliberate: `asyncio`'s default
executor is shared process-wide and sized `min(32, cpu + 4)`, so a worker with more
slots than that would queue sync jobs behind a pool it does not control, each waiting
job holding its lock while it waits.

**A thread cannot be interrupted**, and everything else follows from that.

`cancel_job()` on a running sync job records the request and the job ends `cancelled`
when its processor returns, not before. The alternative would be worse than the wait:
freeing the slot while the thread ran on would leave the worker with more slots than
its pool has threads (the next job claimed, locked, renewed, and not running), and the
terminal state would hand on the job's `concurrency_key` while the work holding it
carried on. So a sync processor that must stop early has to check something itself.

`stop()` is the same story. In-flight sync jobs get the grace period like any other,
and one that outlasts it is left to the stalled sweep, because its thread cannot be
taken back. **The process cannot exit while that thread runs**: Python joins pool
threads at interpreter exit, so a 10-minute sync job means a 10-minute exit, whatever
the container's termination grace says. Bound the work, or make it interruptible.

**The `Job` runtime API is async**, so `await job.update_progress(...)`,
`job.log(...)` and the flow helpers are not available inside a sync processor: a flow
parent needs an `async def`. Everything the queue itself does (retries, failure,
metrics, retention) is identical for both kinds.

A processor that is neither a plain `def` nor an `async def` (a decorated one, a
callable object) is read off its `__call__`. If that reading is wrong and the call
returns a coroutine, it is awaited rather than handed back as the job's result.

## When the loop is blocked

The failure this catches is silent. An async processor that calls a blocking library
starves the loop; lock renewal is a coroutine, so renewals stop, the stalled sweep
takes the job back, and a queue that looks healthy runs its work twice.

A watchdog measures how late its own sleep returns, which sees every cause including
ones inside a dependency, and warns once per episode:

```
event loop was blocked for 8.4s (threshold 7.5s); jobs in flight: 41, 42.
A processor that blocks the loop stops lock renewal, and the stalled sweep
re-runs its jobs elsewhere.
```

The jobs it names are the shortlist of suspects: the processor that blocked the loop
is one of them. The threshold follows the lock rather than a round number (half the
renewal interval, so 7.5 s at the defaults), `blocked_warning=<seconds>` sets it, and
`blocked_warning=0` turns it off. A `blocked` event carries the same thing to a
dashboard or an alert:

```python
worker.on("blocked", lambda lag, jobs: alert(f"loop blocked {lag:.1f}s: {jobs}"))
```

The fix is almost always one of two things: make the call async, or make the
processor a plain `def` so it runs in a thread.

## Concurrency

`concurrency=N` runs N processing loops ("slots") as `asyncio` tasks on one
event loop - see [workers vs. slots](concepts.md). Two practical consequences:

- **Stay `await`-y.** Slots are not threads: CPU-bound or blocking code stalls
  every sibling slot *and* the lock renewers that keep your jobs from being
  treated as stalled ([Reliability](reliability.md)).
- **Connections scale with concurrency.** Each idle slot parks a (blocking-pop)
  connection, so the worker sizes its own pool to `concurrency + headroom`. If
  you pass your own `connection`, size its pool accordingly, and give it a read
  timeout (`socket_timeout`) above `block_timeout`. A `Queue`'s connection
  already has one for the default `block_timeout`, so sharing it with a worker
  needs nothing. A client you build yourself gets redis-py's default of 5 s. The
  worker keeps its blocking pop under whatever it finds, so a tighter read
  timeout means idle slots re-poll sooner than `block_timeout` asks, with a
  warning logged.

A busy slot doesn't return to the blocking wait between jobs: the finish call
also claims the next job in the same round trip (fetch-next - see
[Architecture](architecture.md)), so a saturated worker runs at one round trip
per job.

## Options

| Option | Default | Meaning |
|---|---|---|
| `concurrency` | 1 | Parallel slots in this worker. |
| `rate_limit` | `None` | `{"max": N, "duration": ms}` - queue-wide token bucket (below). |
| `global_concurrency` | `None` | Cap on jobs active at once across all workers on the queue (below). |
| `block_timeout` | 5.0 s | How long an idle slot blocks waiting for a wakeup before re-checking. |
| `lock_duration` / `lock_renew_time` / `renew_locks` | 30000 / half / `True` | The at-least-once lease - see [Reliability](reliability.md). |
| `stalled_interval` / `max_stalled_count` | 30000 / 1 | The recovery sweep - same page. |
| `grace_period` | 30.0 s | Default drain window for `stop()`. |
| `heartbeat_interval` | 5000 ms | Presence cadence for the workers view. |
| `blocked_warning` | half `lock_renew_time` | Seconds the loop may be blocked before a warning (below). `0` turns it off. |

## Rate limiting

```python
worker = Worker("emails", handle, rate_limit={"max": 100, "duration": 60_000})
```

At most `max` jobs start per `duration`, across **all** workers on the queue -
the token bucket lives in Redis, shared, so adding workers doesn't multiply the
limit (give every worker the same config). When a claim hits the limit the job
goes back untouched: no attempt is consumed, and the worker sleeps until a token
frees (emitting a `rate-limited` event with the wait).

## Global concurrency

```python
worker = Worker("exports", handle, concurrency=10, global_concurrency=3)
```

At most `global_concurrency` jobs are active at once across **all** workers on
the queue, however many processes you run (give every worker the same value).
`concurrency` sizes one worker; this caps the queue.

Use it when the constraint is *occupancy*, not arrival rate: a database pool of
N connections, an API that allows N requests in flight. A rate limit bounds job
*starts*, so with long jobs it says nothing about how many run together.

- A claim at the cap touches nothing: the job keeps its place, no attempt is
  consumed, no rate-limit token is spent. The worker waits for a slot.
- There is no slot counter to leak. The cap counts the `active` list itself, so
  a crashed worker's slots come back when the stalled sweep recovers its jobs
  ([Reliability](reliability.md)).
- Slots stick. A busy worker claims its next job in the same round trip that
  finishes the last, so while the queue stays full the workers holding the slots
  keep them, and another worker can sit idle. Slots move when a holder drains,
  stops, or crashes.
- Removing an active job does not hand its slot on at once: the processor may
  still be running. The slot is reused when that processor ends.
- A changed value takes effect as workers restart. While a rollout mixes caps,
  each worker enforces its own, and a freed slot can wait up to `block_timeout`
  for a worker with room.

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
concurrency, global concurrency cap, what it's running, processed/failed counts,
state). That powers the dashboard's workers view; a worker that misses
heartbeats long enough is pruned and logged as a `lost` departure, while
`stop()` flips it to a visible `stopping` state first and logs `stopped` - so
the dashboard can tell a drain from a crash.

That pruning happens when something reads `Queue.workers()`. With no reader, a
worker killed without deregistering still does not leave its record for good: the
record expires a day after its last heartbeat, and any live worker's heartbeat
drops index entries older than that. A worker that died more than a day before
the first read is gone without a `lost` departure.

## Cancellation

`queue.cancel_job(job_id)` stops a job that is already running. The worker running
it cancels the task its processor is on, so `CancelledError` is raised where the
processor awaits: `finally` blocks and `async with` exits run, and the job lands in
`cancelled`.

```python
async def process(job):
    handle = await open_upload(job.data["path"])
    try:
        await handle.stream()          # cancellation lands here
    finally:
        await handle.abort()           # and this still runs
```

- **Nothing is raised into a processor that never awaits.** Cancellation is
  delivered at an await point, so a tight CPU loop runs to completion. Yield if you
  want to be interruptible.
- **Work that must not be interrupted** belongs in the unwinding, not behind
  `asyncio.shield`. A shield does not hold the cancellation back: the processor is
  cancelled at the shield straight away while the shielded task carries on orphaned,
  which is the very outcome the next point warns about. Catch the cancellation, finish
  what has to finish, then re-raise.

  ```python
  try:
      await long_running()
  except asyncio.CancelledError:
      await commit_what_we_have()   # runs to completion
      raise                         # and the job still ends cancelled
  ```
- **Do not swallow `CancelledError`.** A processor that catches it and returns
  normally does not complete the job: the worker knows it asked this one to stop, so
  the job still ends `cancelled` and the return value is thrown away. The same holds
  for a cleanup that raises on the way out, which is not a failure to retry.
- Workers hear a cancellation over a channel of their own and act at once. A worker
  that missed the message finds it at its next lock renewal instead, so the delay is
  bounded by `lock_renew_time`, never lost.

## Shutdown

```python
await worker.stop()        # or stop(grace_period=10)
```

`stop()` stops claiming new jobs, lets in-flight jobs finish for up to the grace
period, cancels whatever remains (those jobs' locks expire and the sweep
recovers them - nothing is lost), deregisters presence, and closes the
connection. Pair `run()`/`stop()` with your framework's startup/shutdown hooks.
