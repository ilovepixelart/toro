# Upgrading

Breaking changes by release, newest first, each with what to do about it.

## 1.0.3

Five things change for a caller, and two for a rolling upgrade. The rest are fixes.

**`attempts_made` counts the runs that finished.** A run cut short by a stall or a
shutdown used to spend one of the job's `attempts`, so with `attempts=2` a job whose
first run stalled was failed for good after one real try. The counter now moves when a
run commits, and inside a processor it reads `0` on the job's first run (it read `1`).

**A `Queue` call whose reply times out raises `redis.exceptions.TimeoutError`** and
is not sent again. A re-sent `add()` whose first attempt had reached Redis enqueued
the job twice. A connection error is still retried three times with backoff; a
timeout is the caller's to handle, since the command may have run.

**`run()` cancelled outright ends cancelled.** A worker whose `run()` task is
cancelled without `stop()` (a framework cancelling its tasks, Ctrl-C under
`asyncio.run()`) ends its loops, leaves the job in flight to the stalled sweep and
re-raises `CancelledError`, as any cancelled task does. It used to return normally,
and on Python 3.10 it failed that job and went on claiming instead.

**`add_scheduler(every=...)` rejects a fractional or bool value** with `ValueError`.
`every=1000.0` was stored as text the worker could not read, which ended the schedule
at its first pickup; a whole float such as `60_000 / 2` still works.

**Delayed jobs are promoted by the claim, not by a sweep.** Every worker used to move
due delayed jobs into the queue once a second, so an idle fleet's Redis load grew with
its size. A claim now promotes what is due before it picks, and a worker with nothing
to claim blocks until the next due time the claim answered with, woken sooner by any
job delayed to an earlier time. A delayed job runs when due rather than up to a
second later, an idle slot sends one blocking pop per `block_timeout`, and the
schedule check that shared the sweep's loop runs every five seconds.

**Rolling upgrade.** Where `result()` is used with `remove_on_complete=True`, upgrade
producers before workers: a 1.0.3 worker publishes such a result inside the event
only (`resultJson`), and a 1.0.2 producer reads it from the job's hash, which is
already gone, and resolves `None`. A job delayed by a 1.0.2 producer does not wake a
1.0.3 worker, which sees it at its next claim or idle poll (`block_timeout`, 5 s by
default); 1.0.2 workers keep their own sweep.

Fixes:

- `result()` delivers exactly what the processor returned: numbers over 14 digits and
  empty lists were rounded and turned into `{}` on the way through the event.
- A repeatable schedule survives an occurrence picked up early, one that fails before
  minting its successor, `cancel_job`, `remove_job` or `clean("delayed")` of a pending
  occurrence, and `remove_scheduler()` landing while the next occurrence is minted.
- Lock renewal survives a Redis error while the lease still holds; the job is not
  run twice.
- A finish is re-sent through a Redis blip while the lease holds, and one whose first
  send ran but lost its reply is answered as that send was, with the job it fetched.
  It used to be dropped (the job re-ran after the sweep) and, once re-sent, refused as
  a lost lock, leaving the fetched job locked in `active` until the sweep.
- A run that lost its lock cannot commit over its own worker's re-run of the job.
- Progress, log lines and stack traces are written only to a job that still exists,
  and the stack trace is stored atomically with the failure.
- `retry_job` starts the stall count over, and retrying a flow parent whose failed
  child was removed by retention runs it instead of parking it for good.
- `stop()` landing before or during `run()`'s startup ends the run; a worker run
  again reports as running, not draining.
- A job cut off by `stop()` past the grace period, or by a cancelled `run()`, goes
  straight back to the queue with its lock dropped. It used to sit locked in `active`
  until the lock expired and a sweep recovered it, 60 to 90 s later, and that recovery
  spent one of its `max_stalled_count` stalls: at the default of 1, a job cut off by two
  deploys in a row was failed for good. A sync processor's job still goes to the sweep.
- A cancel request applies to the run it was made for: a job id re-added while its
  cancelled run unwinds is no longer cancelled with it.
- An `add()` or `add_flow()` re-sent by the client after a connection dropped while
  the reply was on its way no longer enqueues a second copy: each call carries a token
  the script remembers for a minute (`add:<token>`, a new reserved key namespace).
- The schedule check, the stalled sweep and the heartbeat log one warning per
  failure episode and one line on recovery, where they were silent.
- `get_job()` returns `None` for an id that names one of the queue's own keys or a
  hash that is not a job's; `get_jobs()` reads a negative index from the end and an
  inverted range as empty in every state; `latency()` counts from when the head job
  could first run rather than from its add.
- `roots_counts()` and `get_jobs_roots()` no longer diff the whole state on every
  call: on 200,000 waiting jobs a dashboard refresh cost 30 to 100 ms of Redis time
  and now costs about 1 ms. The Redis floor is unchanged (`ZMSCORE` joins
  `ZDIFFSTORE`, both 6.2).

New: a per-job `timeout` (ms) fails an async processor that runs past it, with a
`TimeoutError` that names the limit; the job then retries like any other failure.
Without one, a hung processor renewed its lock forever and kept its slot, its share of
the global cap and its concurrency key until someone cancelled it.

CI now runs the suite on the declared redis-py floor (5.0.1) as well as Redis 6.2.

New: a `backoff` dict accepts `max` (a cap in ms; exponential backoff otherwise
doubles without limit) and `jitter` (0 to 1: up to that share of the delay, added at
random). Without jitter, every job of a batch that failed together retried on the same
millisecond. Neither is on by default.

New: `await queue.set_limits(global_concurrency=..., rate_limit=...)` stores the two
queue-wide limits on the queue, where every claim reads them, so they apply to every
worker at once and a change needs no rollout; `await queue.limits()` reads them back
(`None` for a queue with no limits of its own).
A worker's `global_concurrency` and `rate_limit` arguments still work, and apply only
to a queue whose limits were never set; `set_limits()` with no arguments gives the
queue limits of its own, none, and the workers' arguments no longer apply.

## 1.0.2

Nothing breaks. One fix.

**Latency percentiles are read against the histogram's real bucket edges.** Each
bucket's upper bound was truncated to whole milliseconds while the durations were
bucketed against the exact bound, so at 22 of the 25 edges a duration equal to the
stated bound was counted in the bucket that bound claimed to close: bucket 3 said it
ended before 67 ms and held a 67 ms job. The bounds are now rounded up (67.5 to 68),
which is the same edge for whole-millisecond durations. The estimates `percentiles()`,
`flow_percentiles()` and `metrics_by_name()` report move by under 1 ms.

## 1.0.1

Nothing breaks. Two fixes.

**A retried flow parent waits for the children retried with it.** A parent whose
`on_fail="continue"` child failed, and which then failed in its own processor, went
straight back to `wait` when the flow was retried, and ran beside the child being
retried, on the results it had last time. It now waits for every child that has not
completed or been cancelled, through `retry_job`, `retry_flow` and `retry_all_failed`
alike.

**Changing a schedule's cadence replaces its pending occurrence.** Registering an
existing scheduler id with a different `every` or `cron` left the occurrence already
pending under the old cadence: it still ran, scheduled its own successor, and survived
`remove_scheduler`. It is now removed while it has not started; one already running
finishes.

The package is now marked `Development Status :: 5 - Production/Stable`.

## 1.0.0

Nothing new ships: 1.0 is the promise rather than a feature, what is public, what may
change under it, and what the stored keys mean. See [Versioning](versioning.md).

One thing breaks: input that 0.x accepted without checking now raises `ValueError`
when the queue, the worker or the job is created. Each rule keeps a value from landing
somewhere it cannot be read back, and each error names the rule it broke.

| Now rejected | Where it was a problem |
|---|---|
| A queue name with `:`, or a prefix or queue name over 128 characters or with a control character | Two different (prefix, name) pairs could share one key namespace. |
| A job name over 128 characters or with a control character | Names are metric labels and log fields. |
| A custom `job_id` with `/`, a control character, or over 256 characters | An id is a path segment wherever it is shown; such a job could not be opened or removed. |
| `delay`, `attempts`, `priority`, or a backoff or retention count or age that is fractional, negative, a bool or a string (a whole float such as `86_400 / 2` is still accepted) | The scripts take whole numbers; a fractional count used to keep everything or fail the finish inside Redis. |
| A backoff `type` other than `"fixed"` or `"exponential"`, or a retention dict with keys other than `count` and `age` | An unknown backoff type ran as fixed, and other retention keys were never read. |

What to do: create queues, workers and jobs with 1.0 in a test first. Anything that
raises was being stored in a shape that could not be read back as meant.

Two things appear in Redis. A queue gains a `meta` hash holding the data-model
version, written by the first 1.0 write; `meta` therefore joins the reserved job ids,
so a job with that id must be removed before upgrading (`await queue.remove_job("meta")`),
exactly as `totals` did in 0.10.0. And `toro.DATA_MODEL_VERSION` is now exported, so
a deployment can assert on it.

A 1.0 library refuses a queue whose model is newer than it understands, which is what
makes a rolling upgrade safe in the direction that matters. An older library ignores
the marker, so upgrading from 0.11 needs no ordering: producers and workers can move
in any order, and a mixed fleet is proved (`tests/compat/rolling_upgrade.py`).

## 0.11.0

Nothing breaks. Three things are new.

A plain `def` processor now runs in a thread instead of raising `TypeError` after
running inside the event loop. If you were relying on that error, you were relying on
a bug. Two consequences worth knowing before you switch a processor over: cancelling
a sync job waits for its thread (the job ends `cancelled` when the work does), and the
process cannot exit while a sync job is running. See [Processing](processing.md).

A processor whose return value is not JSON now fails its job, with the encoder's
message as the reason. It used to escape the commit and leave the job `active` until
the stalled sweep re-ran it, which mattered most for test suites using mock
processors.

A watchdog warns when a processor blocks the event loop for longer than half the lock
renewal interval, naming the jobs in flight. It is on by default, because the failure
it catches (renewals stop, the stalled sweep re-runs the work) is otherwise silent.
`blocked_warning=0` turns it off, and `blocked_warning=<seconds>` sets the threshold.

`Queue.pending()` collects jobs and sends them in one round trip when you say the
transaction committed. See [Producing](producing.md).

## 0.10.0

Nothing breaks. Two things are new and worth knowing about.

A queue now keeps lifetime counters in a `totals` hash that never expires, written
in the same step as the transitions they count. It is one small hash per queue; a
queue upgraded mid-life starts counting from the upgrade, so `rate()` is correct
from then on and the absolute totals are not history.

`metrics_text()` renders OpenMetrics for a scraper, and matador serves every queue
it watches from `/metrics`. See [Operating](operating.md).

One id is now reserved: `totals`, the key the counters live on, alongside the other
reserved ids. A job created before this release with that id is sitting on that key,
so remove it (`await queue.remove_job("totals")`) and the counters start clean. Left
there, the counter writes land in its hash; a scrape reads only its own five fields,
so it keeps working, and retention removing that job would take the counters with it.

## 0.9.0

### Upgrade every worker before you cancel anything

A running job is stopped by the worker that owns its processor, and that worker has
to be on 0.9.0 to hear the request. Ask an older one and `cancel_job()` still answers
`True`, but nothing stops: it holds the lock throughout, so it finishes the work and
commits a normal `completed` (or `failed`), return value and all. The caller is told
the job was stopped and gets a job that ran to the end. Roll the fleet first, then
start cancelling. Nothing changes for a queue that never calls `cancel_job()`.

`remove_job()` on a RUNNING job now stops its processor, where it used to leave the
work running with nowhere to report until it ended on its own. A processor that must
not be interrupted should not be removed mid-flight; wait for it, or let the job
finish. The slot it holds, including under a global concurrency cap, frees at once.

`cancelled` is a new `JobState`, so anything enumerating states sees an eighth. It
is NOT a failure: it is counted, listed and retained separately, `retry_job()`
refuses it, and `result()` raises `JobCancelledError` rather than `JobFailedError`.
Code that retries on `JobFailedError` therefore will not retry cancelled work, which
is the point.

## 0.8.0

### Upgrade every worker before you use a concurrency key

A key is taken when a job is enqueued and passed on by the script that commits the
job's finish, and that script is the one the WORKER registers. A worker from an
earlier release commits a finish without passing the key on, so the key stays held
by a job that is already done and every job behind it waits forever. Roll the whole
fleet to 0.8.0 first, then start passing `concurrency_key`. Nothing changes for a
queue that never passes one.

`held` is a new `JobState`, so anything that enumerates states (a dashboard, a
`counts()` reader, a `get_jobs` loop) sees a seventh. A held job waits on its key
rather than on a worker: it is in no other collection, `latency()` does not see it,
and `remove_job` is what cancels it.

## 0.7.0

### Finished jobs are bounded by default

To keep every finished job, as earlier releases did, set `False` for the queue on
every producer:

```python
queue = Queue(
    "emails",
    default_job_options={"remove_on_complete": False, "remove_on_fail": False},
)
```

`remove_on_complete` and `remove_on_fail` left unset used to keep every finished
job, so a queue on its defaults grew until Redis ran out of memory. Unset now
keeps the newest 1000 completed jobs and the newest 5000 failed jobs. `False`,
`True`, a count and `{"count", "age"}` mean what they meant before, with one
edge made whole: a count or an age is now floored to an integer wherever it is
given (`2.9` keeps 2), where a fractional bare count kept everything and a
fractional `count` in the dict form failed the finish inside Redis.

| | Before | Now |
|---|---|---|
| Option unset | keep every finished job | keep the newest 1000 completed, 5000 failed |
| `False` | keep every finished job | keep every finished job |
| A count bound meeting a deep backlog | deleted it all in one script | deletes at most 1000 jobs per finish |

- **This deletes history.** Upgrading the *workers* is what changes the behavior:
  retention is applied as a job finishes, under the options stored with it, jobs
  enqueued before the upgrade included. From the first finish on, history past the
  bound is deleted, oldest first, at most 1000 jobs per finish.
- **`False` on some jobs is not enough.** A trim covers the whole set, so a job kept
  with `False` is still trimmed by a later job that finishes under a bound. See
  [Retention](producing.md#retention).
- **Register your schedulers again.** A scheduler stores its job options when it is
  registered, and workers mint every occurrence from that stored template. Templates
  written by an earlier release store retention unset, so their jobs trim the queue
  whatever `default_job_options` says. `add_scheduler()` with the same id updates the
  template in place; from this release it merges the queue's defaults into it.
- **A custom `job_id` stays idempotent only while its job is kept.** Adding an id that
  exists is a no-op; once the job is trimmed the id is free, and the same `add()`
  enqueues the work again. On defaults that is after 1000 newer completions. Keep
  more history, or keep everything, on queues that rely on an id for longer.
- A trimmed job can no longer be read: `get_job()` returns `None` and `result()`
  called after the trim times out. Metrics are separate counters and keep
  counting.
- **A flow is retained as one unit.** While it runs, its finished children are out
  of every bound's reach; when its root is trimmed, the subtree goes with it. In
  `completed` and `failed` a flow child's score is its retention position, not
  its finish time (`finishedOn` is): a running flow's children score above every
  timestamp, a settled flow's children score just above their root.

### Schedulers inherit the queue's `default_job_options`

`add_scheduler()` ignored them: a queue with `default_job_options={"attempts": 3}`
ran its scheduled jobs with one attempt. They now merge under the scheduler's own
options, as they do for `add()`. Pass the option to `add_scheduler()` to keep a
scheduler on a different value.

### Some custom job ids are refused

`add()` raises `ValueError` for a `job_id` that would land on another key of the
queue: a queue key's name (`completed`, `marker`, ...), a queue namespace or its
bare name (`repeat:`, `worker:`, `metrics:`, `de:`), or another job's aux key
(`...:lock`, `:logs`, `:deps`, `:results`, `:cfail`). Such an id made the job's
hash and that key one Redis key: `completed` broke the queue with `WRONGTYPE`,
`prioritized` made `add()` return as if the job existed and enqueue nothing. A
job already stored under one of these ids keeps working; only `add()` refuses
it, including a repeat `add()` of that job. Colons are otherwise allowed. See
[Custom ids](producing.md#custom-ids-and-deduplication).

### `JobOptions.keep_args` is removed

It mapped a remove option to two arguments of the finish scripts. The scripts now
read the option from the job themselves, so nothing computes it on the Python side.
