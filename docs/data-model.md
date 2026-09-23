# Data model

Everything toro stores lives in Redis under a per-queue prefix. All key names are
computed in one place (`toro/keys.py`) so the Lua scripts and the Python side can
never disagree about where something lives.

## Key prefix

For a queue named `<name>` with prefix `<prefix>` (default `toro`), every key
starts with:

```
<prefix>:<name>:
```

So `Queue("emails")` (default prefix) stores everything under `toro:emails:`.
Using a `{braces}` hash-tag in the prefix forces all of a queue's keys onto one
Redis Cluster slot, which the multi-key Lua scripts require.

## Queue-wide keys

| Key suffix | Type | Holds |
|---|---|---|
| `id` | string (counter) | `INCR`-ed to mint auto job ids. |
| `prioritized` | ZSET | Waiting jobs in global priority order; score packs (priority, sequence). This *is* the `wait` state. |
| `marker` | ZSET | A single idempotent base member (`"0"`); idle workers `BZPOPMIN` it to wake. It only signals; the real claim is atomic. |
| `pc` | string (counter) | Priority sequence counter, so same-priority jobs stay FIFO. |
| `active` | LIST | Ids currently claimed by a worker and running. |
| `delayed` | ZSET | Ids scored by their process-at timestamp (ms); promoted to `prioritized` when due. |
| `completed` | ZSET | Successfully-finished ids, scored by retention position: finish time, except a running flow's children (above every timestamp) and a settled flow's children (just above their root). |
| `failed` | ZSET | Terminally-failed ids, scored the same way. |
| `waiting-children` | ZSET | Flow parents parked until their children settle, scored by enqueue time. |
| `held` | ZSET | Jobs waiting on a concurrency key, scored by enqueue time (the listing). |
| `cancelled` | ZSET | Jobs stopped on purpose, scored like the other finished sets. |
| `ck:<key>` | STRING | The job that holds a concurrency key. Exists only while a job holds it. |
| `held:<key>` | ZSET | The jobs queued behind one key, scored as `prioritized` would score them. |
| `meta-paused` | string (flag) | Exists only while the queue is paused; workers stop claiming new jobs. |
| `events` | pub/sub channel | Carries `added` / `progress` / `completed` / `failed` / `cancelled`; drives `result()` and live dashboards. |
| `cancel` | pub/sub channel | Carries bare job ids: a cancellation asked of whichever worker is running that job. Separate from `events` because that one carries a message per job, and a worker listening there would parse the whole firehose to catch something rare. |
| `limiter` | HASH | The queue-wide rate-limit token bucket (`{tokens, ts}`), shared by every worker. |
| `stalled` | SET | Candidate ids for the mark-and-sweep recovery pass. |
| `stalled-check` | string (PX) | Throttle key so the stalled sweep runs about once per interval cluster-wide. |
| `repeat` | ZSET | Scheduler id -> next-run timestamp. |
| `workers` | ZSET | Live worker id -> last-heartbeat ms; stale entries pruned on read, and entries a day old by any worker's heartbeat. |
| `departed` | LIST (capped) | Recent worker departures: graceful `stopped` or `lost` (crashed). |
| `metrics:<minute>` | HASH | Per-minute counters (`added`/`completed`/`failed`/`cancelled`/`ms`, per-name fields, histograms); self-expiring. |
| `meta` | HASH | What this queue IS rather than what it holds: `model`, the [data-model version](versioning.md) it was stamped with. Written once, on the first write by a 1.0+ library; a library that finds a newer one refuses to run. |
| `totals` | HASH | Outcome counters and `ms` since the queue was created, and never expiring: `rate()` reads across restarts, and a counter that resets reads as a cliff. Five fields, whatever the traffic; the per-name fields and histograms stay in the expiring buckets. |
| `de:<dedupId>` | string (PX) | A live deduplication throttle window; holds the already-queued job's id. |

## Per-scheduler, per-worker, per-job keys

| Key | Type | Holds |
|---|---|---|
| `repeat:<schedulerId>` | HASH | A scheduler's template: `name`, `every`/`cron`, `data`, `opts`. |
| `worker:<workerId>` | HASH | A worker's presence record: host, pid, concurrency, global concurrency cap, current jobs, processed/failed counts, state. Expires a day after the last heartbeat. |
| `<jobId>` | HASH | The job itself: `name`, `data`, `opts`, `state`, `attemptsMade`, timestamps, `returnvalue`/`failedReason`, `progress`, `stacktrace`, plus flow linkage on flow jobs: `parentId`/`onFail`/`rootId` (children), `children` (parents), and `ckey` on a job with a concurrency key. |
| `<jobId>:lock` | string (token, PX) | The per-job lock: the owning worker's token with an expiry. Only the holder may finish or renew it. |
| `<jobId>:logs` | LIST | Log lines appended by `job.log(...)` from inside a processor. |
| `<jobId>:deps` | SET | A flow parent's still-pending child ids - the fan-in barrier; the parent releases when it empties. |
| `<jobId>:results` | HASH | Child id → returnvalue JSON, written as each child completes. |
| `<jobId>:cfail` | HASH | Child id → failure reason for children failed under `on_fail="continue"`. |
| `<jobId>:live` | ZSET | On a flow ROOT while its flow runs: the flow's finished jobs, which are scored out of the trims' reach until the root settles. |

Note the job hash key is just `<prefix>:<name>:<jobId>` (no extra segment), so a job
`5` on `toro:emails:` is the hash `toro:emails:5`, with `toro:emails:5:lock` and
`toro:emails:5:logs` beside it.

## How the pieces connect

- A job moves between `prioritized` / `active` / `delayed` / `held` /
  `waiting-children` / `completed` / `failed` / `cancelled` as its state changes; the
  move and the hash update happen in one Lua script. See [Architecture](architecture.md).
- `:deps` + `:results` + `:cfail` are the flow fan-in machinery - children settle
  into them as they finish. See [Flows](flows.md).
- The `lock` + `stalled` keys are the at-least-once machinery. See
  [Reliability](reliability.md).
- `repeat` + `repeat:<id>` drive [scheduling](scheduling.md); `workers` +
  `worker:<id>` + `departed` drive worker presence in the dashboard.
