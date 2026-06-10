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
| `completed` | ZSET | Successfully-finished ids, scored by finish time (for auto-removal + listing). |
| `failed` | ZSET | Terminally-failed ids, scored by finish time. |
| `meta-paused` | string (flag) | Exists only while the queue is paused; workers stop claiming new jobs. |
| `events` | pub/sub channel | Carries `added` / `progress` / `completed` / `failed`; drives `result()` and live dashboards. |
| `limiter` | HASH | The queue-wide rate-limit token bucket (`{tokens, ts}`), shared by every worker. |
| `stalled` | SET | Candidate ids for the mark-and-sweep recovery pass. |
| `stalled-check` | string (PX) | Throttle key so the stalled sweep runs about once per interval cluster-wide. |
| `repeat` | ZSET | Scheduler id -> next-run timestamp. |
| `workers` | ZSET | Live worker id -> last-heartbeat ms; stale entries pruned lazily on read. |
| `departed` | LIST (capped) | Recent worker departures: graceful `stopped` or `lost` (crashed). |

## Per-scheduler, per-worker, per-job keys

| Key | Type | Holds |
|---|---|---|
| `repeat:<schedulerId>` | HASH | A scheduler's template: `name`, `every`/`cron`, `data`, `opts`. |
| `worker:<workerId>` | HASH | A worker's presence record: host, pid, concurrency, current jobs, processed/failed counts, state. |
| `<jobId>` | HASH | The job itself: `name`, `data`, `opts`, `state`, `attemptsMade`, timestamps, `returnvalue`/`failedReason`, `progress`, `stacktrace`, ... |
| `<jobId>:lock` | string (token, PX) | The per-job lock: the owning worker's token with an expiry. Only the holder may finish or renew it. |
| `<jobId>:logs` | LIST | Log lines appended by `job.log(...)` from inside a processor. |

Note the job hash key is just `<prefix>:<name>:<jobId>` (no extra segment), so a job
`5` on `toro:emails:` is the hash `toro:emails:5`, with `toro:emails:5:lock` and
`toro:emails:5:logs` beside it.

## How the pieces connect

- A job moves between `prioritized` / `active` / `delayed` / `completed` / `failed`
  as its state changes; the move and the hash update happen in one Lua script. See
  [Architecture](architecture.md).
- The `lock` + `stalled` keys are the at-least-once machinery. See
  [Reliability](reliability.md).
- `repeat` + `repeat:<id>` drive [scheduling](scheduling.md); `workers` +
  `worker:<id>` + `departed` drive worker presence in the dashboard.
