# Architecture

How toro's core works, and why. Every state transition is an atomic Lua script;
every job is durable in Redis.

> Prior art: the atomic-Lua and lock/stalled-recovery patterns come from the
> Node.js Redis-queue ecosystem; the specifics are toro's own.

## Atomic state transitions via Lua

Every state move (`wait→active`, `active→completed/failed/delayed`,
`delayed→wait`) is a single Redis Lua script, run atomically, so multi-key
"check-then-act" sequences can't interleave. That removes whole classes of race:

- **pop-then-lock gap** — two workers claiming the same job: the claim pops from
  the priority set and sets the lock inside one script.
- **finish-after-steal** — a worker committing a result for a job a stalled sweep
  already re-queued: guarded by a token check plus `LREM active` returning 0.

Scripts live in `scripts.py`, registered with `redis.asyncio`'s `register_script`.
The Python side only assembles KEYS/ARGV; the guarantees live in the Lua.

## Claiming a job: the prioritized set + a wakeup marker

All waiting jobs live in one `prioritized` ZSET, scored
`(PRIORITY_OFFSET - priority) * 2^32 + seq` — a single global order where higher
priority is more urgent and ties stay FIFO (`seq` is a per-queue counter). This
*is* the `wait` state; there is no separate fast-lane list, so a low-priority job
can't starve a high-priority one.

A single ZSET can't be blocking-popped, so wakeup uses a small **base marker**:
producers `ZADD marker 0 "0"` (idempotent) on enqueue, and idle workers park on
`BZPOPMIN marker`. The marker only wakes a worker; the real claim is the atomic
`MOVE_TO_ACTIVE` (`ZPOPMIN prioritized` → push to `active` → set the lock → load
the job). Because the claim is atomic and idempotent, a missed marker can never
strand a job.

## Fetch-next inside finish

A busy worker doesn't go back to the blocking wait between jobs. The finish
scripts (`MOVE_TO_COMPLETED` / `MOVE_TO_FAILED`) commit the current job **and**
claim the next one in the same round trip; the worker only re-parks on the marker
when the queue is empty (or it's shutting down, signaled by a fetch flag, so it
drains cleanly). All claiming funnels through one shared Lua routine
(`lockAndLoad` / `acquireNext`), used by both the wakeup path and fetch-next.

It's mainly a round-trip win: at concurrency 20, process throughput is roughly
2.3× a claim-per-job design, because the separate per-job claim and load collapse
into the finish call.

## At-least-once: locks, tokens, and stalled recovery

The reliability core ([Reliability](reliability.md) is the full guide):

- On claim, the job gets a lock `<id>:lock = <token>` with `PX lockDuration`
  (default 30s). The token is the claiming worker's; only it can renew or finish.
- A per-job renewer extends the lock on a timer and clears the job from the
  `stalled` set while it's alive.
- A background sweep runs every `stalled_interval` (throttled cluster-wide by a
  `stalled-check` key): any job in `stalled` whose lock has expired is recovered
  (`LREM active`, back to the prioritized set, or failed after
  `max_stalled_count`), then the current `active` list is re-marked as stalled.

The guarantee is **at-least-once**: a job is never lost while Redis persists, but
its handler can run more than once (bounded by `max_stalled_count`) if a worker
dies mid-job. Exactly-once *result commit* is enforced by the token-guarded lock
at finish, not by preventing duplicate handler runs.

## Delayed jobs

Delayed jobs and retries with backoff sit in a `delayed` ZSET scored by their
process-at timestamp (ms). A one-second promotion loop in the worker moves any
due jobs into the prioritized set.

## Higher-level features

- **Priorities** — every job is in the one prioritized ZSET above, so priority is
  a single global order with no starvation, FIFO within a band.
- **Repeatable / cron** — `add_scheduler(every=ms | cron=...)` stores a template
  and enqueues the first occurrence as a delayed job; each occurrence mints its
  successor with a deterministic id when a worker picks it up. `trigger_scheduler`
  runs one now, `remove_scheduler` stops the chain. See [Scheduling](scheduling.md).
- **Rate limiting** — a queue-wide token bucket in Redis
  (`Worker(rate_limit={"max": N, "duration": ms})`), shared by every worker on the
  queue. An over-limit claim returns a sentinel and the worker waits out the window.
- **Events** — Redis pub/sub on an `events` channel (`added`, `progress`,
  `completed`, `failed`); `Queue.result()` awaits the terminal event and
  `Worker.on(event, fn)` exposes in-process hooks. See [Concepts](concepts.md).
- **Auto-removal** — `remove_on_complete` / `remove_on_fail` (bool / count /
  `{count, age}`) enforced inside the finish script, not by a separate sweeper.

## The Lua scripts

Every state change is a Lua script in `scripts.py`, registered once per process
with `register_script` (run by `EVALSHA`). Python only assembles `KEYS`/`ARGV`.

The scripts share a small library of routines:

| Routine | Does |
|---|---|
| `priorityScore` | Packs `(PRIORITY_OFFSET - priority) * 2^32 + seq` for the prioritized ZSET. |
| `enqueue` | Adds a job to `prioritized` at its score and arms the marker. |
| `lockAndLoad` | Sets the lock token and loads the hash for a just-claimed id. |
| `acquireNext` | Pops the top prioritized job into `active` and locks it, honoring the rate limit. |
| `tryRateLimit` | Token bucket: ms until a token frees, or 0 to proceed. |
| `recordFinished` | Records a terminal job in `completed`/`failed` and applies auto-removal. |

And the scripts themselves:

| Script | Caller | Does |
|---|---|---|
| `ADD_JOB` | producer | Mint/accept an id, write the hash, enqueue or delay, dedup, publish `added`. |
| `MOVE_TO_ACTIVE` | worker wakeup | Claim the next job: `ZPOPMIN prioritized` → `active` → lock + load. |
| `MOVE_TO_COMPLETED` | worker finish | Commit the result and fetch-next in one round trip. |
| `MOVE_TO_FAILED` | worker finish | Retry (to `wait`/`delayed`) or terminally fail, and fetch-next. |
| `EXTEND_LOCK` | renewer | Token-guarded lock renewal; clears the job from `stalled`. |
| `MOVE_STALLED` | sweep | Mark-and-sweep recovery of jobs whose lock expired. |
| `PROMOTE_DELAYED` | promote loop | Move up to `PROMOTE_BATCH` (1000) due delayed jobs to `prioritized`. |
| `ADD_SCHEDULED` | scheduler | Enqueue a scheduler occurrence under a deterministic id (idempotent). |
| `PROMOTE_JOB` / `RETRY_JOB` / `REMOVE_JOB` | dashboard | Run a delayed job now / re-enqueue a failed one / delete a job with its lock and logs. |

### Lua → Python return protocol

Scripts signal outcomes with sentinels the worker decodes:

- `RL_SENTINEL` (`"__rl__"`) — a claim hit the rate limiter; the second value is
  ms until a token frees, so the worker waits instead of busy-spinning.
- `LOCK_LOST` (`-2`) — a finish ran but the worker no longer held the lock (the
  job was reclaimed); the result is dropped.
- `NOT_ACTIVE` (`-3`) — a finish ran but the job was no longer in `active`.
- `OUTCOME_FAILED` (`1`) vs `0` — `MOVE_TO_FAILED` telling the worker whether the
  job terminally failed or will retry.

Scores are packed under 2^53 (`PRIORITY_OFFSET = 2^20`, `SEQ_MOD = 2^32`) so ZSET
double scores stay exact, and the scripts use only plain JSON and integer ARGV —
no `cmsgpack` / `bit` / `cjson` — so they run on any Redis build.

## Python-specific choices

- **async-first** — `redis.asyncio`, `async def` processors, one event loop;
  concurrency is N `asyncio` tasks sharing the loop.
- **Cluster** — a `{braces}` hash-tag in the prefix keeps all of a queue's keys on
  one slot, which the multi-key Lua scripts require.
