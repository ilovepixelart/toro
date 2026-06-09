# toro — design & architecture

`toro` is an async-first, Redis-backed job queue for Python. This doc describes
how it works and the reasoning behind the core choices.

> Prior art: the design draws on the proven Redis-queue patterns popularized by
> the Node.js ecosystem (atomic Lua transitions, reliable blocking pop, lock +
> stalled recovery). Credit where due — but the rest of this doc describes
> toro's own design.

## Core principles

### 1. Atomic state transitions via Lua
Every state move (`wait→active`, `active→completed/failed/delayed`,
`delayed→wait`) is a single Redis Lua script. Redis runs each script atomically,
so multi-key "check-then-act" sequences can't interleave. This kills three
classes of race:
- **pop-then-lock gap** — two workers both grabbing the same job.
- **finish-after-steal** — a worker committing a result for a job a stalled-sweep
  already re-queued (guarded with a token check + `LREM active` returning 0).
- **priority insertion vs concurrent consume** — `LINSERT` position computed and
  used in one snapshot.

Scripts live in `scripts.py`, registered via `redis.asyncio`'s `register_script`.
The Python side only assembles KEYS/ARGV — the guarantees live in the Lua.

### 2. Reliable fetch: blocking pop on a dedicated connection
The worker pops `wait→active` with `BLMOVE` (the non-deprecated `BRPOPLPUSH`) so
the job is on `active` *before* the worker starts. If the worker dies mid-job,
the id is still on `active` and the stalled sweep (principle 3) recovers it. The
blocking call should use a **separate** Redis connection, since a blocked
connection can't issue other commands.

→ **toro:** the `wait` list became a single `prioritized` ZSET (see *Priorities*),
so the blocking wake is `BZPOPMIN` on a 0-scored base **marker** and the claim is
the atomic `MOVE_TO_ACTIVE` (`ZPOPMIN prioritized` → `active` → lock + load). A
missed marker can't strand a job — the claim is atomic and idempotent. The
blocking pop still shares the worker's connection; a dedicated connection is a
possible refinement.

### 3. Locks + token + stalled recovery — the at-least-once guarantee
- On move-to-active, set `<q>:<id>:lock = <token>  PX lockDuration` (default 30s).
  The token is a per-worker UUID — only the owner can renew.
- A renewer extends the lock every `lockDuration/2` (token-guarded `GET==token`
  then re-`SET PX`); a successful renew also `SREM`s the job from the `stalled`
  set.
- A sweep runs every `stalledInterval` (~30s), throttled cluster-wide by a
  `stalled-check` PX key. It is **mark-and-sweep**: sweep the `stalled` set (any
  member whose `:lock` is gone → `LREM active`, push back to `wait`, or fail if
  `stalledCounter > maxStalledCount`, default 1), then re-`SADD` the current
  `active` list into `stalled`. Live workers renew and remove themselves before
  the next sweep; only genuinely dead jobs remain.

**Guarantee:** at-least-once. A job is never lost while Redis persists, but its
handler may run more than once (bounded by `maxStalledCount`). Exactly-once
*result commit* is enforced by the token-guarded lock at finish time, not by
preventing duplicate handler execution.

→ **toro:** ✅ implemented as described — a per-job `<id>:lock = <token>`, a
lock-renewal task per in-flight job, and a background `_stalled_loop` running the
`MOVE_STALLED` mark-and-sweep script every `stalled_interval`. Tune it with
`Worker(stalled_interval=, max_stalled_count=, lock_duration=, renew_locks=)`;
`examples/stalled.py` demonstrates exactly-once result commit when a worker dies
mid-job.

### 4. One delay timer, pub/sub-woken
Delayed jobs live in a ZSET scored `(ts << 12) | (counter & 0xFFF)` — low 12 bits
keep FIFO order among same-ms jobs. Arm a single timer for the next due job
(capped by a guard interval) and re-arm it instantly when a sooner job is added,
via a pub/sub message on the `delayed` channel. A promotion script moves all due
jobs to `wait`.

→ **toro:** currently a 1s poll in `_promote_loop` (simple, correct, slightly
wasteful). Upgrade path: single `asyncio` timer re-armed via a pub/sub
subscriber. Also adopt the packed score for correct same-ms ordering.

### 5. Fetch-next-in-finish  ✅ done
The finish script commits the current job and pops + locks the next one in the
same round trip — skipped when shutting down (fetch flag), so the queue drains
cleanly.

→ **toro:** implemented. All job acquisition funnels through one shared Lua
routine (`lockAndLoad` / `acquireNext` in `scripts.py`), used by both the
blocking-wakeup path (`MOVE_TO_ACTIVE`) and the fetch-next tail of
`MOVE_TO_COMPLETED` / `MOVE_TO_FAILED`. This routine is the seed of a future
`moveToActive`: to add priorities/markers we change only *which* job
`acquireNext` picks — `lockAndLoad` and every caller stay untouched.

**Measured:** process throughput went ~6,080 → ~13,900 jobs/s (~2.3×) at
concurrency 20. Notably cmds/job barely changed (13 → 12) — the win is fewer
*round trips* (the old `BLMOVE` + `MOVE_TO_ACTIVE` + `HGETALL` per job collapse
into the finish call), not less server work.

## Higher-level features

- **Priorities:** ✅ done — and we deliberately *diverge* from the common approach. The usual design
  inserts into `wait` with `LINSERT` (O(N)) and keeps a separate `wait`
  fast-lane that strictly beats `prioritized` (priority-0 jobs can starve
  prioritized ones). toro instead puts **every** job in one `prioritized` ZSET
  scored `(PRIORITY_OFFSET - priority) * 2^32 + seq` — a single GLOBAL order,
  higher priority = more urgent, FIFO within a level, no fast lane. Because a
  single ZSET can't be `BLMOVE`'d, this brings in the base marker: producers
  `ZADD marker 0 "0"` (idempotent), idle workers `BZPOPMIN marker`, and the
  atomic `MOVE_TO_ACTIVE` does `ZPOPMIN prioritized` → active → lock. The 1s
  delay poll still promotes delayed → prioritized (delay-marker is future work).
- **Repeatable/cron:** ✅ done — `add_scheduler(every=ms | cron=...)` validates the
  schedule (`croniter` for cron) and enqueues the first occurrence as a delayed
  job; each occurrence mints its successor with a deterministic id when a worker
  picks it up. `trigger_scheduler` runs one now, `remove_scheduler` stops it.
- **Rate limiting:** ✅ done — a queue-wide token bucket in Redis
  (`Worker(rate_limit={"max": N, "duration": ms})`), shared by every worker on the
  queue. An over-limit claim returns a sentinel and the worker waits out the retry
  window instead of busy-spinning.
- **Events:** ✅ done — Redis pub/sub on an `events` channel: `add`, progress,
  `completed` and `failed` publish; `Queue.result()` subscribes and awaits the
  terminal event, and `Worker.on(event, fn)` exposes lifecycle hooks. Streams
  (replay + consumer groups) remain a possible upgrade.
- **Auto-removal:** ✅ done — `remove_on_complete` / `remove_on_fail`
  (bool / count / `{count, age}`) enforced inside the finish script, not by a sweeper.

## Python-specific choices
- **async-first**: `redis.asyncio`, `async def` processors, one event loop.
  Concurrency = N `asyncio` tasks sharing the loop.
- **Cluster:** use a `{braces}` hash-tag prefix so all keys for a queue share a
  slot (multi-key Lua needs one slot).
- **Lua deps:** plain JSON, integers passed as ARGV — keeps scripts portable
  across Redis builds (no `cmsgpack`/`bit`/`cjson` requirement).
