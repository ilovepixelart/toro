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

→ **toro:** uses `BLMOVE wait active RIGHT LEFT`. TODO: isolate the blocking pop
on its own connection.

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

→ **toro:** NOT YET IMPLEMENTED. Top priority after the core. Plan: per-job lock
key + token, an `asyncio` lock-renewal task per in-flight job, and a background
`_stalled_loop` running a mark-and-sweep Lua script.

### 4. One delay timer, pub/sub-woken
Delayed jobs live in a ZSET scored `(ts << 12) | (counter & 0xFFF)` — low 12 bits
keep FIFO order among same-ms jobs. Arm a single timer for the next due job
(capped by a guard interval) and re-arm it instantly when a sooner job is added,
via a pub/sub message on the `delayed` channel. A promotion script moves all due
jobs to `wait`.

→ **toro:** currently a 1s poll in `_promote_loop` (simple, correct, slightly
wasteful). Upgrade path: single `asyncio` timer re-armed via a pub/sub
subscriber. Also adopt the packed score for correct same-ms ordering.

### 5. Fetch-next-in-finish
The finish script can pop + lock the next job in the same round-trip, saving a
hop — skipped when paused/closing/rate-limited.

→ **toro:** optimization for later; correctness doesn't depend on it.

## Higher-level features (roadmap)
- **Priorities:** a `priority` ZSET + insert-time `LINSERT` into `wait`. Consume
  stays a uniform blocking pop. O(N) insert, O(1) pop.
- **Repeatable/cron:** a `repeat` ZSET of schedule entries; each occurrence
  schedules its successor as a *delayed* job with a deterministic id. Port with
  `croniter`.
- **Rate limiting:** a `PSETEX`/`INCR` counter per window (optionally per group);
  limited jobs parked in `delayed`. Disables fetch-next.
- **Events:** Redis pub/sub or Streams. Streams give replay + consumer groups,
  which suits an async dashboard.
- **Auto-removal:** `removeOnComplete`/`removeOnFail` (bool/count/{count,age})
  enforced inside the finish script, not by a sweeper.

## Python-specific choices
- **async-first**: `redis.asyncio`, `async def` processors, one event loop.
  Concurrency = N `asyncio` tasks sharing the loop.
- **Cluster:** use a `{braces}` hash-tag prefix so all keys for a queue share a
  slot (multi-key Lua needs one slot).
- **Lua deps:** plain JSON, integers passed as ARGV — keeps scripts portable
  across Redis builds (no `cmsgpack`/`bit`/`cjson` requirement).
