# Reliability

toro's guarantee is **at-least-once**: a job is never lost while Redis persists,
but its handler can run more than once (bounded) if a worker dies mid-job.
Exactly-once *result commit* is enforced separately, by a per-job lock token.
This page is the full story behind that sentence.

## The per-job lock

When a worker claims a job, the claim script sets a lock beside the job hash:

```
<prefix>:<name>:<jobId>:lock = <worker token>   (PX lock_duration, default 30s)
```

The token is unique to the claiming worker (a random id minted at startup). The
lock is a *lease*, not a mutex: it expires on its own if nobody renews it, which
is exactly what turns a dead worker's jobs back into runnable ones.

While the job runs, a per-job **renewer** task extends the lock every
`lock_renew_time` (default `lock_duration / 2`) and clears the job from the
`stalled` candidate set. Renewal is token-guarded — a worker can never renew a
lock another worker has since taken over. If a renewal finds the token gone, the
worker emits `lock-lost` and stops touching the job.

## Stalled recovery: mark and sweep

A worker that dies (OOM, SIGKILL, machine loss) can't clean up after itself, so
every worker runs a recovery sweep every `stalled_interval` (default 30s),
throttled by a shared `stalled-check` key so the whole fleet sweeps about once
per interval, not once per worker:

1. **Sweep**: for every id in the `stalled` set whose lock has *expired*, remove
   it from `active` and decide its fate: if its `stalledCounter` exceeds
   `max_stalled_count` (default 1) it terminally fails with
   `"job stalled more than allowable limit"`; otherwise it goes back into the
   prioritized set at its stored priority and will run again.
2. **Mark**: every id currently in `active` is written to the `stalled` set,
   becoming a candidate for the *next* sweep.

A healthy job is marked, then unmarked by its renewer before the next sweep ever
sees it. Only a job whose worker stopped renewing — i.e. died — stays marked
with an expired lock long enough to be recovered. The whole pass is one Lua
script, so recovery can't race a finish.

## Exactly-once commit: the token-guarded finish

The handler may run more than once; the *result* is committed exactly once. The
finish scripts (`MOVE_TO_COMPLETED` / `MOVE_TO_FAILED`) begin with two guards:

- the lock must still hold **this worker's token** — otherwise the script
  returns `LOCK_LOST` (-2) and commits nothing;
- the job must still be in `active` — otherwise `NOT_ACTIVE` (-3), same result.

So when a slow worker comes back from the dead after its job was recovered and
re-run elsewhere, its late finish is dropped on the floor, with a `lock-lost`
event instead of a double commit.

## What actually happens when a worker dies

Worker A claims job 7, runs for a while, then the process is SIGKILLed:

1. A's renewer dies with it; job 7's lock expires within `lock_duration`.
2. The next sweep (any live worker) finds 7 in `stalled` with no lock, removes
   it from `active`, bumps its `stalledCounter`, and re-enqueues it.
3. Worker B claims 7 and runs it to completion. If the ghost of A's process
   somehow finishes late, the token guard rejects the commit.
4. A's presence record stops heartbeating; the dashboard prunes it and logs a
   `lost` departure (a graceful `stop()` would have logged `stopped`).

The handler ran (up to) twice; the result committed once. Worst-case extra runs
are bounded by `max_stalled_count`.

## Retries vs. stalls

Two different counters bound two different failure modes:

- `attempts_made` vs `attempts` — *your code failed*: the processor raised.
  Decided at finish time; retries re-enqueue (with [backoff](producing.md) if
  configured) until attempts run out, then the job fails with your exception.
- `stalledCounter` vs `max_stalled_count` — *the worker failed*: nobody renewed
  the lock. Decided by the sweep; bounds how many times an apparently
  worker-killing job is allowed to take a worker down with it.

## Knobs

| Worker option | Default | Meaning |
|---|---|---|
| `lock_duration` | 30000 ms | Lock lease length. Must comfortably exceed your event-loop stalls, not your job length — the renewer handles long jobs. |
| `lock_renew_time` | `lock_duration / 2` | Renewal cadence. |
| `renew_locks` | `True` | Disable only to *test* stalled recovery. |
| `stalled_interval` | 30000 ms | Sweep cadence; `0` disables the sweep entirely. |
| `max_stalled_count` | 1 | Recoveries allowed before the job is failed as a worker-killer. |
| `grace_period` | 30 s | How long `stop()` lets in-flight jobs drain before cancelling. |

## What this asks of you

- **Make handlers idempotent.** At-least-once means a crash can re-run work that
  already happened externally (an email, a charge). Use your own dedup keys for
  side effects that must not repeat.
- **Keep the event loop responsive.** The renewer is an `asyncio` task; a
  processor that blocks the loop for longer than `lock_duration` starves its own
  renewer and gets treated as dead. See [Processing jobs](processing.md).
- **Durability is Redis's.** "Never lost" holds to the strength of your Redis
  persistence (AOF/RDB) and failover setup.

The key layout behind all of this is in the [data model](data-model.md); the
scripts that implement it are listed in [Architecture](architecture.md).
