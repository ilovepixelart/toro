# Scheduling

Repeatable jobs: fixed-interval (`every`) or cron schedules that keep enqueuing
occurrences until removed.

## Registering a schedule

```python
await queue.add_scheduler("nightly-rollup", cron="0 3 * * *",
                          name="rollup", data={"scope": "all"})
await queue.add_scheduler("poll-inbox", every=30_000)
```

`add_scheduler(scheduler_id, *, every=ms | cron="...", name=None, data=None,
priority=0, **job_options)` stores the schedule's template (name, cadence, data,
options) and enqueues the first occurrence. Exactly one of `every` / `cron`.
Re-calling with the same id **updates** the schedule in place.

The `scheduler_id` is your handle (and a Redis key segment): a non-empty string
without `:` or control characters. `name` defaults to the scheduler id; `data`
and the job options (attempts, backoff, auto-removal, priority) are stamped onto
every occurrence.

## The two cadences

- **`every=ms`** — slot-aligned to the interval grid: the next run is the next
  multiple of `every` on the wall clock, not "last run + interval". Successive
  runs don't drift, and a late tick (worker down for a while) catches up to the
  *next* slot instead of firing a backlog burst.
- **`cron="*/5 * * * *"`** — a standard cron expression, evaluated in **UTC**
  via [croniter](https://pypi.org/project/croniter/) — an optional dependency
  (`pip install croniter`). Expressions are validated at `add_scheduler` time,
  so a typo fails at registration, not silently inside a worker later.

## How occurrences work

Each occurrence is an ordinary [delayed job](producing.md) with a deterministic
id:

```
repeat:<schedulerId>:<dueMillis>
```

That id makes enqueueing idempotent — the same occurrence can never exist twice,
no matter how many workers or producers race to create it.

The chain sustains itself: when a worker *first picks up* an occurrence, it
mints the successor before running the handler. The schedule therefore stays on
time regardless of how long the run takes, whether it fails, or how many retries
follow — retries re-run *that occurrence*, they never shift the cadence. If the
scheduler was removed in the meantime, no successor is minted and the chain
ends.

## Managing schedules

| Call | Does |
|---|---|
| `await queue.schedulers()` | List active schedules: id, name, cadence, next run (the dashboard's schedulers view). |
| `await queue.trigger_scheduler(id)` | Enqueue one occurrence *now*, with the schedule's stored options; the regular cadence is unaffected. |
| `await queue.remove_scheduler(id)` | Stop the schedule and drop its pending occurrence. In-flight runs finish normally. |

## Semantics worth knowing

- **At-least-once still applies.** An occurrence is a normal job: it can retry
  (per the schedule's `attempts`/`backoff`) and, on worker death, be recovered
  and re-run ([Reliability](reliability.md)). Make handlers idempotent.
- **No catch-up replay.** If every worker is down across several due times, the
  pending occurrence is promoted late and the next one lands on the current
  grid/cron slot — you get *one* late run, not a burst of missed ones.
- **One pending occurrence per schedule** exists at a time (the deterministic id
  guarantees it), so a misbehaving schedule can't flood the queue.
