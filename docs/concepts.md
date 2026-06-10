# Concepts

The mental model behind toro.

## Queue, Worker, Job

toro has a clean producer/consumer split, and both talk to the same Redis.

- A **`Queue`** is the *producer* handle. You use it to enqueue jobs
  (`queue.add(...)`), schedule repeatable ones, and inspect state (counts,
  listing, search). Creating a `Queue` opens (or shares) a Redis connection but
  starts no background work.
- A **`Worker`** is the *consumer*. You give it a queue name and an `async`
  processor function; calling `worker.run()` starts claiming jobs, running the
  processor over each, and recovering jobs from workers that died. A worker also
  runs small background loops (delayed-job promotion, stalled-job sweep,
  heartbeat) while it's alive.
- A **`Job`** is one unit of work. It carries an `id`, a `name` (a label you
  choose, e.g. `"welcome"`), a JSON-serializable `data` payload, its options, and
  bookkeeping the system fills in: `state`, `attempts_made`, timestamps
  (`timestamp`, `processed_on`, `finished_on`), `progress`, `stacktrace`, and
  either a `returnvalue` or a `failed_reason`. (A job's log lines and its lock
  live in separate Redis keys, not as fields on the `Job` — see the
  [data model](data-model.md).)

Producers and consumers never call each other. They coordinate only through
Redis, which is what lets you run them in different processes or on different
machines.

## Job states

Every job is in exactly one state at a time. toro exposes them as a `Literal`
type, `JobState`:

| State | Meaning |
|---|---|
| `wait` | Ready to run, waiting for a free worker. (Stored in the priority-ordered set, so "wait" and "prioritized" are the same place.) |
| `delayed` | Scheduled for the future; not yet runnable. Promoted to `wait` when due. |
| `active` | Claimed by a worker and currently running. |
| `completed` | Finished successfully; `returnvalue` holds the result. |
| `failed` | Exhausted its retry attempts; `failed_reason` holds the error. |

The normal path is `wait → active → completed`. A failure with retries left goes
`active → wait` (or `active → delayed`, if a backoff delay applies) and tries
again; only after the last attempt does it land in `failed`. A delayed or
repeatable job starts in `delayed`. See [Job lifecycle](architecture.md) for the
exact transitions and [Producing jobs](producing.md) for how delay and retries
are configured.

## Workers vs. slots

These are easy to conflate but distinct, and the dashboard shows both.

- A **worker** is a running `Worker` instance (one heartbeat, one identity). It
  lives inside an OS process, but it is *not* the process: you can run several
  workers in one process, one per process, or spread across machines.
- A **slot** is one unit of *parallel* work *inside* a worker. A worker created
  with `concurrency=N` runs N async processing loops, so it can have up to N jobs
  in flight at once. "Slots" on the dashboard is the sum of every live worker's
  concurrency: your total throughput capacity.

So `live` counts workers, `slots` counts concurrent capacity. With the default
`concurrency=1` they happen to match; bump concurrency and slots climb while the
worker count stays put.

```
host (machine)
└── process (pid)
    └── worker        (a Worker instance, unique id)   ← "live"
        └── slots     (concurrency async loops)        ← "slots"
            └── jobs   (one per slot at a time)
```

Because slots are `asyncio` tasks sharing one event loop (not threads or
processes), a processor that blocks the loop blocks its sibling slots. Keep
processors `await`-y.

## Events

toro publishes events to a Redis pub/sub channel: `added` when a job is enqueued
(from the producer in `Queue.add`), `progress` from a running processor
(`job.update_progress`), and `completed` / `failed`, which the finish Lua scripts
publish atomically with the state change. `failed` fires only on terminal failure,
not on a retry. Two things consume the channel:

- **`await job.result()`** (or `queue.result(job_id)`) on the producer side
  subscribes and waits for the terminal event, returning the value or raising
  `JobFailedError`.
- **A dashboard** (such as [matador](https://github.com/ilovepixelart/matador))
  subscribes to refresh live as state changes.

`Worker.on(event, fn)` lets a worker react to its own lifecycle with in-process
callbacks (`completed`, `failed`, `retrying`, `stalled`, `lock-lost`,
`rate-limited`) — separate from the pub/sub channel above. See
[Processing jobs](processing.md).

## Reliability in one sentence

toro is **at-least-once**: a job is never lost while Redis persists, but its
handler can run more than once (bounded) if a worker dies mid-job. Exactly-once
*result commit* is enforced by a per-job lock token. The full story is in
[Reliability](reliability.md).
