# Coming from another queue

A vocabulary map, and an honest list of what has no equivalent here.

## The words

| Elsewhere | Here |
|---|---|
| task, message | **job** |
| `@task` decorator, `.delay(...)`, `.apply_async(...)` | a processor function, and `await queue.add(name, data)` from anywhere |
| broker | Redis. There is no broker abstraction and there will not be one. |
| result backend | the job's own hash: `await queue.result(job_id)`, or `job.returnvalue` |
| routing keys, exchanges, bindings | one queue per stream of work, and `job.name` to dispatch inside a processor |
| `countdown=`, `eta=` | `delay=` (ms) |
| `max_retries`, `retry_backoff` | `attempts` and `backoff` ([Producing](producing.md)) |
| `acks_late=True` | always on: a job is locked while it runs and recovered if its worker dies ([Reliability](reliability.md)) |
| visibility timeout | `lock_duration`, renewed while the job runs. A slow job does not get delivered twice for being slow. |
| `revoke()`, `AbortableTask` | `await queue.cancel_job(job_id)`, which stops a running job's processor where it awaits |
| `group`, `chain`, `chord` | **flows**: `add_flow(...)` with children, fan-in through `job.children_results()` ([Flows](flows.md)) |
| beat, cron worker, scheduler process | `await queue.add_scheduler(...)`: the schedule lives in Redis and any worker mints the next occurrence ([Scheduling](scheduling.md)) |
| prefetch count | `concurrency`, which is how many jobs a worker runs at once. Nothing is prefetched: a slot claims one job atomically when it is free. |
| prefork pool, `--pool=threads` | one process, one event loop, N slots. A sync processor gets a thread ([Scaling](scaling.md)). |
| `rate_limit="10/s"` | `rate_limit={"max": 10, "duration": 1000}`, queue-wide across every worker |
| priority queues, `x-max-priority` | `priority=` on the job, one global order |
| unique task, `task_id` | `job_id=` for idempotency, `deduplication={"id", "ttl"}` for a throttle window |
| flower, rq-dashboard, bull-board | [matador](https://github.com/ilovepixelart/matador) |

## What has no equivalent

Said plainly, because finding out later is worse.

- **No process pool inside a worker.** toro is an async queue: N slots share one event
  loop, and a sync processor gets a thread. CPU-bound work belongs in a process you
  run yourself, or in a queue built for it.
- **No broker abstraction.** Redis, and the Redis-compatible servers that implement
  the same commands. Not RabbitMQ, not SQS, not a database table.
- **No DAGs.** Flows are trees: one parent, many children, any depth. A node with two
  parents is not expressible, deliberately ([Flows](flows.md)).
- **No task-class hooks.** No `on_failure`, no `autoretry_for`, no base class. A
  processor is a function; what it does on failure it does with `try`.
- **No task discovery or autoimport.** You wire your processor to a worker yourself,
  which means there is nothing to misconfigure at import time.
- **No CLI.** A worker is `Worker(...)` and `await worker.run()` inside your own
  entrypoint, so your logging, settings and lifecycle are the ones that apply.
- **No synchronous client.** Producing is `await queue.add(...)`. From sync code, run
  it on a loop you own (`asyncio.run`, or `run_coroutine_threadsafe` onto a loop in a
  thread).

## The two things worth changing in your head

**A job is data, not a callable.** Nothing is pickled and nothing is imported by name:
a job is a name and a JSON payload, and the worker decides what that name means. An
old job cannot summon code that no longer exists, and a queue full of jobs survives a
refactor.

**The enqueue belongs after the commit.** If you are coming from a framework hook
(`transaction.on_commit`), the equivalent is `queue.pending()`: collect during the
transaction, flush when it commits ([Producing](producing.md)).
