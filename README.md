# toro 🐂

An **async-first**, Redis-backed job queue for Python. Every state transition is
an atomic Lua script; producing and processing are `asyncio` end to end.

```bash
pip install toro-queue      # the import name is `toro`
```

> Installed as **`toro-queue`** on PyPI (the name `toro` was taken), but you
> `import toro`. See [DESIGN.md](https://github.com/ilovepixelart/toro/blob/main/DESIGN.md) for the architecture and the
> at-least-once reliability model.

## Why toro

- **Async-native.** Enqueue and process with `async`/`await` — no thread pools,
  no sync bridge. A natural fit for FastAPI, aiohttp, or any asyncio app.
- **Atomic by construction.** Claims, retries, promotions and finishes are Lua
  scripts, so a job can't be lost or double-committed between two round trips.
- **At-least-once delivery.** Per-job locks + a background mark-and-sweep recover
  jobs from workers that crashed — without the visibility-timeout double-delivery
  trap of some other queues.
- **Typed.** Ships `py.typed`; the public API is fully annotated.

## Features

| | |
|---|---|
| **Enqueue** | delayed jobs, global **priorities** (FIFO within a band) |
| **Retries** | fixed or exponential **backoff**, capped attempts |
| **Schedules** | repeatable **cron** and fixed-interval (`every`) jobs |
| **Rate limiting** | queue-wide token bucket shared across all workers |
| **Dedup** | custom (idempotent) job ids + a throttle window (`{id, ttl}`) |
| **Auto-removal** | keep the last N and/or finished-within-age completed/failed |
| **Reliability** | per-job locks, lock renewal, stalled-job recovery |
| **Observability** | progress, per-job logs, lifecycle events, `await result()` |
| **Lifecycle** | pause / resume, graceful shutdown that drains in-flight jobs |
| **Dashboard** | [matador](https://github.com/ilovepixelart/matador) — a live web UI |

## Quick start

```python
import asyncio
from toro import Queue, Worker

async def main():
    queue = Queue("emails")
    await queue.add("welcome", {"to": "ada@example.com"})

    async def process(job):
        print("sending", job.data)
        return {"ok": True}

    worker = Worker("emails", process, concurrency=8)
    worker.on("completed", lambda job, result: print("done", job.id))
    await worker.run()

asyncio.run(main())
```

## A taste of the options

```python
# Priorities, delay, and retry-with-backoff
await queue.add("report", data, priority=10, delay=5000,
                attempts=5, backoff={"type": "exponential", "delay": 1000})

# Idempotent custom id (a second add with the same id is ignored)
await queue.add("charge", data, job_id="order-1234")

# A repeatable schedule (cron or every-N-ms); "run now" with trigger_scheduler
await queue.add_scheduler("nightly-rollup", cron="0 0 * * *")

# Queue-wide rate limit: at most 100 jobs / second across every worker
worker = Worker("emails", process, rate_limit={"max": 100, "duration": 1000})

# Wait for a result from the producer side
job = await queue.add("resize", {"src": "a.png"})
print(await job.result(timeout=30))
```

## Develop

Managed with [uv](https://astral.sh/uv); the Astral toolchain throughout.

```bash
uv sync                          # venv + deps + dev group
uv run ruff check .              # lint  (strict: select = ALL)
uv run ruff format .             # format
uv run ty check                  # type check
uv run pytest -m "unit or integration"   # tests (integration needs Redis on :6379)
uv run python examples/basic.py
```

The suite is a pyramid — `-m unit` (fast, no Redis), `-m integration` (Redis),
and `-m load` (the open-loop benchmark harness in `tests/load/`).

## License

[MIT](https://github.com/ilovepixelart/toro/blob/main/LICENSE)
