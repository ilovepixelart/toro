# toro 🐂

An **async-first**, Redis-backed job queue for Python. State transitions are
atomic Lua scripts; processing is `asyncio` end to end.

See [DESIGN.md](./DESIGN.md) for the architecture and reliability model.

## Status

Early scaffold. Working today:

- `Queue.add()` with `delay`, `attempts`, `backoff` (fixed / exponential)
- `Worker` with `concurrency`, atomic Lua moves, retry-with-backoff
- Delayed-job promotion, `completed`/`failed`/`retrying` events
- Introspection + admin (`get_jobs`, `retry_job`, `remove_job`) — used by
  [matador](../matador), the dashboard

Not yet (see DESIGN.md): lock + token + **stalled-job recovery** (the
at-least-once guarantee), priorities, repeatable/cron, rate limiting.

## Develop (uv)

This project is managed with [uv](https://astral.sh/uv).

```bash
uv sync                 # create .venv and install deps + dev group
uv run ruff check .     # lint
uv run pytest           # tests (needs a Redis on localhost:6379)
uv run python examples/basic.py
```

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

    worker = Worker("emails", process, concurrency=4)
    worker.on("completed", lambda job, res: print("done", job.id))
    await worker.run()

asyncio.run(main())
```
