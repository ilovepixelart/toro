# toro documentation

Reference docs for how toro works. The [README](../README.md) is the quick start.

## Pages

- **[Concepts](concepts.md)** - the mental model: queues, workers, jobs, the five
  job states, and the difference between *workers* and *slots*.
- **[Data model](data-model.md)** - the exact Redis keys a queue uses and what
  each one stores.
- **[Reliability](reliability.md)** - the at-least-once guarantee: per-job locks,
  worker tokens, and stalled-job recovery.
- **[Producing jobs](producing.md)** - `Queue.add()` and every option (priority,
  delay, retries/backoff, deduplication, custom ids).
- **[Processing jobs](processing.md)** - `Worker`: concurrency, lifecycle events,
  rate limiting, and graceful shutdown.
- **[Scheduling](scheduling.md)** - repeatable and cron jobs, and how each
  occurrence schedules the next.
- **[Architecture](architecture.md)** - the atomic-Lua core and the design
  decisions behind the queue.
- **[Security](security.md)** - what toro guarantees (JSON-only, no dynamic
  dispatch, no string-built commands) and what you own (Redis access, secrets).
