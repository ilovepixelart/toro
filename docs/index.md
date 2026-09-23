# toro documentation

Reference docs for how toro works. The [README](../README.md) is the quick start.

## Pages

- **[Concepts](concepts.md)** - the mental model: queues, workers, jobs, the eight
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
- **[Flows](flows.md)** - parent/child job trees: fan-out/fan-in, failure
  policies, flow-aware retry and removal.
- **[Architecture](architecture.md)** - the atomic-Lua core and the design
  decisions behind the queue.
- **[Operating](operating.md)** - what to scrape, what the numbers mean, and a
  dashboard that can be read-only.
- **[Scaling](scaling.md)** - the process model, slots against replicas, what the
  loop and the enqueue path are actually worth, measured.
- **[Security](security.md)** - what toro guarantees (JSON-only, no dynamic
  dispatch, no string-built commands) and what you own (Redis access, secrets).
- **[Versioning](versioning.md)** - what is public, what semver means here, and the
  data model's own version.
- **[Coming from another queue](migrating.md)** - the vocabulary map, and what has
  no equivalent here.
- **[FAQ](faq.md)** - the questions with uncomfortable answers, starting with "why
  not just use Postgres".
- **[Upgrading](upgrading.md)** - breaking changes by release, each with what to
  do about it.

## Design notes

- **[Flows (design)](flows-design.md)** - the decisions and landscape lessons
  behind the flows feature.
