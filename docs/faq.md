# Questions with honest answers

## Why not just use Postgres?

Often you should. If you already run Postgres, your volume is modest, and your jobs
are mostly "do this after the commit", then a table with `SELECT ... FOR UPDATE SKIP
LOCKED` is fewer moving parts than anything on this page, and it gives you the one
thing a separate queue cannot: the enqueue is **in** the transaction. No dual write,
no outbox, no `pending()`.

toro is the better answer when some of these are true:

- You already run Redis, so the queue adds no new operational surface.
- You want throughput in the thousands of jobs a second without tuning a database for
  a workload it was not sized for. A job costs about 43 Redis commands end to end,
  all of them scripted ([Scaling](scaling.md)).
- You want the machinery rather than the storage: per-job locks with renewal, a
  stalled-job sweep, retries with backoff, rate limits, a concurrency cap across every
  replica, keyed serialization, flows with fan-in, schedules, live events, and a
  dashboard. In Postgres each of those is yours to write and to keep correct.
- Your jobs are I/O-bound and your app is already async, which is the shape toro is
  built around.

The honest cost: Redis is a second thing to run, back up and reason about, and your
enqueue is not transactional. `queue.pending()` closes the half people actually hit
(a job about a row that was rolled back) and does not close the other half (a process
that dies between the commit and the flush). If that half matters to you, an outbox
table in Postgres is the right answer, whatever queue drains it.

## Is delivery exactly once?

No. It is **at least once**, and anything that claims otherwise is selling something.
A worker that dies with a job in hand has its lock expire, and the sweep gives the job
to someone else, because the alternative is losing it. Make processors idempotent;
[Reliability](reliability.md) says exactly when a job can run twice.

## What happens if Redis loses data?

You lose jobs. Jobs live in Redis and nowhere else, so its durability settings are
your queue's durability settings: run it with AOF (`appendfsync everysec` is the usual
compromise) and replication if a job is worth more than a second of writes.

## Can two versions of toro run at once?

Yes, one minor version apart, which is what a rolling upgrade needs. The keys carry a
data-model version and a library that finds a newer one stops rather than writing into
it ([Versioning](versioning.md)).

## Why is there no CLI?

Because the entrypoint is where your settings, logging and lifecycle live. A worker is
four lines inside your own `main()`, and nothing has to guess which module to import
or which settings object is the real one.

## Can I use it from synchronous code?

To produce, yes: run the coroutine on a loop you own. To process, no. A processor
runs on the worker's loop (a plain `def` gets a thread, which is not the same thing as
a sync worker), and a queue whose workers are async is the point of this one.

## Does it need a dashboard?

No. [matador](https://github.com/ilovepixelart/matador) is a separate package, and the
queue works, reports and cleans up with nobody looking. What the dashboard shows is
what `Queue` already exposes; it has no privileged access.

## Where do the numbers in the docs come from?

`tests/perf/harness.py` and `bench/bench.py`, both in the repo, both runnable against
a local Redis. Where a number is a ratio, it travels; where it is absolute, it is a
property of the machine that measured it and is labelled as such.
