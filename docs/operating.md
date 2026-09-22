# Operating a queue

What to scrape, what the numbers mean, and where they come from.

## Scraping

```python
text = await queue.metrics_text()          # one queue, OpenMetrics
totals = await queue.lifetime_totals()     # just the counters
```

[matador](https://github.com/ilovepixelart/matador) serves every queue it watches
from one `/metrics` endpoint. Serve it yourself with any framework: the text is the
whole response, and the content type is
`application/openmetrics-text; version=1.0.0; charset=utf-8`.

| Family | Type | Labels | Meaning |
|---|---|---|---|
| `toro_jobs_total` | counter | `queue`, `outcome` | Jobs by outcome since the queue was created. |
| `toro_job_duration_ms_total` | counter | `queue` | Processing time of finished jobs, in ms. |
| `toro_queue_depth` | gauge | `queue`, `state` | Jobs currently in each state. |

`outcome` is `added`, `completed`, `failed` or `cancelled`. **A cancellation is never
counted as a failure**: that is the point of it being a state of its own, and a
failure rate that includes deliberate stops is the thing this avoids.

## Why the counters never reset

The per-minute buckets a dashboard charts self-expire after eight hours. That is
right for a chart and useless for `rate()`, which reads a counter across restarts: a
counter that resets looks like a cliff, and the recovery looks like a spike that
never happened. The totals live in one non-expiring hash per queue, bounded by its
field list rather than by traffic.

Counters are exported even at zero, for the same reason: a family that first appears
when a job fails makes `rate()` start from nothing at that instant.

## What it costs

The totals are written in the same atomic step as the transition they count, so a
scraped counter can never disagree with the state change. That is two more Redis
commands per job (30.4 to 32.5 measured over 2,000 jobs), all of them inside scripts
that already run, so there are no extra round trips.

Scraping itself reads Redis at scrape time and keeps no state in the process, so N
replicas scraped independently report the same numbers and none of them has to be
running for the figures to be right.

## A read-only dashboard

matador takes a `can_mutate(request)` predicate. It receives the raw request, because
the dashboard has no identity of its own and whatever the host app authenticates with
is what arrives:

```python
app.mount("/queues", create_app(["emails"], can_mutate=lambda r: r.user.is_admin))
```

Every state-changing request is refused, and the controls are not drawn at all: a
button that exists and refuses invites the click and reports a failure that was never
one. See matador's [views](https://github.com/ilovepixelart/matador/blob/main/docs/views.md).
