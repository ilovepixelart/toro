# Export what a queue is doing, and let a dashboard be read-only

## Problem and outcome

toro records per-minute counters and serves them to its own dashboard, and that is
the end of it. Teams alert in the metrics stack they already run, so the artifact
they ask a queue for is an exporter, not another threshold engine. Two things stop
them today: there is no scrape endpoint, and every per-minute bucket self-expires
after eight hours, so a counter cannot answer "how many since forever", which is
what `rate()` needs to survive a restart.

Separately, a dashboard that can pause a queue and delete jobs cannot be shared
with people who should only look at it.

Outcome: a queue exposes OpenMetrics text that a Prometheus-compatible scraper can
read, backed by totals that do not expire, so `rate()` is correct across a restart.
matador serves it from its mount, and can be run read-only, with the controls gone
from the page rather than merely refused.

## Design

- **Totals live beside the buckets, in the same script.** The per-minute buckets stay
  as they are (self-expiring, what the dashboard charts). A single non-expiring hash
  gains the same increments in the same atomic step, so a counter can never disagree
  with the transition it counts. That is the existing rule for metrics and it is not
  being relaxed for this.
- **Counters only, and monotonic.** A counter that can go backwards breaks `rate()`,
  so nothing is derived from a set's cardinality. Gauges (queue depth per state) are
  read at scrape time, which is what a gauge means.
- **The exporter is a reader, not a collector.** `Queue.metrics_text()` renders from
  what is already in Redis: no background task, no in-process state, so N replicas
  scraped independently report the same numbers.
- **Read-only is a predicate, not a flag.** `can_mutate(request)` receives the raw
  request, because the mounting app owns identity and matador has none of its own. One
  guard covers every mutating route, and the templates ask the same predicate, so a
  control that cannot be used is not drawn.
- **The guard is derived from the route table**, not a hand-listed set: a route added
  later is covered by construction, which a list would not be.

## Acceptance clauses

| ID | Behavior | Check |
|---|---|---|
| OP-001 | `metrics_text()` renders valid OpenMetrics: `# TYPE`/`# HELP` per family, one sample per queue, and a parser reads it back. | `tests/unit/test_metrics_text.py` |
| OP-002 | The totals survive a restart: a counter scraped, the queue's process restarted, scraped again, never decreases and counts the work done in between. | `tests/integration/test_metrics_export.py::test_totals_survive_a_restart` |
| OP-003 | Totals are written in the same atomic step as the transition they count, so a counter can never disagree with the state change. | `::test_a_total_cannot_disagree_with_its_transition` |
| OP-004 | A cancellation is counted as a cancellation, not a failure, in the export as everywhere else. | `::test_the_export_keeps_cancellations_out_of_failures` |
| OP-005 | Gauges read current depth per state, including `held` and `cancelled`. | `::test_depth_gauges_cover_every_state` |
| OP-006 | matador serves `/metrics` from its mount, in the scraper's content type. | `tests/integration/test_metrics_endpoint.py` |
| OP-007 | In read-only mode every mutating route refuses, and the set is derived from the route table so a new route is covered without being listed. | `tests/integration/test_read_only.py::test_every_mutating_route_refuses` |
| OP-008 | In read-only mode the controls are absent from the markup, not merely refused when clicked. | `::test_controls_are_not_drawn` |
| OP-009 | `can_mutate` receives the request, so the host app can allow some callers and not others. | `::test_the_predicate_sees_the_request` |
| OP-010 | Scraping costs one round trip per queue and nothing on the hot paths. | measured, as in `cancel.md` |

## Out of scope

- A bespoke alerting or threshold engine. Teams alert in Grafana or Datadog; this
  ships the numbers, not the opinions.
- Per-job or per-name metric families. The cardinality is unbounded by design.
- Authentication. matador has no identity of its own; `can_mutate` is where the host
  app's identity arrives.

## Risks

- **A totals hash is a key that never expires.** It is one hash per queue with a fixed
  set of fields, so it is bounded by the number of queues, not by traffic.
- **Adding writes to the finish scripts costs on the hot path.** Measured, and the
  same clause that governed the cancel channel governs this: no measurable cost.

## Tasks

| # | Clause | Work | Files | Test strategy |
|---|---|---|---|---|
| 1 | OP-003 | The totals hash, incremented in the metrics routine | `toro/scripts.py` | red first |
| 2 | OP-001, OP-004, OP-005 | `metrics_text()` and its families | `toro/queue.py` | unit, red first |
| 3 | OP-002 | Restart survival | | integration |
| 4 | OP-006 | `/metrics` from the mount | `matador/app.py` | red first |
| 5 | OP-007, OP-008, OP-009 | `can_mutate`, the guard, the templates | `matador/app.py`, templates | red first |
| 6 | OP-010 | Throughput with and without | bench | before and after |
| 7 | | Docs: an operating page, the data model, upgrading | `docs/` | review |
| 8 | | Prove: full suite, mutation audit, adversarial review | | evidence |
