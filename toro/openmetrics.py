"""Rendering what a scraper reads.

A pure function of the numbers: no Redis, no clock, no process state, so N replicas
scraped independently render the same text from the same data, and every rule of the
format is checkable without a queue in the loop.
"""

from __future__ import annotations

from collections.abc import Mapping

# Counters are declared even at zero. A family that appears only once a job has
# failed makes `rate()` start at a cliff, which reads as a spike that never happened.
OUTCOMES = ("added", "completed", "failed", "cancelled")


def _label(value: str) -> str:
    """Escape a label value. A queue name is a Redis key segment, not a vetted
    identifier, and an unescaped quote or newline would end the sample early and
    corrupt every line after it.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render(queue: str, totals: Mapping[str, int], depths: Mapping[str, int]) -> str:
    """OpenMetrics text for one queue: lifetime counters and current depth."""
    name = _label(queue)
    out = [
        "# TYPE toro_jobs_total counter",
        "# HELP toro_jobs_total Jobs by outcome since the queue was created.",
    ]
    out += [
        f'toro_jobs_total{{queue="{name}",outcome="{outcome}"}} {int(totals.get(outcome, 0))}'
        for outcome in OUTCOMES
    ]
    out += [
        "# TYPE toro_job_duration_ms_total counter",
        "# HELP toro_job_duration_ms_total Processing time of finished jobs, in ms.",
        f'toro_job_duration_ms_total{{queue="{name}"}} {int(totals.get("ms", 0))}',
        "# TYPE toro_queue_depth gauge",
        "# HELP toro_queue_depth Jobs currently in each state.",
    ]
    # Depth is read at scrape time, which is what a gauge means: it can go down, so it
    # must never be derived from a counter.
    out += [
        f'toro_queue_depth{{queue="{name}",state="{_label(state)}"}} {int(count)}'
        for state, count in depths.items()
    ]
    out.append("# EOF")
    return "\n".join(out) + "\n"
