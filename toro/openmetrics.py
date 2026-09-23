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

# A counter's FAMILY carries no suffix and its SAMPLE ends in `_total`: a family
# declared as `toro_jobs_total` would need a `toro_jobs_total_total` sample, and a
# strict parser rejects the whole document over it, every other family included.
_FAMILIES = (
    ("toro_jobs", "counter", "Jobs by outcome since the queue was created."),
    ("toro_job_duration_ms", "counter", "Processing time of finished jobs, in ms."),
    ("toro_queue_depth", "gauge", "Jobs currently in each state."),
)

# queue name -> (lifetime totals, current depth per state)
Snapshot = Mapping[str, tuple[Mapping[str, int], Mapping[str, int]]]


def _label(value: str) -> str:
    """Escape a label value. A queue name is a Redis key segment, not a vetted
    identifier, and an unescaped quote or newline would end the sample early and
    corrupt every line after it.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _samples(family: str, queues: Snapshot) -> list[str]:
    out: list[str] = []
    for queue, (totals, depths) in queues.items():
        name = _label(queue)
        if family == "toro_jobs":
            out += [
                f'toro_jobs_total{{queue="{name}",outcome="{o}"}} {int(totals.get(o, 0))}'
                for o in OUTCOMES
            ]
        elif family == "toro_job_duration_ms":
            out.append(f'toro_job_duration_ms_total{{queue="{name}"}} {int(totals.get("ms", 0))}')
        else:
            # Depth is read at scrape time, which is what a gauge means: it can go
            # down, so it must never be derived from a counter.
            out += [
                f'toro_queue_depth{{queue="{name}",state="{_label(state)}"}} {int(count)}'
                for state, count in depths.items()
            ]
    return out


def render_all(queues: Snapshot) -> str:
    """OpenMetrics text for any number of queues.

    Every family is declared ONCE and its samples follow, whatever the queue count:
    a render per queue concatenated would repeat each TYPE and HELP line, which a
    parser may reject outright or silently drop the samples after.
    """
    out: list[str] = []
    for family, kind, help_text in _FAMILIES:
        out.append(f"# TYPE {family} {kind}")
        out.append(f"# HELP {family} {help_text}")
        out += _samples(family, queues)
    out.append("# EOF")
    return "\n".join(out) + "\n"


def render(queue: str, totals: Mapping[str, int], depths: Mapping[str, int]) -> str:
    """One queue's exposition."""
    return render_all({queue: (totals, depths)})
