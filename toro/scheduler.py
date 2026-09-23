"""Computing the next run time for repeatable schedules.

Two modes:
  * every=ms  - fixed interval, slot-aligned to the grid (no drift / backlog burst)
  * cron="*/5 * * * *"  - cron expression (via croniter), evaluated in UTC
"""

from __future__ import annotations

import time
from datetime import datetime, timezone


def valid_cron(cron: str) -> bool:
    """Whether `cron` is an expression this queue can actually schedule.

    Parsing is not enough: `0 0 30 2 *` (February the 30th) parses and then has no
    next date, which used to surface after the scheduler's template had been written
    and left it behind, invisible to `schedulers()`. A schedule is valid when it can
    name its next occurrence.
    """
    try:
        # croniter is an optional dep, imported lazily.
        from croniter import croniter  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("cron schedules need croniter: pip install croniter") from exc
    if not croniter.is_valid(cron):
        return False
    try:
        croniter(cron, time.time()).get_next(float)
    except Exception:
        return False
    return True


def next_run(now_ms: int, *, every: int | None = None, cron: str | None = None) -> int:
    """Next occurrence (epoch ms) strictly after now_ms."""
    if every:
        every = int(every)
        # Align to the interval grid so successive runs don't drift, and a late
        # tick catches up to the next slot instead of firing a backlog.
        return (now_ms // every + 1) * every
    if cron:
        try:
            # croniter is an optional dep, imported lazily.
            from croniter import croniter  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("cron schedules need croniter: pip install croniter") from exc
        base = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        nxt = croniter(cron, base).get_next(datetime)
        return int(nxt.timestamp() * 1000)
    raise ValueError("a schedule needs either `every` (ms) or `cron`")
