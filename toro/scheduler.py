"""Computing the next run time for repeatable schedules.

Two modes:
  * every=ms  — fixed interval, slot-aligned to the grid (no drift / backlog burst)
  * cron="*/5 * * * *"  — cron expression (via croniter), evaluated in UTC
"""

from __future__ import annotations

from datetime import datetime, timezone


def valid_cron(cron: str) -> bool:
    """Whether `cron` parses as a cron expression — validate before storing a schedule."""
    try:
        # croniter is an optional dep, imported lazily.
        from croniter import croniter  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("cron schedules need croniter: pip install croniter") from exc
    return bool(croniter.is_valid(cron))


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
