"""The queue-wide limits a claim enforces: a cap on the jobs active at once and a
token bucket over job starts, both shared by every worker on the queue.

They are the queue's own, set with `Queue.set_limits()` and read by the claim script
from the queue's `meta` hash, so they apply to every worker at once. A worker's
`global_concurrency` and `rate_limit` arguments apply only to a queue whose limits
were never set.
"""

from __future__ import annotations

from typing import TypedDict


class RateLimit(TypedDict):
    """The queue-wide token bucket: ``{"max": N, "duration": ms}`` - at most N
    job starts per duration, shared by every worker on the queue.
    """

    max: int
    duration: int


class Limits(TypedDict):
    """What `Queue.limits()` reads back: None for a limit that is not set."""

    global_concurrency: int | None
    rate_limit: RateLimit | None


def limit_fields(
    global_concurrency: int | None, rate_limit: RateLimit | None
) -> tuple[int, int, int]:
    """Validate the two limits and return them as the scripts read them: the cap, the
    rate's max and its duration, 0 for a limit that is not set.
    """
    if rate_limit is not None and (
        int(rate_limit.get("max", 0)) <= 0 or int(rate_limit.get("duration", 0)) <= 0
    ):
        raise ValueError("rate_limit needs {'max': positive, 'duration': positive ms}")
    # bool is an int subclass, so it is rejected by name: True would silently mean 1.
    if global_concurrency is not None and (
        isinstance(global_concurrency, bool)
        or not isinstance(global_concurrency, int)
        or global_concurrency <= 0
    ):
        raise ValueError("global_concurrency needs a positive integer")
    # int(): an int subclass (an IntEnum) would reach Redis as its repr, which Lua
    # reads as no number at all.
    return (
        int(global_concurrency or 0),
        int(rate_limit["max"]) if rate_limit else 0,
        int(rate_limit["duration"]) if rate_limit else 0,
    )
