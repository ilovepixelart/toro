"""The queue-wide limits a claim enforces: a cap on the jobs active at once and a
token bucket over job starts, both shared by every worker on the queue.

They are the queue's own, set with `Queue.set_limits()` and read by the claim script
from the queue's `meta` hash, so they apply to every worker at once. A worker's
`global_concurrency` and `rate_limit` arguments apply only to a queue whose limits
were never set.
"""

from __future__ import annotations

from typing import TypedDict

from .job import _whole


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

    Whole numbers of at least 1, like every other option (`_whole`): a string was
    coerced, a fractional value truncated and a bool read as 1, and `set_limits`
    then stored the coerced value for every worker on the queue.
    """
    # int(): an int subclass (an IntEnum) passes `_whole` and would reach Redis as its
    # repr, which Lua reads as no number at all.
    cap = (
        0
        if global_concurrency is None
        else int(_whole(global_concurrency, "global_concurrency", minimum=1))
    )
    if rate_limit is None:
        return cap, 0, 0
    return (
        cap,
        int(_whole(rate_limit.get("max"), "rate_limit max", minimum=1)),
        int(_whole(rate_limit.get("duration"), "rate_limit duration (ms)", minimum=1)),
    )
