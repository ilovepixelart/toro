"""The ARGV the worker hands MOVE_TO_COMPLETED."""

import pytest

from toro import scripts

LIMIT = scripts.MAX_INLINE_RESULT_BYTES
LOCK_OWNER = "worker-1"  # the lock value; any string


def _inline_flag(returnvalue: str) -> str:
    args = scripts.completed_args(
        job_id="1",
        returnvalue=returnvalue,
        now=0,
        token=LOCK_OWNER,
        fetch="0",
        lock_duration=30_000,
        rl_max=0,
        rl_duration=0,
        global_concurrency=0,
    )
    return str(args[-1])


@pytest.mark.parametrize(
    ("size", "inline"),
    [(LIMIT - 1, "1"), (LIMIT, "1"), (LIMIT + 1, "0")],
    ids=["under-the-limit", "at-the-limit", "one-over"],
)
def test_a_result_rides_the_completion_event_up_to_the_limit_inclusive(size, inline):
    """A result of exactly the limit is still published with the event; one byte
    more and the event carries only the id, the waiter reading the value from the
    hash. The value is JSON with non-ASCII escaped, so its length is its size."""
    assert _inline_flag("x" * size) == inline
