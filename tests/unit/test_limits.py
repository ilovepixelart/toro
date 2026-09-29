"""Unit: the queue-wide limits are validated like every other option."""

import pytest

from toro.limits import limit_fields


@pytest.mark.parametrize(
    "rate_limit",
    [
        {"max": "5", "duration": 1000},
        {"max": 2.5, "duration": 1000},
        {"max": True, "duration": 1000},
        {"max": 5, "duration": "1000"},
        {"max": 5},
        {"max": 0, "duration": 1000},
    ],
    ids=[
        "max-a-string",
        "max-fractional",
        "max-a-bool",
        "duration-a-string",
        "no-duration",
        "max-zero",
    ],
)
def test_a_rate_limit_that_is_not_whole_numbers_is_refused(rate_limit):
    """A string was coerced, a fractional value truncated and a bool read as 1, and
    `set_limits` then stored the coerced value for every worker on the queue."""
    with pytest.raises(ValueError, match="rate_limit"):
        limit_fields(None, rate_limit)


@pytest.mark.parametrize(
    "cap", ["3", 2.5, True, 0], ids=["a-string", "fractional", "a-bool", "zero"]
)
def test_a_cap_that_is_not_a_whole_number_is_refused(cap):
    with pytest.raises(ValueError, match="global_concurrency"):
        limit_fields(cap, None)


def test_whole_floats_from_arithmetic_are_accepted():
    """`60_000 / 2` is a float that is whole, as every other option accepts."""
    assert limit_fields(6 / 2, {"max": 10.0, "duration": 60_000 / 2}) == (3, 10, 30_000)


def test_no_limits_read_as_zero():
    assert limit_fields(None, None) == (0, 0, 0)
