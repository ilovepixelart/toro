"""Unit: compute_backoff - the retry-delay policy, in isolation from the Worker."""

import pytest

from toro.worker import compute_backoff


@pytest.mark.parametrize(
    "backoff, attempts_made, expected",
    [
        (None, 1, 0),  # no backoff
        (0, 1, 0),
        (5000, 1, 5000),  # fixed int ms ...
        (5000, 4, 5000),  # ... independent of attempt
        ({"delay": 1000}, 1, 1000),  # dict w/o type = fixed
        ({"type": "fixed", "delay": 1000}, 9, 1000),
        ({"type": "exponential", "delay": 1000}, 1, 1000),  # 1000 * 2^0
        ({"type": "exponential", "delay": 1000}, 2, 2000),  # 1000 * 2^1
        ({"type": "exponential", "delay": 1000}, 4, 8000),  # 1000 * 2^3
        ({"type": "exponential", "delay": 250}, 5, 4000),  # 250 * 2^4
    ],
)
def test_compute_backoff(backoff, attempts_made, expected):
    assert compute_backoff(backoff, attempts_made) == expected
