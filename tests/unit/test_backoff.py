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


@pytest.mark.parametrize(
    "backoff, attempts_made, expected",
    [
        ({"type": "exponential", "delay": 1000, "max": 3000}, 4, 3000),  # 8000 capped
        ({"type": "exponential", "delay": 1000, "max": 3000}, 2, 2000),  # under the cap
        ({"type": "fixed", "delay": 5000, "max": 3000}, 1, 3000),  # a cap applies to fixed too
        ({"type": "exponential", "delay": 1000, "max": 60_000}, 40, 60_000),  # no overflow
    ],
)
def test_a_cap_bounds_the_delay(backoff, attempts_made, expected):
    assert compute_backoff(backoff, attempts_made) == expected


def test_jitter_adds_up_to_its_share_of_the_delay_at_random():
    backoff = {"type": "fixed", "delay": 1000, "jitter": 0.2}
    assert compute_backoff(backoff, 1, rand=lambda: 0.0) == 1000
    assert compute_backoff(backoff, 1, rand=lambda: 0.5) == 1100
    assert compute_backoff(backoff, 1, rand=lambda: 1.0) == 1200
    samples = {compute_backoff(backoff, 1) for _ in range(200)}
    assert all(1000 <= s <= 1200 for s in samples)
    assert len(samples) > 1  # random, not a constant


def test_jitter_applies_after_the_cap():
    backoff = {"type": "exponential", "delay": 1000, "max": 3000, "jitter": 0.5}
    assert compute_backoff(backoff, 4, rand=lambda: 1.0) == 4500  # 3000 capped, then +50%
