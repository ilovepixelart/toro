"""Unit: the `max` and `jitter` keys of a backoff dict are validated with the rest."""

import pytest

from toro.job import JobOptions


def test_max_and_jitter_are_kept_and_normalised():
    opts = JobOptions(backoff={"type": "exponential", "delay": 100, "max": 5000.0, "jitter": 0.3})
    assert opts.backoff == {"type": "exponential", "delay": 100, "max": 5000, "jitter": 0.3}


@pytest.mark.parametrize("bad", [0, -1, 1.5, True, "5000"])
def test_max_must_be_a_whole_number_of_milliseconds_above_zero(bad):
    with pytest.raises(ValueError, match="backoff max"):
        JobOptions(backoff={"type": "fixed", "delay": 100, "max": bad})


@pytest.mark.parametrize("bad", [-0.1, 1.1, True, "0.5", 2])
def test_jitter_must_be_a_share_between_zero_and_one(bad):
    with pytest.raises(ValueError, match="backoff jitter"):
        JobOptions(backoff={"type": "fixed", "delay": 100, "jitter": bad})
