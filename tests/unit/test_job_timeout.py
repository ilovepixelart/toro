"""Unit: the `timeout` job option is validated and stored like the others."""

import pytest

from toro.job import JobOptions


def test_timeout_round_trips_through_the_stored_options():
    opts = JobOptions(timeout=1500)
    assert JobOptions.from_dict(opts.to_dict()).timeout == 1500


def test_timeout_is_unset_by_default_and_stays_unset():
    assert JobOptions().timeout is None
    assert JobOptions.from_dict({}).timeout is None


@pytest.mark.parametrize("bad", [0, -1, 1.5, True, "1000"])
def test_timeout_must_be_a_whole_number_of_milliseconds_above_zero(bad):
    with pytest.raises(ValueError, match="timeout"):
        JobOptions(timeout=bad)
