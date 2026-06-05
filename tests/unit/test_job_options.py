"""Unit: JobOptions — defaults, (de)serialization, and the auto-removal mapping."""

import pytest

from toro.job import JobOptions


def test_defaults():
    o = JobOptions()
    assert (o.delay, o.attempts, o.priority) == (0, 1, 0)
    assert o.backoff is None
    assert o.remove_on_complete is None
    assert o.remove_on_fail is None


def test_to_dict_from_dict_roundtrip():
    o = JobOptions(
        delay=500,
        attempts=3,
        backoff={"type": "exponential", "delay": 100},
        priority=5,
        remove_on_complete=1000,
        remove_on_fail={"age": 3600},
    )
    assert JobOptions.from_dict(o.to_dict()) == o


def test_from_dict_tolerates_missing_keys():
    assert JobOptions.from_dict({}) == JobOptions()


@pytest.mark.parametrize(
    "opt, expected",
    [
        (None, (-1, -1)),  # keep all (default)
        (False, (-1, -1)),  # keep all
        (True, (0, -1)),  # remove immediately
        (1000, (1000, -1)),  # keep newest 1000
        ({"count": 500}, (500, -1)),  # keep newest 500
        ({"age": 3600}, (-1, 3600)),  # keep within 1h
        ({"age": 3600, "count": 500}, (500, 3600)),  # both bounds
        ({}, (-1, -1)),  # empty dict = keep all
        ("nonsense", (-1, -1)),  # unknown type = safe default
    ],
)
def test_keep_args_maps_every_removal_form(opt, expected):
    assert JobOptions.keep_args(opt) == expected


def test_keep_args_distinguishes_true_from_one():
    # bool is an int subclass — guard that True/1 don't collapse together.
    assert JobOptions.keep_args(True) == (0, -1)
    assert JobOptions.keep_args(1) == (1, -1)
