"""Unit: JobOptions - defaults, (de)serialization, and the key a job serializes on."""

import pytest

from toro import Queue
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


def test_a_concurrency_key_round_trips():
    o = JobOptions(concurrency_key="order-42")
    assert JobOptions.from_dict(o.to_dict()) == o
    assert o.to_dict()["concurrencyKey"] == "order-42"


def test_no_concurrency_key_by_default():
    assert JobOptions().concurrency_key is None


@pytest.mark.parametrize("bad", ["", "a:b", "ctrl\x01", "\n", 7])
def test_a_key_that_could_collide_is_refused(bad):
    """Every enqueue path builds its options here, so this is the one place the rule
    has to hold: a key becomes a Redis key segment."""
    with pytest.raises(ValueError, match="concurrency_key"):
        JobOptions(concurrency_key=bad)


@pytest.mark.parametrize("name", ["", "x" * 200, "a\nb", "a\x00b"])
def test_a_job_name_that_is_not_a_label_is_refused(name):
    """A job name is a label: it is rendered on every row of a dashboard, written into
    a metrics field per distinct value, and read back in log lines. An empty one names
    nothing, and an unbounded one is a payload wearing a label's clothes."""
    q = Queue("nametest", prefix="torotest")
    with pytest.raises(ValueError, match="name"):
        q._stage_add(name, {}, job_id=None, deduplication=None, opts={})


@pytest.mark.parametrize("name", ["welcome", "email:daily", "a" * 128])
def test_an_ordinary_name_is_accepted(name):
    q = Queue("nametest", prefix="torotest")

    assert q._stage_add(name, {}, job_id=None, deduplication=None, opts={}) is not None
