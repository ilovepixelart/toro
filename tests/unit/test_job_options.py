"""Unit: JobOptions - defaults, (de)serialization, and the key a job serializes on."""

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
