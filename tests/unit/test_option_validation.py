"""Unit: an option that is not what it claims is refused before anything is written.

The scripts `tonumber()` their arguments AFTER they have written, so an option of the
wrong type used to abort a script mid-way: a job deleted from `active` with its lock
gone and no state to recover it from, a dedup window pointing at a job that was never
enqueued, half a flow running while its caller was told the whole thing failed. The
validation belongs where the value arrives.
"""

import pytest

from toro import JobOptions


@pytest.mark.parametrize(
    "option",
    [
        {"delay": "soon"},
        {"delay": -1},
        {"delay": 1.5},
        {"delay": True},  # bool is an int subclass: True would silently mean 1ms
        {"attempts": "two"},
        {"attempts": 0},
        {"attempts": float("inf")},  # json.dumps writes Infinity; tonumber reads nil
        {"priority": "high"},
        {"priority": -1},
        {"backoff": "later"},
        {"backoff": -5},
        {"backoff": {"type": "quadratic", "delay": 1000}},
        {"backoff": {"type": "fixed"}},
        {"backoff": {"type": "fixed", "delay": "soon"}},
        {"remove_on_complete": "2"},  # a string silently meant "keep everything"
        {"remove_on_complete": -1},
        {"remove_on_fail": "all"},
        {"remove_on_fail": {"count": "2"}},
        {"remove_on_fail": {"age": -1}},
        {"remove_on_fail": {"count": 2, "nonsense": 1}},
    ],
)
def test_an_option_of_the_wrong_shape_is_refused(option):
    with pytest.raises(ValueError, match=next(iter(option))):
        JobOptions(**option)


@pytest.mark.parametrize(
    "option",
    [
        {"delay": 0},
        {"delay": 60_000},
        {"attempts": 1},
        {"attempts": 10},
        {"priority": 0},
        {"priority": 2**20},
        {"backoff": None},
        {"backoff": 1000},
        {"backoff": {"type": "fixed", "delay": 1000}},
        {"backoff": {"type": "exponential", "delay": 500}},
        {"remove_on_complete": None},
        {"remove_on_complete": True},
        {"remove_on_complete": False},
        {"remove_on_complete": 100},
        {"remove_on_fail": {"count": 10}},
        {"remove_on_fail": {"age": 3600}},
        {"remove_on_fail": {"count": 10, "age": 3600}},
    ],
)
def test_every_shape_the_docs_promise_is_accepted(option):
    assert JobOptions(**option) is not None


def test_the_message_says_which_option_and_what_it_wanted():
    """A ValueError from a queue is read by someone who passed a config value
    through three layers; the option's name is the only useful part."""
    with pytest.raises(ValueError, match="attempts"):
        JobOptions(attempts="two")
