"""Unit: Worker constructor options - validated at construction, before any I/O."""

import pytest

from toro import Worker


async def _noop(job):
    return None


@pytest.mark.parametrize("bad", [0, -1, 2.5, "3", True])
def test_global_concurrency_validation(bad):
    with pytest.raises(ValueError, match="global_concurrency"):
        Worker("q", _noop, global_concurrency=bad)


def test_global_concurrency_stored():
    assert Worker("q", _noop).global_concurrency == 0  # unset = no cap, like rl_max
    assert Worker("q", _noop, global_concurrency=3).global_concurrency == 3
