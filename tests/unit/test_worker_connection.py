"""Unit: the connection a Worker builds for itself must outlast its blocking pop."""

import pytest

from toro import Worker
from toro.connection import read_timeout
from toro.worker import pop_timeout


async def _noop(job):
    return None


def test_read_timeout_exceeds_the_blocking_pop_at_defaults():
    """redis-py 8 defaults socket_timeout to 5s, the same as block_timeout. Equal is
    not enough: the client gives up on the read as the server answers it. The value
    is spelled out because conftest shrinks the default block_timeout in tests."""
    w = Worker("q", _noop, block_timeout=5.0)
    timeout = read_timeout(w.redis)
    assert timeout is None or timeout > w.block_timeout


def test_read_timeout_follows_a_longer_block_timeout():
    w = Worker("q", _noop, block_timeout=30.0)
    timeout = read_timeout(w.redis)
    assert timeout is None or timeout > 30.0


@pytest.mark.parametrize(
    ("read_timeout_s", "block_timeout", "expected"),
    [
        (None, 5.0, 5.0),  # no read timeout: nothing to stay under
        (10.0, 5.0, 5.0),  # fits with room
        (35.0, 30.0, 30.0),  # a worker-built connection: block_timeout + margin
        (5.0, 5.0, 4.0),  # redis-py 8 defaults on a caller-provided connection
        (0.4, 1.0, 0.2),  # tiny read timeout: half of it, never zero or negative
    ],
)
def test_pop_timeout_stays_under_the_read_timeout(read_timeout_s, block_timeout, expected):
    assert pop_timeout(read_timeout_s, block_timeout) == expected
