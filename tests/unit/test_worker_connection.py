"""Unit: the connection a Worker builds for itself must outlast its blocking pop."""

import logging

import pytest
import redis.asyncio as aioredis
from redis.asyncio.connection import Connection

from toro import Queue, Worker
from toro.connection import DEFAULT_BLOCK_TIMEOUT, read_timeout
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


def test_a_caller_provided_connection_on_library_defaults_is_clamped():
    """The common real case: a client built with no socket_timeout at all. The
    library default applies but never shows up in the pool's kwargs. The oracle is
    redis-py's own Connection, not toro's helper, so a helper that misses the
    default cannot simply agree with itself."""
    library_default = Connection().socket_timeout
    conn = aioredis.from_url("redis://localhost:6379", decode_responses=True)
    assert read_timeout(conn) == library_default

    w = Worker("q", _noop, connection=conn, block_timeout=30.0)
    if library_default is None:
        assert w._pop_timeout == 30.0
    else:
        assert w._pop_timeout < library_default


def test_a_socket_timeout_that_arrived_as_text_still_sizes_the_pop():
    """redis-py stores `socket_timeout` exactly as it is handed over, so a client
    built from configuration carries whatever the config layer produced, and a
    timeout read from an environment variable is a string. The arithmetic that sizes
    the blocking pop raises TypeError on one, which is a Worker that cannot be
    constructed at all: `'5' - 1.0` is not a number.
    """
    conn = aioredis.from_url("redis://localhost:6379", decode_responses=True, socket_timeout="5")
    assert read_timeout(conn) == 5.0

    w = Worker("q", _noop, connection=conn, block_timeout=5.0)
    assert w._pop_timeout == 4.0


def test_a_queue_built_connection_hosts_a_default_worker_quietly(caplog):
    """Sharing the Queue's connection with a Worker is an ordinary setup. At the
    default block_timeout it must neither shorten the pop nor log a warning: a
    default configuration that warns teaches people to ignore warnings. redis-py's
    guidance for blocking commands is a socket_timeout above the block window, so
    every connection toro builds has to be sized for toro's own default pop."""
    queue = Queue("q")
    with caplog.at_level(logging.WARNING, logger="toro.worker"):
        # spelled out: conftest shrinks the default block_timeout in tests
        w = Worker("q", _noop, connection=queue.redis, block_timeout=DEFAULT_BLOCK_TIMEOUT)
    assert w._pop_timeout == DEFAULT_BLOCK_TIMEOUT
    assert [r.getMessage() for r in caplog.records] == []


def test_a_pop_longer_than_a_shared_connection_allows_still_warns(caplog):
    """The warning stays for the case that deserves one: asking for a pop the
    connection in hand cannot hold."""
    queue = Queue("q")
    with caplog.at_level(logging.WARNING, logger="toro.worker"):
        w = Worker("q", _noop, connection=queue.redis, block_timeout=60.0)
    assert w._pop_timeout < 60.0
    assert len(caplog.records) == 1
