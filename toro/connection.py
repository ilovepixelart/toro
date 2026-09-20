"""Redis connection factory with sane defaults for toro's long-lived clients."""

from __future__ import annotations

import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

# How long an idle worker slot blocks on the marker by default. It lives here, not
# in worker.py, because every connection toro builds is sized for it: a Queue's
# connection is routinely shared with a Worker, and must hold that worker's pop.
DEFAULT_BLOCK_TIMEOUT = 5.0

# How far a connection's read timeout sits above the longest blocking pop it serves.
READ_MARGIN = 5.0


def connect(
    url: str, *, max_connections: int = 50, blocking_timeout: float = DEFAULT_BLOCK_TIMEOUT
) -> aioredis.Redis:
    """Open a ``decode_responses`` client tuned for toro's long-lived connections.

    Connections that sit idle for a while (a worker's blocking pop, the result()
    pub/sub, mostly-idle producers) get:

    * ``health_check_interval`` + ``socket_keepalive`` to recycle half-open
      connections a NAT/load-balancer idle timeout silently dropped, instead of
      failing the next real command.
    * a ``Retry`` policy so a transient reconnect is invisible to the worker loops,
      rather than surfacing as an exception they'd swallow into a skipped iteration.
    * a ``BlockingConnectionPool``, so a burst of concurrent commands past the pool
      size awaits a free connection (an async wait - the event loop keeps running)
      instead of raising MaxConnectionsError, which the default pool does.

    ``max_connections`` must exceed the count of connections held LONG-term: a
    worker parks one per process loop inside BZPOPMIN, so it sizes the pool from
    its concurrency (measured: concurrency=100 on a 50-pool starves and errors).

    ``blocking_timeout`` is the longest server-side block this client will issue.
    The read timeout is set above it: a blocking pop has to come back before the
    client gives up on the read, or it raises instead of timing out quietly.
    redis-py's own guidance for blocking commands is a ``socket_timeout`` larger
    than the block window, and its default of 5s does not beat a 5s pop. The
    default here fits a default worker, so a Queue's connection can be handed to
    one as it is.
    """
    pool = aioredis.BlockingConnectionPool.from_url(
        url,
        max_connections=max_connections,
        decode_responses=True,
        health_check_interval=30,
        socket_keepalive=True,
        socket_timeout=blocking_timeout + READ_MARGIN,
        retry=Retry(ExponentialBackoff(), retries=3),
        retry_on_error=[RedisConnectionError, RedisTimeoutError],
    )
    return aioredis.Redis(connection_pool=pool)


def read_timeout(client: aioredis.Redis) -> float | None:
    """Return the read timeout a connection from this client's pool will really use.

    Read off a connection from the pool's own factory, so the library default
    counts: it is absent from ``connection_kwargs`` unless the caller passed it.
    The factory is what a pool subclass overrides, and building the object opens
    nothing.
    """
    timeout = client.connection_pool.make_connection().socket_timeout
    return None if timeout is None else float(timeout)
