"""Redis connection factory with sane defaults for toro's long-lived clients."""

from __future__ import annotations

from typing import Any

import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

# How far a connection's read timeout sits above the longest blocking pop it serves.
READ_MARGIN = 5.0


def connect(
    url: str, *, max_connections: int = 50, blocking_timeout: float | None = None
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
    redis-py 8 defaults the read timeout to 5s, which a 5s pop does not beat.
    """
    extra: dict[str, Any] = {}
    if blocking_timeout is not None:
        extra["socket_timeout"] = blocking_timeout + READ_MARGIN
    pool = aioredis.BlockingConnectionPool.from_url(
        url,
        max_connections=max_connections,
        decode_responses=True,
        health_check_interval=30,
        socket_keepalive=True,
        retry=Retry(ExponentialBackoff(), retries=3),
        retry_on_error=[RedisConnectionError, RedisTimeoutError],
        **extra,
    )
    return aioredis.Redis(connection_pool=pool)


def read_timeout(client: aioredis.Redis) -> float | None:
    """Return the read timeout a connection from this client's pool will really use.

    Read off a connection built from the pool's own class and kwargs, so the
    library default counts: it is absent from ``connection_kwargs`` unless the
    caller passed it. Building the object opens nothing.
    """
    pool = client.connection_pool
    return pool.connection_class(**pool.connection_kwargs).socket_timeout
