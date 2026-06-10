"""Redis connection factory with sane defaults for toro's long-lived clients."""

from __future__ import annotations

import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError


def connect(url: str, *, max_connections: int = 50) -> aioredis.Redis:
    """Open a ``decode_responses`` client tuned for toro's long-lived connections.

    Connections that sit idle for a while (a worker's blocking pop, the result()
    pub/sub, mostly-idle producers) get:

    * ``health_check_interval`` + ``socket_keepalive`` to recycle half-open
      connections a NAT/load-balancer idle timeout silently dropped, instead of
      failing the next real command.
    * a ``Retry`` policy so a transient reconnect is invisible to the worker loops,
      rather than surfacing as an exception they'd swallow into a skipped iteration.
    * a ``BlockingConnectionPool``, so a burst of concurrent commands past the pool
      size awaits a free connection (an async wait — the event loop keeps running)
      instead of raising MaxConnectionsError, which the default pool does.

    ``max_connections`` must exceed the count of connections held LONG-term: a
    worker parks one per process loop inside BZPOPMIN, so it sizes the pool from
    its concurrency (measured: concurrency=100 on a 50-pool starves and errors).
    """
    pool = aioredis.BlockingConnectionPool.from_url(
        url,
        max_connections=max_connections,
        decode_responses=True,
        health_check_interval=30,
        socket_keepalive=True,
        retry=Retry(ExponentialBackoff(), retries=3),
        retry_on_error=[RedisConnectionError, RedisTimeoutError],
    )
    return aioredis.Redis(connection_pool=pool)
