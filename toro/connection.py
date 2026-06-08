"""Redis connection factory with sane defaults for toro's long-lived clients."""

from __future__ import annotations

import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError


def connect(url: str) -> aioredis.Redis:
    """Open a ``decode_responses`` client tuned for toro's long-lived connections.

    Connections that sit idle for a while (a worker's blocking pop, the result()
    pub/sub, mostly-idle producers) get:

    * ``health_check_interval`` + ``socket_keepalive`` to recycle half-open
      connections a NAT/load-balancer idle timeout silently dropped, instead of
      failing the next real command.
    * a ``Retry`` policy so a transient reconnect is invisible to the worker loops,
      rather than surfacing as an exception they'd swallow into a skipped iteration.
    """
    return aioredis.from_url(
        url,
        decode_responses=True,
        health_check_interval=30,
        socket_keepalive=True,
        retry=Retry(ExponentialBackoff(), retries=3),
        retry_on_error=[RedisConnectionError, RedisTimeoutError],
    )
