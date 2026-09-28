"""Integration: the tuned Redis connection factory used by Queue/Worker."""

import asyncio
import hashlib

import pytest
from redis.exceptions import TimeoutError as RedisTimeoutError

from toro import Queue, scripts
from toro.connection import connect

PREFIX = "torotest"


async def test_connect_pings_and_applies_tuning():
    r = connect("redis://localhost:6379")
    try:
        assert await r.ping() is True
        kw = r.connection_pool.connection_kwargs
        assert kw.get("health_check_interval") == 30  # recycle half-open idle conns
        assert kw.get("socket_keepalive") is True
    finally:
        await r.aclose()


async def _swallow_first_reply_to(sha: str, upstream_port: int = 6379):
    """A TCP proxy to Redis that drops the reply to the first EVALSHA of `sha`:
    the command runs on the server, and the client never hears back."""
    swallowed = {"done": False}

    async def pipe(reader, writer, state, from_client):
        try:
            while data := await reader.read(65536):
                if from_client and not swallowed["done"] and sha.encode() in data:
                    swallowed["done"] = True
                    state["swallow"] = True
                elif not from_client and state.get("swallow"):
                    state["swallow"] = False  # this is the reply nobody will hear
                    continue
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    async def handle(client_reader, client_writer):
        server_reader, server_writer = await asyncio.open_connection("localhost", upstream_port)
        state: dict[str, bool] = {}
        await asyncio.gather(
            pipe(client_reader, server_writer, state, True),
            pipe(server_reader, client_writer, state, False),
        )

    return await asyncio.start_server(handle, "localhost", 0)


async def test_a_reply_that_times_out_does_not_run_the_script_twice(q):
    """Only the reply was lost: the job was enqueued. Re-sending ADD_JOB on the read
    timeout enqueued it a second time, so one add() made two jobs. The caller now
    hears the timeout instead, and the queue holds the one job."""
    sha = hashlib.sha1(scripts.ADD_JOB.encode()).hexdigest()  # noqa: S324 - Redis names scripts by SHA1
    proxy = await _swallow_first_reply_to(sha)
    port = proxy.sockets[0].getsockname()[1]
    conn = connect(f"redis://localhost:{port}", blocking_timeout=0.1)
    proxied = Queue(q.name, prefix=PREFIX, connection=conn)
    # Cached first: an EVALSHA the server answers NOSCRIPT enqueues nothing, and the
    # proxy would swallow that reply just the same, so the file run alone (or after
    # SCRIPT FLUSH) would see one timeout and zero jobs.
    await q.redis.script_load(scripts.ADD_JOB)
    try:
        with pytest.raises(RedisTimeoutError):
            await proxied.add("once", {})
        assert (await q.counts())["wait"] == 1
    finally:
        await proxied.close()
        await conn.aclose()
        proxy.close()
