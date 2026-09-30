"""Integration: the tuned Redis connection factory used by Queue/Worker."""

import contextlib
import hashlib
import logging

import pytest
from redis.exceptions import TimeoutError as RedisTimeoutError

from toro import FlowChild, Queue, scripts
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


async def test_a_reply_that_times_out_does_not_run_the_script_twice(q, swallow_first_reply):
    """Only the reply was lost: the job was enqueued. Re-sending ADD_JOB on the read
    timeout enqueued it a second time, so one add() made two jobs. The caller now
    hears the timeout instead, and the queue holds the one job."""
    sha = hashlib.sha1(scripts.ADD_JOB.encode()).hexdigest()  # noqa: S324 - Redis names scripts by SHA1
    proxy = await swallow_first_reply(sha)
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


async def test_an_add_re_sent_after_a_dropped_connection_makes_one_job(q, drop_link_after_first):
    """The command reached Redis and ran; the connection dropped with the reply on
    its way; the client re-sent it on a new connection, as it does for a connection
    error. Two jobs came out of one add(). The call's token now names the job it
    made, and the re-sent call is answered with that job."""
    sha = hashlib.sha1(scripts.ADD_JOB.encode()).hexdigest()  # noqa: S324 - Redis names scripts by SHA1
    await q.redis.script_load(scripts.ADD_JOB)  # cached: the replay must be the add, not a NOSCRIPT
    proxy = await drop_link_after_first(sha)
    port = proxy.sockets[0].getsockname()[1]
    conn = connect(f"redis://localhost:{port}", blocking_timeout=0.1)
    proxied = Queue(q.name, prefix=PREFIX, connection=conn)
    try:
        job = await proxied.add("once", {"n": 1})
        assert (await q.counts())["wait"] == 1
        assert [j.id for j in await q.get_jobs("wait")] == [job.id]
    finally:
        await proxied.close()
        await conn.aclose()
        proxy.close()


async def test_a_flow_re_sent_after_a_dropped_connection_makes_one_tree(q, drop_link_after_first):
    sha = hashlib.sha1(scripts.ADD_FLOW.encode()).hexdigest()  # noqa: S324 - Redis names scripts by SHA1
    await q.redis.script_load(scripts.ADD_FLOW)
    proxy = await drop_link_after_first(sha)
    port = proxy.sockets[0].getsockname()[1]
    conn = connect(f"redis://localhost:{port}", blocking_timeout=0.1)
    proxied = Queue(q.name, prefix=PREFIX, connection=conn)
    try:
        root = await proxied.add_flow("root", {}, children=[FlowChild("leaf", {})])
        assert (await q.counts())["wait"] == 1  # the one leaf
        assert (await q.counts())["waiting-children"] == 1  # the one root
        assert (await q.get_job(root.id)) is not None
    finally:
        await proxied.close()
        await conn.aclose()
        proxy.close()


# ---- the eviction policy of the Redis behind the queue -----------------------------


async def _policy(q: Queue) -> str:
    return (await q.redis.config_get("maxmemory-policy"))["maxmemory-policy"]


@contextlib.asynccontextmanager
async def _policy_set_to(q: Queue, policy: str):
    """The dev Redis under this policy for the block, and back after."""
    original = await _policy(q)
    await q.redis.config_set("maxmemory-policy", policy)
    try:
        yield
    finally:
        await q.redis.config_set("maxmemory-policy", original)


def _policy_warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if "maxmemory-policy" in r.getMessage()]


async def test_an_evicting_policy_is_warned_about_once_at_the_first_write(q, caplog):
    """An `allkeys-*` policy evicts queue keys under memory pressure, jobs and all,
    and nothing can tell an evicted job from one never added. The first write says
    so, once per connection: a dashboard opening a queue per name over one client
    is told once, not per queue."""
    async with _policy_set_to(q, "allkeys-lru"):
        with caplog.at_level(logging.WARNING, logger="toro"):
            await q.add("x", {})
            await q.add("y", {})
            another = Queue(q.name, prefix=PREFIX, connection=q.redis)
            await another.add("z", {})
    warned = _policy_warnings(caplog)
    assert len(warned) == 1, warned
    assert "allkeys-lru" in warned[0]


async def test_a_policy_that_keeps_the_queue_is_not_warned_about(q, caplog):
    """`noeviction` refuses writes instead, which surfaces as an error, and a
    `volatile-*` policy touches only keys with a TTL, which a job hash never has."""
    async with _policy_set_to(q, "volatile-lru"):
        with caplog.at_level(logging.WARNING, logger="toro"):
            await q.add("x", {})
    assert _policy_warnings(caplog) == []


async def test_a_worker_warns_at_start_on_a_connection_of_its_own(q, caplog, run_worker, run_until):
    """A worker process never writes through a Queue, so it checks at its own start."""
    async with _policy_set_to(q, "allkeys-random"):
        with caplog.at_level(logging.WARNING, logger="toro"):
            async with run_worker(q, lambda job: None):
                assert await run_until(lambda: bool(_policy_warnings(caplog)))
    warned = _policy_warnings(caplog)
    assert len(warned) == 1, warned
    assert "allkeys-random" in warned[0]
