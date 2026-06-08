"""Integration: the tuned Redis connection factory used by Queue/Worker."""

from toro.connection import connect


async def test_connect_pings_and_applies_tuning():
    r = connect("redis://localhost:6379")
    try:
        assert await r.ping() is True
        kw = r.connection_pool.connection_kwargs
        assert kw.get("health_check_interval") == 30  # recycle half-open idle conns
        assert kw.get("socket_keepalive") is True
    finally:
        await r.aclose()
