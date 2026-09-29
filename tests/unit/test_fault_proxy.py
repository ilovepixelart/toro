"""Unit: the TCP proxies the fault-injection tests put in front of Redis.

A reply is one RESP value, not one TCP read: the proxy that loses "the first reply"
must lose every chunk it arrives in, or the client reads the tail of the reply as an
answer of its own. On a loaded CI runner a finish's memo answer, `[1, [...], "2"]`,
came back as the bare integer 1.
"""

import asyncio

SHA = "0123456789abcdef0123456789abcdef01234567"


async def _upstream_in_two_chunks(reader, writer):
    """A stand-in server: answers the EVALSHA with an array sent in two writes."""
    await reader.read(65536)
    writer.write(b"*3\r\n")
    await writer.drain()
    await asyncio.sleep(0.05)
    writer.write(b":1\r\n$1\r\n2\r\n")
    await writer.drain()
    await asyncio.sleep(0.5)
    writer.close()


async def test_a_swallowed_reply_is_swallowed_whole(swallow_first_reply):
    upstream = await asyncio.start_server(_upstream_in_two_chunks, "localhost", 0)
    proxy = await swallow_first_reply(SHA, upstream_port=upstream.sockets[0].getsockname()[1])
    try:
        reader, writer = await asyncio.open_connection(
            "localhost", proxy.sockets[0].getsockname()[1]
        )
        writer.write(f"EVALSHA {SHA} 0\r\n".encode())
        await writer.drain()
        try:
            heard = await asyncio.wait_for(reader.read(65536), 0.4)
        except asyncio.TimeoutError:
            heard = b""
        writer.close()
        assert heard == b"", f"the client heard part of the lost reply: {heard!r}"
    finally:
        proxy.close()
        upstream.close()
