"""The one place a raw Redis reply gets its Python type.

Every client runs with ``decode_responses`` on, so replies are ``str`` at runtime,
while redis-py types them as ``bytes | str``. These helpers name that boundary once.
They take ``Any`` on purpose: ``dict`` and ``list`` are invariant, so a cast straight
from redis-py's declared type is a cast between unrelated types, not a narrowing.
"""

from __future__ import annotations

from typing import Any, cast


def _str_list(reply: Any) -> list[str]:
    """Type a Redis list/zset reply as list[str]."""
    return cast("list[str]", reply)


def _str_dict(reply: Any) -> dict[str, str]:
    """Type a Redis hash reply as dict[str, str]."""
    return cast("dict[str, str]", reply)


def _hash_replies(reply: Any) -> list[dict[str, str]]:
    """Type a pipeline's list of hash replies as list[dict[str, str]]."""
    return cast("list[dict[str, str]]", reply)


def _scored(reply: Any) -> list[tuple[str, float]]:
    """Type a zset reply read WITHSCORES as list[(member, score)]."""
    return cast("list[tuple[str, float]]", reply)
