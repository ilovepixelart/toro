"""Unit: a custom job id becomes a Redis key (`<base><id>`), so it must not land on
another key of the same queue. The checks here are derived from the layout itself, so
a key added to `Keys` or to the Lua scripts cannot be left unguarded."""

import inspect
import re

import pytest

from toro import scripts
from toro.keys import Keys

KEYS = Keys("emails", "toro")
NAMES = [n for n, v in vars(Keys).items() if isinstance(v, property)]
PER_JOB = ["lock", "logs", "deps", "results", "cfail"]
NAMESPACED = [
    n
    for n, v in vars(Keys).items()
    if inspect.isfunction(v) and n not in {"__init__", "job", "job_id_conflict", *PER_JOB}
]


def _suffix(key: str) -> str:
    return key[len(KEYS.base) :]


@pytest.mark.parametrize("name", NAMES)
def test_a_queue_level_key_is_not_a_job_id(name):
    assert KEYS.job_id_conflict(_suffix(getattr(KEYS, name))) is not None


@pytest.mark.parametrize("method", NAMESPACED)
def test_a_namespaced_key_is_not_a_job_id(method):
    # scheduler("x") -> repeat:x, worker("x") -> worker:x, metrics_bucket(7) -> metrics:7
    assert KEYS.job_id_conflict(_suffix(getattr(KEYS, method)(7))) is not None


@pytest.mark.parametrize("method", PER_JOB)
def test_another_jobs_aux_key_is_not_a_job_id(method):
    assert KEYS.job_id_conflict(_suffix(getattr(KEYS, method)("order-123"))) is not None


def test_every_key_the_lua_builds_from_the_base_is_guarded():
    """`base .. "name"` and `base .. "ns:" .. id` in the scripts: keys Python may
    never build (the dedup window `de:<id>` is one) are reserved all the same."""
    literals = set(
        re.findall(r'base \.\. "([^"]+)"', scripts._LIB + scripts.ADD_JOB + scripts.ADD_FLOW)
    )
    assert "de:" in literals  # the scan sees the Lua-only namespace
    for literal in literals:
        probe = literal + "x" if literal.endswith(":") else literal
        assert KEYS.job_id_conflict(probe) is not None, literal


@pytest.mark.parametrize(
    "job_id", ["order-123", "order:123", "user:42:welcome", "completed-2024", "locksmith", "a"]
)
def test_ordinary_ids_are_free(job_id):
    assert KEYS.job_id_conflict(job_id) is None
