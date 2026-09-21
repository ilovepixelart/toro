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


LUA = "".join(v for k, v in vars(scripts).items() if k.isupper() and isinstance(v, str))
# `base .. "name"`, `KEYS[6] .. "events"`, `base .. "de:" .. id`: what the scripts hang
# off the queue's base. And `jobKey .. ":lock"`, `base .. id .. ":deps"`: a job's aux keys.
LUA_NAMES = sorted(set(re.findall(r'(?:base|KEYS\[\d+\]) \.\. "([a-z][a-z:-]*)"', LUA)))
LUA_SUFFIXES = sorted(set(re.findall(r'\.\. "(:[a-z]+)"', LUA)))


def test_the_scan_sees_what_only_the_lua_builds():
    assert "de:" in LUA_NAMES  # the dedup window has no `Keys` method
    assert {":lock", ":deps", ":results", ":cfail"} <= set(LUA_SUFFIXES)


@pytest.mark.parametrize("literal", LUA_NAMES)
def test_every_key_the_lua_hangs_off_the_base_is_guarded(literal):
    probe = literal + "x" if literal.endswith(":") else literal
    assert KEYS.job_id_conflict(probe) is not None


@pytest.mark.parametrize("suffix", LUA_SUFFIXES)
def test_every_aux_suffix_the_lua_builds_is_guarded(suffix):
    assert KEYS.job_id_conflict("order-123" + suffix) is not None


BARE_NAMESPACES = sorted(
    {n[:-1] for n in LUA_NAMES if n.endswith(":")}
    | {_suffix(getattr(KEYS, method)(7)).split(":")[0] for method in NAMESPACED}
)


@pytest.mark.parametrize("name", BARE_NAMESPACES)
def test_a_bare_namespace_name_is_not_a_job_id(name):
    """A job called `de` owns `de:lock` and `de:logs`: the dedup windows of the dedup
    ids `lock` and `logs`. The job's own aux keys collide, not its hash."""
    assert KEYS.job_id_conflict(name) is not None


def test_a_property_added_by_a_subclass_is_guarded():
    class Extended(Keys):
        @property
        def audit(self) -> str:
            return f"{self.base}audit"

    assert Extended("emails").job_id_conflict("audit") is not None


@pytest.mark.parametrize(
    "job_id", ["order-123", "order:123", "user:42:welcome", "completed-2024", "locksmith", "a"]
)
def test_ordinary_ids_are_free(job_id):
    assert KEYS.job_id_conflict(job_id) is None
