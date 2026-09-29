"""Unit: the numbers the Lua shares with Python come from Python.

`_CONSTANTS` exists so the two cannot drift, yet the priority packing and the lock
renewal's answers were spelled out again as digits inside the Lua: a Python constant
changed on its own passed every test that derived its expectation from the constant,
while the scripts kept the old number.
"""

import re

from toro import scripts


def _scripts_with_lib() -> list[str]:
    return [
        value
        for name, value in vars(scripts).items()
        if name.isupper() and isinstance(value, str) and value.startswith(scripts._CONSTANTS)
    ]


def test_the_priority_packing_reads_the_python_constants():
    assert f"local PRIORITY_OFFSET = {scripts.PRIORITY_OFFSET}\n" in scripts._CONSTANTS
    assert f"local SEQ_MOD = {scripts.SEQ_MOD}\n" in scripts._CONSTANTS
    for script in _scripts_with_lib():
        body = script[len(scripts._CONSTANTS) :]
        assert str(scripts.PRIORITY_OFFSET) not in body
        assert str(scripts.SEQ_MOD) not in body


def test_the_finish_sentinels_read_the_python_constants():
    assert f"local LOCK_LOST = {scripts.LOCK_LOST}\n" in scripts._CONSTANTS
    assert f"local NOT_ACTIVE = {scripts.NOT_ACTIVE}\n" in scripts._CONSTANTS
    for script in _scripts_with_lib():
        body = script[len(scripts._CONSTANTS) :]
        assert not re.search(r"return -[23]\b", body)


def test_the_lock_renewal_answers_read_the_python_constants():
    assert f"local CANCEL_REQUESTED = {scripts.LOCK_CANCEL_REQUESTED}\n" in scripts.EXTEND_LOCK
    assert f"local JOB_GONE = {scripts.LOCK_JOB_GONE}\n" in scripts.EXTEND_LOCK
    assert "return CANCEL_REQUESTED" in scripts.EXTEND_LOCK
    assert "return JOB_GONE" in scripts.EXTEND_LOCK
    assert not re.search(r"return (2|-1)\b", scripts.EXTEND_LOCK)
