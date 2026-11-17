"""The Redis layers run only against the Redis REDIS_URL names, never a default.

Port 6379 on a development machine may belong to another project, and these tests
write to it. Run as real pytest sessions in a subprocess over a copy of the shared
conftest, so what is proved is the wiring itself. The only Redis URL used here is a
closed port: nothing in this file reaches a server.
"""

import pathlib
import subprocess
import sys

import pytest

TESTS = pathlib.Path(__file__).resolve().parents[1]
CLOSED = "redis://127.0.0.1:1"


def _session(pytester, monkeypatch, url: str | None, folder: str, body: str):
    if url is None:
        monkeypatch.delenv("REDIS_URL", raising=False)
    else:
        monkeypatch.setenv("REDIS_URL", url)
    pytester.makeconftest((TESTS / "conftest.py").read_text())
    pytester.makepyprojecttoml(
        '[tool.pytest.ini_options]\nasyncio_mode = "auto"\n'
        'markers = ["unit: x", "integration: x", "load: x", "perf: x"]\n'
    )
    (pytester.path / folder).mkdir()
    (pytester.path / folder / "test_inner.py").write_text(body)
    return pytester.runpytest_subprocess("-p", "no:cacheprovider", "-rs")


@pytest.mark.parametrize("folder", ["integration", "load", "perf"])
def test_a_redis_layer_skips_when_redis_url_is_unset(pytester, monkeypatch, folder):
    result = _session(
        pytester, monkeypatch, None, folder, "def test_x():\n    raise AssertionError\n"
    )

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*set REDIS_URL to a Redis you own*"])


def test_reachability_is_checked_on_redis_url(pytester, monkeypatch):
    result = _session(
        pytester, monkeypatch, CLOSED, "integration", "def test_x():\n    raise AssertionError\n"
    )

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines([f"*no Redis answers on REDIS_URL ({CLOSED})*"])


@pytest.mark.parametrize("undo", ["", "    monkeypatch.undo()\n"], ids=["plain", "after-undo"])
def test_a_queue_and_worker_without_url_use_redis_url(pytester, monkeypatch, undo):
    """Also after the test's own `monkeypatch.undo()`, which reverts everything
    the test's monkeypatch set, autouse fixtures' patches included."""
    body = (
        "from toro import Queue, Worker\n\n"
        "def test_x(monkeypatch):\n"
        f"{undo}"
        "    for client in (Queue('q').redis, Worker('q', print).redis):\n"
        "        kw = client.connection_pool.connection_kwargs\n"
        "        assert (kw['host'], kw['port']) == ('127.0.0.1', 1)\n"
    )

    _session(pytester, monkeypatch, CLOSED, "unit", body).assert_outcomes(passed=1)


@pytest.mark.parametrize(
    "script", ["load/harness.py", "perf/harness.py", "compat/rolling_upgrade.py"]
)
def test_a_script_refuses_to_start_without_redis_url(monkeypatch, script):
    monkeypatch.delenv("REDIS_URL", raising=False)

    run = subprocess.run(  # noqa: S603
        [sys.executable, str(TESTS / script)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert run.returncode == 2
    assert "set REDIS_URL to a Redis you own" in run.stderr
