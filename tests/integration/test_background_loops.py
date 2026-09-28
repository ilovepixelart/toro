"""Integration: what a worker says when a background loop keeps failing.

The delayed-job sweep, the stalled-job sweep and the heartbeat each retry every
interval whatever went wrong, which is right: a Redis blip must not end them. They
also swallowed every error, so a worker whose sweeps had been failing for an hour
looked healthy, with delayed jobs never promoted and stalled ones never recovered.
"""

import logging

import pytest

# (the method the loop awaits, worker options, how the log names the loop)
LOOPS = [
    pytest.param(
        ("_write_heartbeat", {"heartbeat_interval": 50, "stalled_interval": 0}, "the heartbeat"),
        id="heartbeat",
    ),
    pytest.param(
        ("_promote_delayed", {"stalled_interval": 0}, "the delayed-job sweep"), id="promote"
    ),
    pytest.param(
        ("check_stalled", {"stalled_interval": 50}, "the stalled-job sweep"), id="stalled"
    ),
]


@pytest.mark.parametrize("case", LOOPS)
async def test_a_failing_loop_warns_once_and_notes_its_recovery(
    q, run_worker, run_until, caplog, case
):
    """One warning opens a failure episode and one info line closes it; the retries in
    between stay quiet, so an outage costs two log lines however long it lasts."""
    method, options, what = case
    calls = 0
    broken = True
    real = None

    async def flaky(*args, **kwargs):
        nonlocal calls
        calls += 1
        if broken:
            raise ConnectionError("redis away")
        return await real(*args, **kwargs)

    async def registered() -> bool:
        return w.token in [entry["id"] for entry in await q.workers()]

    def recovered() -> bool:
        return any("recovered" in r.message for r in caplog.records)

    with caplog.at_level(logging.INFO, logger="toro.worker"):
        async with run_worker(q, lambda job: None, **options) as w:
            assert await run_until(registered)  # startup's own heartbeat write is done
            real = getattr(w, method)
            setattr(w, method, flaky)  # this worker's only, and it ends with the test
            assert await run_until(lambda: calls >= 3, timeout=10)  # three failures, one episode
            broken = False
            assert await run_until(recovered, timeout=10)

    warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    infos = [r.message for r in caplog.records if r.levelno == logging.INFO]
    assert len(warnings) == 1 and what in warnings[0] and "redis away" in warnings[0]
    assert infos == [f"{what} recovered"]
