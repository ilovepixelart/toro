"""Unit: next_run — the repeatable-schedule math (every-grid + cron), no Redis."""

import pytest

from toro.scheduler import next_run


def test_every_returns_next_grid_slot_strictly_after_now():
    assert next_run(10_500, every=1000) == 11_000
    # exactly on a slot → the NEXT slot, never "now"
    assert next_run(11_000, every=1000) == 12_000


def test_every_catches_up_to_next_slot_not_a_backlog():
    # a late tick jumps to the next slot rather than firing every missed one
    assert next_run(57_000, every=5000) == 60_000


def test_cron_minute_boundary():
    # 1970-01-01 00:00:30 UTC (30_000 ms) → top of the next minute (60_000 ms)
    assert next_run(30_000, cron="* * * * *") == 60_000


def test_cron_every_five_minutes():
    assert next_run(0, cron="*/5 * * * *") == 5 * 60_000


def test_requires_exactly_a_schedule():
    with pytest.raises(ValueError):
        next_run(0)
