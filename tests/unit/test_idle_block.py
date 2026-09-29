"""Unit: how long an idle slot blocks once the claim has said when the next delayed
job is due."""

import pytest

from toro.worker import block_for


@pytest.mark.parametrize(
    ("due_ms", "expected"),
    [
        pytest.param(None, 5.0, id="nothing-delayed-blocks-the-whole-poll"),
        pytest.param(12_000, 2.0, id="due-inside-the-poll-ends-the-block-then"),
        pytest.param(30_000, 5.0, id="due-past-the-poll-blocks-the-whole-poll"),
        pytest.param(10_000, 0.001, id="due-now-returns-at-once-never-zero"),
        pytest.param(9_000, 0.001, id="due-passed-returns-at-once-never-zero"),
    ],
)
def test_block_for_ends_at_the_next_due_time_or_the_poll(due_ms, expected):
    # 0 would block for good: a blocking pop reads it as "no timeout".
    assert block_for(5.0, due_ms, now_ms=10_000) == expected
