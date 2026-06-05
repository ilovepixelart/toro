"""Unit: priority clamping + the score-packing invariant that keeps ZSET scores exact."""

from toro import scripts
from toro.queue import _clamp_priority


def test_clamp_priority_bounds():
    assert _clamp_priority(-5) == 0  # never negative
    assert _clamp_priority(0) == 0
    assert _clamp_priority(42) == 42
    assert _clamp_priority(scripts.PRIORITY_OFFSET + 10) == scripts.PRIORITY_OFFSET  # capped


def test_clamp_priority_coerces_to_int():
    assert _clamp_priority(3.9) == 3


def test_packed_score_stays_within_an_exact_double():
    # priorityScore = (PRIORITY_OFFSET - priority) * SEQ_MOD + seq. The worst case
    # (priority 0, seq at the top) must stay < 2^53 or ZSET float scores lose precision.
    worst = scripts.PRIORITY_OFFSET * scripts.SEQ_MOD + (scripts.SEQ_MOD - 1)
    assert worst < 2**53
