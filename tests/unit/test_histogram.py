"""The duration-histogram math: log bucket bounds and percentile readout."""

import itertools

from toro import scripts
from toro.queue import _percentile, bucket_estimate_ms, bucket_upper_ms


def test_bucket_bounds_grow_log_scale():
    bounds = [bucket_upper_ms(i) for i in range(scripts.HIST_BUCKETS)]
    assert bounds[0] == scripts.HIST_BASE_MS  # [0, 20ms)
    assert bounds == sorted(bounds)
    # each bucket ~1.5x the previous (int truncation allowed)
    for a, b in itertools.pairwise(bounds):
        assert 1.4 < b / a <= 1.51
    assert bounds[-1] > 5 * 60 * 1000  # top bucket reaches past 5 minutes


def test_percentile_of_empty_is_zero():
    assert _percentile([], 0.95) == 0
    assert _percentile([0] * scripts.HIST_BUCKETS, 0.95) == 0


def test_percentile_single_bucket():
    buckets = [0] * scripts.HIST_BUCKETS
    buckets[3] = 10  # everything in one bucket
    for q in (0.5, 0.95, 0.99):
        assert _percentile(buckets, q) == bucket_estimate_ms(3)


def test_estimate_is_the_geometric_mean_not_the_upper_bound():
    # the representative value sits inside the bucket (DDSketch-style), so the
    # worst-case error is two-sided ~22% instead of one-sided +50%
    for idx in (1, 5, 20):
        upper = bucket_upper_ms(idx)
        lower = bucket_upper_ms(idx - 1)
        est = bucket_estimate_ms(idx)
        assert lower < est < upper


def test_rank_arithmetic_survives_float_artifacts():
    # 20 * 0.95 == 19.000000000000004; ceil of that must be 19, not 20
    buckets = [0] * scripts.HIST_BUCKETS
    buckets[0] = 19
    buckets[10] = 1  # one straggler at rank 20
    assert _percentile(buckets, 0.95) == bucket_estimate_ms(0)


def test_percentiles_expose_the_tail_a_mean_would_hide():
    buckets = [0] * scripts.HIST_BUCKETS
    buckets[0] = 99  # 99 jobs under 20ms
    buckets[20] = 1  # one ~1.5min straggler
    assert _percentile(buckets, 0.50) == bucket_estimate_ms(0)
    assert _percentile(buckets, 0.95) == bucket_estimate_ms(0)
    assert _percentile(buckets, 0.99) == bucket_estimate_ms(0)
    assert _percentile(buckets, 1.0) == bucket_estimate_ms(20)  # the straggler


def test_percentile_picks_the_crossing_bucket():
    buckets = [0] * scripts.HIST_BUCKETS
    buckets[5] = 2
    buckets[6] = 2
    assert _percentile(buckets, 0.5) == bucket_estimate_ms(5)
    assert _percentile(buckets, 0.99) == bucket_estimate_ms(6)
