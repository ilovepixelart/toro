"""The duration-histogram math: log bucket bounds and percentile readout."""

import itertools

from toro import scripts
from toro.queue import _percentile, bucket_upper_ms


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
        assert _percentile(buckets, q) == bucket_upper_ms(3)


def test_percentiles_expose_the_tail_a_mean_would_hide():
    buckets = [0] * scripts.HIST_BUCKETS
    buckets[0] = 99  # 99 jobs under 20ms
    buckets[20] = 1  # one ~1.5min straggler
    assert _percentile(buckets, 0.50) == bucket_upper_ms(0)
    assert _percentile(buckets, 0.95) == bucket_upper_ms(0)
    assert _percentile(buckets, 0.99) == bucket_upper_ms(0)
    assert _percentile(buckets, 1.0) == bucket_upper_ms(20)  # the straggler


def test_percentile_is_conservative_upper_bound():
    buckets = [0] * scripts.HIST_BUCKETS
    buckets[5] = 2
    buckets[6] = 2
    assert _percentile(buckets, 0.5) == bucket_upper_ms(5)
    assert _percentile(buckets, 0.99) == bucket_upper_ms(6)
