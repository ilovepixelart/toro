"""Unit: rendering what a scraper reads (docs/specs/operate.md).

The rendering is a pure function of the numbers, so every rule of the format is
checked here without a Redis in the loop.
"""

import pytest

from toro.openmetrics import render

TOTALS = {"added": 7, "completed": 4, "failed": 2, "cancelled": 1, "ms": 512}
DEPTHS = {"wait": 3, "active": 1, "held": 2, "delayed": 0, "cancelled": 1}


def _families(text: str) -> dict[str, str]:
    """Family name -> its declared type, from the TYPE lines."""
    return {
        line.split()[2]: line.split()[3] for line in text.splitlines() if line.startswith("# TYPE ")
    }


def test_every_sample_belongs_to_a_declared_family():
    text = render("emails", TOTALS, DEPTHS)
    declared = _families(text)

    assert declared, "nothing was declared"
    for line in text.splitlines():
        if line.startswith("#") or not line:
            continue
        family = line.split("{")[0].split()[0]
        assert family in declared, f"{family} has no TYPE line"


def test_a_counter_is_named_total_and_a_gauge_is_not():
    declared = _families(render("emails", TOTALS, DEPTHS))

    for name, kind in declared.items():
        if kind == "counter":
            assert name.endswith("_total"), f"{name} is a counter and must end in _total"
        else:
            assert not name.endswith("_total"), f"{name} is a {kind}, not a counter"


def test_outcomes_are_labels_on_one_family_not_families_of_their_own():
    text = render("emails", TOTALS, DEPTHS)

    for outcome, n in (("completed", 4), ("failed", 2), ("cancelled", 1), ("added", 7)):
        assert f'toro_jobs_total{{queue="emails",outcome="{outcome}"}} {n}' in text


def test_depth_is_reported_for_every_state_given():
    text = render("emails", TOTALS, DEPTHS)

    for state, n in DEPTHS.items():
        assert f'toro_queue_depth{{queue="emails",state="{state}"}} {n}' in text


def test_it_ends_with_the_terminator_a_scraper_looks_for():
    assert render("emails", TOTALS, DEPTHS).endswith("# EOF\n")


def test_a_missing_counter_reads_as_zero_not_as_a_gap():
    """A family that appears only once a job has failed makes `rate()` start at a
    cliff. Every outcome is always present."""
    text = render("emails", {"added": 1}, {})

    for outcome in ("added", "completed", "failed", "cancelled"):
        assert f'outcome="{outcome}"' in text


@pytest.mark.parametrize("name", ['we"ird', "back\\slash", "new\nline"])
def test_a_label_value_cannot_break_out_of_its_quotes(name):
    text = render(name, TOTALS, DEPTHS)

    for line in text.splitlines():
        if line.startswith("#") or not line:
            continue
        # drop what is escaped, so only the quotes that delimit a value are left
        structural = line.replace("\\\\", "").replace('\\"', "").replace("\\n", "")
        assert structural.count('"') % 2 == 0, f"unbalanced quotes: {line!r}"
        assert "\n" not in line
