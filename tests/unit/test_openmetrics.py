"""Unit: rendering what a scraper reads (docs/specs/operate.md).

The rendering is a pure function of the numbers, so every rule of the format is
checked here without a Redis in the loop.
"""

import pytest
from prometheus_client.openmetrics.parser import text_string_to_metric_families as parse

from toro.openmetrics import render, render_all

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
        sample = line.split("{")[0].split()[0]
        # a counter's sample is its family plus `_total`; every other sample is the
        # family itself
        family = sample.removesuffix("_total")
        assert family in declared, f"{sample} has no TYPE line"
        if family != sample:
            assert declared[family] == "counter", f"{sample} is not a counter sample"


def test_the_reference_parser_reads_it_back():
    """OP-001. The rule this catches and reading the spec does not: a counter's FAMILY
    is `toro_jobs` and its SAMPLE is `toro_jobs_total`. A family already named
    `toro_jobs_total` would need a `toro_jobs_total_total` sample, and the parser
    rejects the whole document, gauge included, rather than the one family."""
    text = render("emails", TOTALS, DEPTHS)

    parsed = {family.name: family for family in parse(text)}

    assert set(parsed) == {"toro_jobs", "toro_job_duration_ms", "toro_queue_depth"}
    samples = {
        (sample.name, sample.labels.get("outcome") or sample.labels.get("state")): sample.value
        for family in parsed.values()
        for sample in family.samples
    }
    assert samples[("toro_jobs_total", "completed")] == 4
    assert samples[("toro_job_duration_ms_total", None)] == 512
    assert samples[("toro_queue_depth", "wait")] == 3


def test_the_total_suffix_is_on_the_sample_not_on_the_family():
    """Backwards is the easy mistake, and it costs the whole document rather than the
    one family: a family called `toro_jobs_total` would need a `_total_total` sample."""
    text = render("emails", TOTALS, DEPTHS)
    declared = _families(text)

    for name, kind in declared.items():
        assert not name.endswith("_total"), f"{name} is a family name, so it carries no suffix"
        sample = f"{name}_total" if kind == "counter" else name
        assert f"\n{sample}{{" in text or f"\n{sample} " in text, f"{sample} has no sample line"


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


@pytest.mark.parametrize(
    ("name", "escaped"),
    [('we"ird', 'we\\"ird'), ("back\\slash", "back\\\\slash"), ("new\nline", "new\\nline")],
)
def test_a_label_value_cannot_break_out_of_its_quotes(name, escaped):
    text = render(name, TOTALS, DEPTHS)

    # the escape itself, so a character that is merely left alone is not mistaken for
    # one that is escaped: a quote-balance count cannot fail on a backslash
    assert f'queue="{escaped}"' in text
    assert list(parse(text)), "the reference parser could not read the escaped name"
    for line in text.splitlines():
        if line.startswith("#") or not line:
            continue
        # drop what is escaped, so only the quotes that delimit a value are left
        structural = line.replace("\\\\", "").replace('\\"', "").replace("\\n", "")
        assert structural.count('"') % 2 == 0, f"unbalanced quotes: {line!r}"
        assert "\n" not in line


def test_a_family_is_declared_once_however_many_queues():
    """A dashboard serves several queues from one endpoint. Concatenating a render
    per queue repeats every TYPE and HELP line, which is not valid exposition: a
    parser is entitled to reject the duplicate or to drop the samples after it."""
    text = render_all({"emails": (TOTALS, DEPTHS), "reports": (TOTALS, DEPTHS)})

    types = [line for line in text.splitlines() if line.startswith("# TYPE ")]
    assert len(types) == len(set(types)), f"a family was declared twice: {types}"
    helps = [line for line in text.splitlines() if line.startswith("# HELP ")]
    assert len(helps) == len(set(helps))
    for queue in ("emails", "reports"):
        assert f'toro_jobs_total{{queue="{queue}",outcome="completed"}} 4' in text
    assert text.count("# EOF") == 1
    assert text.endswith("# EOF\n")


def test_one_queue_renders_the_same_either_way():
    assert render("emails", TOTALS, DEPTHS) == render_all({"emails": (TOTALS, DEPTHS)})
