"""Unit: the ARGV an occurrence is minted with, in the order ADD_SCHEDULED reads."""

import json

from toro import scripts

TEMPLATE = {
    "name": "rollup",
    "data": json.dumps({"day": 1}),
    "opts": json.dumps({"priority": 3, "concurrencyKey": "tenant-1", "attempts": 2}),
}


def test_the_argv_follows_the_script_header():
    args = scripts.scheduled_args(
        occurrence_id="repeat:nightly:1700000000000",
        template=TEMPLATE,
        now=1699999999000,
        when=1700000000000,
        scheduler_id="nightly",
    )
    assert args == [
        "repeat:nightly:1700000000000",  # ARGV[1] the occurrence's id
        "rollup",  # ARGV[2] name
        TEMPLATE["data"],  # ARGV[3] data
        TEMPLATE["opts"],  # ARGV[4] opts
        1699999999000,  # ARGV[5] now
        1700000000000,  # ARGV[6] when (the occurrence's due time)
        3,  # ARGV[7] priority, out of the stored opts
        "nightly",  # ARGV[8] scheduler id
        "tenant-1",  # ARGV[9] concurrency key
        scripts.METRICS_RETENTION_MS,  # ARGV[10]
    ]


def test_a_template_without_priority_or_key_mints_at_the_defaults():
    plain = {**TEMPLATE, "opts": json.dumps({"attempts": 2})}
    args = scripts.scheduled_args(
        occurrence_id="repeat:s:1", template=plain, now=0, when=1, scheduler_id="s"
    )
    assert (args[6], args[8]) == (0, "")
