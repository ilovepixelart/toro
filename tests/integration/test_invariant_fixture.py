"""The `q` fixture's teardown check: a test that passes but leaves the queue
inconsistent fails at its own teardown, naming the invariant it broke, instead of
some later test failing for a reason it cannot show.

Run as a real pytest session in a subprocess (the inner session needs its own event
loop) over a copy of the shared conftest, so what is proved is the wiring itself.
"""

import pathlib

CONFTEST = pathlib.Path(__file__).resolve().parents[1] / "conftest.py"


def test_a_passing_test_that_leaves_a_job_in_two_states_errors_at_teardown(pytester):
    pytester.makeconftest(CONFTEST.read_text())
    pytester.makepyprojecttoml(
        '[tool.pytest.ini_options]\nasyncio_mode = "auto"\n'
        'markers = ["unit: x", "integration: x", "load: x", "perf: x"]\n'
    )
    pytester.makepyfile(
        test_leaves="""
        async def test_leaves_a_job_in_two_states(q):
            await q.redis.zadd(q.keys.prioritized, {"ghost": 0})
            await q.redis.rpush(q.keys.active, "ghost")
        """
    )

    result = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-q")

    result.assert_outcomes(passed=1, errors=1)
    result.stdout.fnmatch_lines(["*ghost in both prioritized and active*"])
