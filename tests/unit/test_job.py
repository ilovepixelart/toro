"""Unit: Job.from_hash — decoding a Redis hash into a typed Job."""

import json

import pytest

from toro.job import Job, JobOptions


def test_from_hash_parses_all_fields():
    h = {
        "name": "send",
        "data": json.dumps({"to": "a@b.c"}),
        "opts": json.dumps(JobOptions(attempts=3).to_dict()),
        "attemptsMade": "2",
        "timestamp": "1700000000000",
        "returnvalue": json.dumps({"ok": True}),
        "state": "completed",
        "processedOn": "1700000000100",
        "finishedOn": "1700000000500",
        "progress": "75",
        "stacktrace": "Traceback (most recent call last): ...",
    }
    j = Job.from_hash("42", h)

    assert j.id == "42"
    assert j.name == "send"
    assert j.data == {"to": "a@b.c"}
    assert j.opts.attempts == 3
    assert j.attempts_made == 2
    assert j.timestamp == 1700000000000
    assert j.returnvalue == {"ok": True}
    assert j.state == "completed"
    assert (j.processed_on, j.finished_on) == (1700000000100, 1700000000500)
    assert j.progress == 75
    assert j.stacktrace.startswith("Traceback")


def test_from_hash_tolerates_a_sparse_hash():
    j = Job.from_hash("1", {"name": "x"})
    assert j.name == "x"
    assert j.data is None
    assert j.attempts_made == 0  # defaults to 0, not a crash
    assert isinstance(j.opts, JobOptions)
    assert j.returnvalue is None


async def test_result_requires_a_queue_backed_job():
    # A bare Job (e.g. built from a hash) has no owning queue — result() must refuse.
    with pytest.raises(RuntimeError):
        await Job(id="1", name="x", data={}).result()


async def test_progress_and_log_require_a_worker_context():
    # update_progress/log only make sense inside a running processor.
    job = Job(id="1", name="x", data={})
    with pytest.raises(RuntimeError):
        await job.update_progress(50)
    with pytest.raises(RuntimeError):
        await job.log("nope")
