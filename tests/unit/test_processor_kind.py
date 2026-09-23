"""Unit: telling an async processor from a sync one (docs/specs/sync-and-scale.md).

Asked once, at construction, and by inspection: calling the processor to find out
what it returns would run a sync one inside the event loop, which is the thing the
whole feature exists to avoid.
"""

import functools

import pytest

from toro.worker import _is_async


async def _async_job(job):
    return 1


def _sync_job(job):
    return 1


class AsyncCallable:
    async def __call__(self, job):
        return 1


class SyncCallable:
    def __call__(self, job):
        return 1


@pytest.mark.parametrize(
    ("processor", "is_async"),
    [
        (_async_job, True),
        (_sync_job, False),
        (AsyncCallable(), True),  # a class-based processor: the question is __call__
        (SyncCallable(), False),
        (functools.partial(_async_job), True),  # partials are the common wrapper
        (functools.partial(_sync_job), False),
        (lambda job: 1, False),
    ],
)
def test_the_kind_is_read_off_the_callable(processor, is_async):
    assert _is_async(processor) is is_async
