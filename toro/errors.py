"""toro exceptions."""

from __future__ import annotations

from typing import Any


class ToroError(Exception):
    """Base class for toro errors."""


class JobFailedError(ToroError):
    """Raised by result() when the awaited job ended in the failed state."""

    def __init__(self, reason: str | None) -> None:
        super().__init__(reason or "job failed")
        self.reason = reason


class JobCancelledError(ToroError):
    """Raised by result() when the awaited job was cancelled.

    Not a JobFailedError: a caller that retries on failure must not retry work that
    was deliberately stopped.
    """

    def __init__(self, job_id: str, reason: str | None = None) -> None:
        super().__init__(f"job {job_id} was cancelled" + (f": {reason}" if reason else ""))
        self.job_id = job_id
        self.reason = reason


class PartialFlushError(ToroError):
    """A batch of collected jobs where some were sent and some were not.

    Redis has no rollback, so a pipelined batch whose scripts raise on one job leaves
    the others enqueued. `sent` is what landed; what did not stays in the buffer, so
    a retry sends only that and nothing is enqueued twice.
    """

    def __init__(self, sent: list[Any], errors: list[BaseException]) -> None:
        total = len(sent) + len(errors)
        super().__init__(f"{len(sent)} of {total} jobs were sent; the rest failed: {errors[0]}")
        self.sent = sent
        self.errors = errors


class IncompatibleDataModelError(ToroError):
    """The queue's keys were written by a toro that understands a newer data model.

    Two versions share one Redis during a rolling upgrade, so this is the ordinary
    way an upgrade goes wrong: the older library stops rather than writing into a
    shape it was not built for.
    """

    def __init__(self, queue: str, found: int, understood: int) -> None:
        super().__init__(
            f"queue {queue!r} uses data model {found}; this toro understands {understood}. "
            f"Upgrade this process, or point it at a queue of its own."
        )
        self.found = found
        self.understood = understood
