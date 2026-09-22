"""toro exceptions."""

from __future__ import annotations


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
