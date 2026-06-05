"""toro exceptions."""

from __future__ import annotations


class ToroError(Exception):
    """Base class for toro errors."""


class JobFailedError(ToroError):
    """Raised by result() when the awaited job ended in the failed state."""

    def __init__(self, reason: str | None) -> None:
        super().__init__(reason or "job failed")
        self.reason = reason
