"""toro - an async-first, Redis-backed job queue for Python."""

from importlib.metadata import version

from .errors import (
    IncompatibleDataModelError,
    JobCancelledError,
    JobFailedError,
    PartialFlushError,
    ToroError,
)
from .flow import FlowChild, FlowView, OnFail
from .job import Backoff, BackoffOpts, Deduplication, Job, JobOptions, JobState, RemoveOption
from .openmetrics import render_all
from .queue import FlowMetricsPoint, MetricsPoint, NameMetrics, PendingJobs, Queue
from .scripts import DATA_MODEL_VERSION
from .worker import RateLimit, Worker

__all__ = [
    "DATA_MODEL_VERSION",
    "Backoff",
    "BackoffOpts",
    "Deduplication",
    "FlowChild",
    "FlowMetricsPoint",
    "FlowView",
    "IncompatibleDataModelError",
    "Job",
    "JobCancelledError",
    "JobFailedError",
    "JobOptions",
    "JobState",
    "MetricsPoint",
    "NameMetrics",
    "OnFail",
    "PartialFlushError",
    "PendingJobs",
    "Queue",
    "RateLimit",
    "RemoveOption",
    "ToroError",
    "Worker",
    "render_all",
]
# Asked, not repeated: `pyproject.toml` holds the number and `uv version --bump`
# edits it there, so this module has nothing to keep in step.
__version__ = version("toro-queue")
