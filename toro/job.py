"""The Job: a typed view over the Redis hash that stores one unit of work."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, TypeAlias, TypedDict, cast

from redis.asyncio import Redis

# The lifecycle states a job can be in (also the queryable states for get_jobs).
# `waiting-children` is the flow-parent park: enqueued, but runnable only once
# every child has settled.
JobState = Literal["wait", "active", "delayed", "completed", "failed", "waiting-children"]


class BackoffOpts(TypedDict, total=False):
    """Retry backoff as a dict: ``{"type": "fixed"|"exponential", "delay": ms}``."""

    type: Literal["fixed", "exponential"]
    delay: int


class Deduplication(TypedDict):
    """``Queue.add()``'s throttle window: ``{"id": str, "ttl": ms}``."""

    id: str
    ttl: int


# What the `backoff` option accepts: nothing, fixed ms, or a BackoffOpts dict.
# (String aliases: evaluated only by type checkers, never at import time.)
Backoff: TypeAlias = "int | float | BackoffOpts | None"
# Auto-removal: None/False keep all · True remove at once · int keep the newest N ·
# {"count": N, "age": seconds} bound both.
RemoveOption: TypeAlias = "bool | int | dict[str, int] | None"


class SupportsResult(Protocol):
    """The slice of Queue that `Job.result()` needs. Typing `_queue` against this
    (not the concrete Queue) keeps the domain object from importing the queue/redis
    layer - dependency inversion, and no circular import to dodge.
    """

    async def result(self, job_id: str, *, timeout: float = ...) -> Any: ...


@dataclass
class JobOptions:
    """Per-job options (delay, attempts, backoff, priority, auto-removal)."""

    delay: int = 0  # ms to wait before the job becomes processable
    attempts: int = 1  # total tries before the job is considered failed
    backoff: Backoff = None  # int ms, or {"type": "fixed"|"exponential", "delay": ms}
    priority: int = 0  # higher = more urgent (global order); 0 = default, FIFO
    remove_on_complete: RemoveOption = None
    remove_on_fail: RemoveOption = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "delay": self.delay,
            "attempts": self.attempts,
            "backoff": self.backoff,
            "priority": self.priority,
            "removeOnComplete": self.remove_on_complete,
            "removeOnFail": self.remove_on_fail,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> JobOptions:
        return cls(
            delay=d.get("delay", 0),
            attempts=d.get("attempts", 1),
            backoff=d.get("backoff"),
            priority=d.get("priority", 0),
            remove_on_complete=d.get("removeOnComplete"),
            remove_on_fail=d.get("removeOnFail"),
        )

    @staticmethod
    def keep_args(opt: RemoveOption) -> tuple[int, int]:
        """Map a remove option to (keepCount, keepAge_seconds) for the Lua side.

        keepCount: -1 keep all · 0 remove immediately · N keep newest N.
        keepAge:   -1 no age limit · S keep only those finished within S seconds.
        """
        if opt is None or opt is False:
            return (-1, -1)
        if opt is True:
            return (0, -1)
        if isinstance(opt, int):
            return (int(opt), -1)
        if isinstance(opt, dict):
            return (int(opt.get("count", -1)), int(opt.get("age", -1)))
        return (-1, -1)


def decode_results(h: dict[str, str]) -> dict[str, Any]:
    """Decode a flow parent's :results hash (child id -> returnvalue JSON).

    The one decode used by both the processor-side (Job) and queue-side
    (Queue) readers, so the two can never drift.
    """
    return {cid: json.loads(v) for cid, v in h.items()}


@dataclass(frozen=True, slots=True)
class JobContext:
    """Worker-side handles attached to a Job while its processor runs, so the handler
    can report progress / append logs.
    """

    redis: Redis
    job_key: str
    events_key: str
    logs_key: str
    job_id: str
    results_key: str  # the flow aux keys, derived via Keys by the worker so
    cfail_key: str  # the layout stays defined in exactly one place (keys.py)


@dataclass
class Job:
    """A snapshot of one job: its id, data, options, state and lifecycle timestamps."""

    id: str
    name: str
    data: Any
    opts: JobOptions = field(default_factory=JobOptions)
    attempts_made: int = 0
    timestamp: int | None = None
    returnvalue: Any = None
    failed_reason: str | None = None
    state: JobState | None = None
    processed_on: int | None = None
    finished_on: int | None = None
    progress: Any = None
    stacktrace: str | None = None
    parent_id: str | None = None  # set on flow children: the parent this job settles into
    children_ids: list[str] | None = None  # set on flow parents: the full child id list
    # Back-reference to the owning Queue, set on jobs returned by Queue.add() so
    # producers can `await job.result()`. Not part of the job's data/identity.
    _queue: SupportsResult | None = field(default=None, repr=False, compare=False)
    # Worker-side context, set while a processor runs so the handler can report
    # progress and append logs.
    _ctx: JobContext | None = field(default=None, repr=False, compare=False)

    async def result(self, *, timeout: float = 30.0) -> Any:
        """Wait for this job to finish; return its value or raise JobFailedError."""
        if self._queue is None:
            raise RuntimeError("job.result() requires a job returned by Queue.add()")
        return await self._queue.result(self.id, timeout=timeout)

    async def update_progress(self, value: Any) -> None:
        """Report progress (a number 0-100 or any JSON value) from a processor."""
        if self._ctx is None:
            raise RuntimeError("update_progress() is only available inside a worker processor")
        ctx = self._ctx
        self.progress = value
        await ctx.redis.hset(ctx.job_key, "progress", json.dumps(value))
        await ctx.redis.publish(
            ctx.events_key,
            json.dumps({"jobId": ctx.job_id, "event": "progress", "progress": value}),
        )

    async def log(self, message: str) -> None:
        """Append a log line to this job (visible in the dashboard)."""
        if self._ctx is None:
            raise RuntimeError("log() is only available inside a worker processor")
        await self._ctx.redis.rpush(self._ctx.logs_key, message)

    async def children_results(self) -> dict[str, Any]:
        """Pull this flow parent's collected child results, keyed by child id.

        The explicit pull (no implicit argument injection) - call it from the
        parent's processor. Empty for jobs that aren't flow parents.
        """
        if self._ctx is None:
            raise RuntimeError("children_results() is only available inside a worker processor")
        return decode_results(
            cast("dict[str, str]", await self._ctx.redis.hgetall(self._ctx.results_key))
        )

    async def failed_children(self) -> dict[str, str]:
        """Pull child id -> failure reason for children failed under
        ``on_fail="continue"``. Empty when every child succeeded.
        """
        if self._ctx is None:
            raise RuntimeError("failed_children() is only available inside a worker processor")
        return cast("dict[str, str]", await self._ctx.redis.hgetall(self._ctx.cfail_key))

    @classmethod
    def from_hash(cls, job_id: str, h: dict[str, str]) -> Job:
        """Build a Job from a decoded Redis hash (str keys/values)."""
        return cls(
            id=job_id,
            name=h.get("name", ""),
            data=json.loads(h["data"]) if h.get("data") else None,
            opts=JobOptions.from_dict(json.loads(h["opts"])) if h.get("opts") else JobOptions(),
            attempts_made=int(h.get("attemptsMade", 0)),
            timestamp=int(h["timestamp"]) if h.get("timestamp") else None,
            returnvalue=json.loads(h["returnvalue"]) if h.get("returnvalue") else None,
            failed_reason=h.get("failedReason"),
            state=cast("JobState | None", h.get("state")),  # Redis stores it untyped
            processed_on=int(h["processedOn"]) if h.get("processedOn") else None,
            finished_on=int(h["finishedOn"]) if h.get("finishedOn") else None,
            progress=json.loads(h["progress"]) if h.get("progress") else None,
            stacktrace=h.get("stacktrace"),
            parent_id=h.get("parentId"),
            children_ids=json.loads(h["children"]) if h.get("children") else None,
        )
