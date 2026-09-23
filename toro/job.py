"""The Job: a typed view over the Redis hash that stores one unit of work."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, TypeAlias, TypedDict, cast

from redis.asyncio import Redis

from ._replies import _str_dict

# The lifecycle states a job can be in (also the queryable states for get_jobs).
# `waiting-children` is the flow-parent park: enqueued, but runnable only once
# every child has settled. `held` waits on a concurrency key, not on a worker.
# `cancelled` was stopped on purpose, which is not a failure and is not counted as one.
JobState = Literal[
    "wait", "active", "delayed", "held", "completed", "failed", "cancelled", "waiting-children"
]
# The states a job is finished in: no further attempt, and retention applies. The one
# such list - Lua asks the same question through `isFinished` in scripts.py.
FINISHED_STATES: tuple[JobState, ...] = ("completed", "failed", "cancelled")


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
# Auto-removal: None (unset) keep the newest DEFAULT_KEEP_* · False keep all ·
# True remove at once · int keep the newest N · {"count": N, "age": seconds} bound both.
RemoveOption: TypeAlias = "bool | int | dict[str, int] | None"
# What an unset option keeps. A count, not an age: a count caps memory whatever
# the throughput. Failures are what gets debugged, so more of them are kept.
# The option is read in ONE place, `keepFor` in scripts.py, which takes these.
DEFAULT_KEEP_COMPLETED = 1000
DEFAULT_KEEP_FAILED = 5000


class SupportsResult(Protocol):
    """The slice of Queue that `Job.result()` needs. Typing `_queue` against this
    (not the concrete Queue) keeps the domain object from importing the queue/redis
    layer - dependency inversion, and no circular import to dodge.
    """

    async def result(self, job_id: str, *, timeout: float = ...) -> Any: ...


def key_segment(value: object, what: str) -> str | None:
    """Validate a value that becomes a Redis key segment, or None when unset.

    `:` or a control character would let two distinct values collide into one key,
    and silently share what belongs to one of them.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value or ":" in value or any(ord(c) < 0x20 for c in value):
        msg = f"{what} must be a non-empty string with no ':' or control characters"
        raise ValueError(msg)
    return value


def _whole(value: object, what: str, *, minimum: int = 0) -> int:
    """Return a count of milliseconds, attempts or priority as a whole number.

    The Lua `tonumber()`s these after it has already written, so a value that is not
    a number aborts a script mid-way and leaves a job no API can reach. bool is an
    int subclass and is rejected by name: `attempts=True` would silently mean 1.
    """
    if isinstance(value, bool):  # an int subclass: `attempts=True` would mean 1
        msg = f"{what} must be a whole number >= {minimum}, not {value!r}"
        raise ValueError(msg)  # noqa: TRY004 - an option's shape is a value error here,
        # as it is for every other option: a caller catching ValueError catches them all
    # A float that IS whole is a config value that came through arithmetic
    # (`86_400 / 2`), and refusing it teaches nothing. A fractional one is a mistake
    # the scripts cannot carry out: a rank is an integer.
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if not isinstance(value, int) or value < minimum:
        msg = f"{what} must be a whole number >= {minimum}, not {value!r}"
        raise ValueError(msg)
    return value


def _backoff(value: Backoff) -> Backoff:
    """`None`, milliseconds, or `{"type": "fixed"|"exponential", "delay": ms}`."""
    if value is None:
        return None
    if isinstance(value, dict):
        kind = value.get("type")
        if kind not in ("fixed", "exponential"):
            msg = f"backoff type must be 'fixed' or 'exponential', not {kind!r}"
            raise ValueError(msg)
        return {**value, "delay": _whole(value.get("delay"), "backoff delay")}
    return _whole(value, "backoff")


def _retention(value: RemoveOption, what: str) -> RemoveOption:
    """`None` (the bounded default), a bool, a count, or `{"count": N, "age": s}`.

    A string here used to mean "keep everything": the Lua reads a number or gives up,
    so `remove_on_complete="2"` silently disabled the bound it was asking for.
    """
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, dict):
        if not value or set(value) - {"count", "age"}:
            msg = f"{what} dict takes 'count' and/or 'age', not {sorted(value)}"
            raise ValueError(msg)
        return {field: _whole(number, f"{what} {field}") for field, number in value.items()}
    return _whole(value, what)


@dataclass
class JobOptions:
    """Per-job options (delay, attempts, backoff, priority, auto-removal)."""

    delay: int = 0  # ms to wait before the job becomes processable
    attempts: int = 1  # total tries before the job is considered failed
    backoff: Backoff = None  # int ms, or {"type": "fixed"|"exponential", "delay": ms}
    priority: int = 0  # higher = more urgent (global order); 0 = default, FIFO
    remove_on_complete: RemoveOption = None
    remove_on_fail: RemoveOption = None
    # Jobs that share a key run one at a time, in the order they were added.
    concurrency_key: str | None = None

    def __post_init__(self) -> None:
        # Validated here rather than in Queue.add, so every way of enqueuing - a job,
        # a flow node, a scheduler template - is covered by construction.
        self.concurrency_key = key_segment(self.concurrency_key, "concurrency_key")
        self.delay = _whole(self.delay, "delay")
        self.attempts = _whole(self.attempts, "attempts", minimum=1)
        self.priority = _whole(self.priority, "priority")
        self.backoff = _backoff(self.backoff)
        self.remove_on_complete = _retention(self.remove_on_complete, "remove_on_complete")
        self.remove_on_fail = _retention(self.remove_on_fail, "remove_on_fail")

    def to_dict(self) -> dict[str, Any]:
        return {
            "delay": self.delay,
            "attempts": self.attempts,
            "backoff": self.backoff,
            "priority": self.priority,
            "removeOnComplete": self.remove_on_complete,
            "removeOnFail": self.remove_on_fail,
            "concurrencyKey": self.concurrency_key,
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
            concurrency_key=d.get("concurrencyKey"),
        )


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
    ccancel_key: str


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
    cancel_reason: str | None = None  # why it was stopped, when the caller said
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
        return decode_results(_str_dict(await self._ctx.redis.hgetall(self._ctx.results_key)))

    async def failed_children(self) -> dict[str, str]:
        """Pull child id -> failure reason for children failed under
        ``on_fail="continue"``. Empty when every child succeeded.
        """
        if self._ctx is None:
            raise RuntimeError("failed_children() is only available inside a worker processor")
        return _str_dict(await self._ctx.redis.hgetall(self._ctx.cfail_key))

    async def cancelled_children(self) -> dict[str, str]:
        """Pull child id -> why it was stopped, for children cancelled under
        ``on_fail="continue"``. Separate from `failed_children()`: a job somebody
        stopped did not fail, and a parent deciding what to do wants to know which.
        """
        if self._ctx is None:
            raise RuntimeError("cancelled_children() is only available inside a worker processor")
        return _str_dict(await self._ctx.redis.hgetall(self._ctx.ccancel_key))

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
            cancel_reason=h.get("cancelReason"),
            state=cast("JobState | None", h.get("state")),  # Redis stores it untyped
            processed_on=int(h["processedOn"]) if h.get("processedOn") else None,
            finished_on=int(h["finishedOn"]) if h.get("finishedOn") else None,
            progress=json.loads(h["progress"]) if h.get("progress") else None,
            stacktrace=h.get("stacktrace"),
            parent_id=h.get("parentId"),
            children_ids=json.loads(h["children"]) if h.get("children") else None,
        )
