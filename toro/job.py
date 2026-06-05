"""The Job: a typed view over the Redis hash that stores one unit of work."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class JobOptions:
    """Per-job options (delay, attempts, backoff, priority, auto-removal)."""

    delay: int = 0  # ms to wait before the job becomes processable
    attempts: int = 1  # total tries before the job is considered failed
    backoff: Any = None  # int ms, or {"type": "fixed"|"exponential", "delay": ms}
    priority: int = 0  # higher = more urgent (global order); 0 = default, FIFO
    # Auto-removal: None/False keep all · True remove on finish · int keep last N ·
    # {"count": N, "age": seconds} keep within count and/or age.
    remove_on_complete: Any = None
    remove_on_fail: Any = None

    def to_dict(self) -> dict:
        return {
            "delay": self.delay,
            "attempts": self.attempts,
            "backoff": self.backoff,
            "priority": self.priority,
            "removeOnComplete": self.remove_on_complete,
            "removeOnFail": self.remove_on_fail,
        }

    @classmethod
    def from_dict(cls, d: dict) -> JobOptions:
        return cls(
            delay=d.get("delay", 0),
            attempts=d.get("attempts", 1),
            backoff=d.get("backoff"),
            priority=d.get("priority", 0),
            remove_on_complete=d.get("removeOnComplete"),
            remove_on_fail=d.get("removeOnFail"),
        )

    @staticmethod
    def keep_args(opt: Any) -> tuple[int, int]:
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
    state: str | None = None
    processed_on: int | None = None
    finished_on: int | None = None
    progress: Any = None
    stacktrace: str | None = None
    # Back-reference to the owning Queue, set on jobs returned by Queue.add() so
    # producers can `await job.result()`. Not part of the job's data/identity.
    _queue: Any = field(default=None, repr=False, compare=False)
    # Worker-side context (redis, jobKey, eventsKey, logsKey, jobId), set while a
    # processor runs so the handler can report progress and append logs.
    _ctx: Any = field(default=None, repr=False, compare=False)

    async def result(self, *, timeout: float = 30.0) -> Any:
        """Wait for this job to finish; return its value or raise JobFailedError."""
        if self._queue is None:
            raise RuntimeError("job.result() requires a job returned by Queue.add()")
        return await self._queue.result(self.id, timeout=timeout)

    async def update_progress(self, value: Any) -> None:
        """Report progress (a number 0-100 or any JSON value) from a processor."""
        if self._ctx is None:
            raise RuntimeError("update_progress() is only available inside a worker processor")
        redis, job_key, events_key, _logs_key, jid = self._ctx
        self.progress = value
        await redis.hset(job_key, "progress", json.dumps(value))
        await redis.publish(
            events_key, json.dumps({"jobId": jid, "event": "progress", "progress": value})
        )

    async def log(self, message: str) -> None:
        """Append a log line to this job (visible in the dashboard)."""
        if self._ctx is None:
            raise RuntimeError("log() is only available inside a worker processor")
        redis, _job_key, _events_key, logs_key, _jid = self._ctx
        await redis.rpush(logs_key, message)

    @classmethod
    def from_hash(cls, job_id: str, h: dict) -> Job:
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
            state=h.get("state"),
            processed_on=int(h["processedOn"]) if h.get("processedOn") else None,
            finished_on=int(h["finishedOn"]) if h.get("finishedOn") else None,
            progress=json.loads(h["progress"]) if h.get("progress") else None,
            stacktrace=h.get("stacktrace"),
        )
