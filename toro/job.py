"""The Job: a typed view over the Redis hash that stores one unit of work."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class JobOptions:
    delay: int = 0            # ms to wait before the job becomes processable
    attempts: int = 1         # total tries before the job is considered failed
    backoff: Any = None       # int ms, or {"type": "fixed"|"exponential", "delay": ms}

    def to_dict(self) -> dict:
        return {"delay": self.delay, "attempts": self.attempts, "backoff": self.backoff}

    @classmethod
    def from_dict(cls, d: dict) -> JobOptions:
        return cls(
            delay=d.get("delay", 0),
            attempts=d.get("attempts", 1),
            backoff=d.get("backoff"),
        )


@dataclass
class Job:
    id: str
    name: str
    data: Any
    opts: JobOptions = field(default_factory=JobOptions)
    attempts_made: int = 0
    timestamp: int | None = None
    returnvalue: Any = None
    failed_reason: str | None = None
    state: str | None = None

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
        )
