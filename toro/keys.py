"""Central place that knows how Redis keys are laid out for a queue.

Keeping this in one spot means the Lua scripts and the Python side can never
disagree about where a list/zset/hash lives.
"""
from __future__ import annotations


class Keys:
    def __init__(self, queue_name: str, prefix: str = "toro"):
        self.queue_name = queue_name
        self.prefix = prefix
        self.base = f"{prefix}:{queue_name}:"

    @property
    def id(self) -> str:
        return f"{self.base}id"

    @property
    def prioritized(self) -> str:
        # The single global-priority-ordered store of waiting jobs.
        return f"{self.base}prioritized"

    @property
    def marker(self) -> str:
        # Wakeup signal: idle workers BZPOPMIN here; producers ZADD an idempotent base marker.
        return f"{self.base}marker"

    @property
    def pc(self) -> str:
        # Priority sequence counter — breaks priority ties in FIFO order.
        return f"{self.base}pc"

    @property
    def active(self) -> str:
        return f"{self.base}active"

    @property
    def delayed(self) -> str:
        return f"{self.base}delayed"

    @property
    def completed(self) -> str:
        return f"{self.base}completed"

    @property
    def failed(self) -> str:
        return f"{self.base}failed"

    @property
    def stalled(self) -> str:
        return f"{self.base}stalled"

    @property
    def stalled_check(self) -> str:
        return f"{self.base}stalled-check"

    def job(self, job_id: str | int) -> str:
        return f"{self.base}{job_id}"

    def lock(self, job_id: str | int) -> str:
        return f"{self.base}{job_id}:lock"
