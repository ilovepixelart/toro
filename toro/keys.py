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
    def wait(self) -> str:
        return f"{self.base}wait"

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
