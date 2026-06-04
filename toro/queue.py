"""Queue: the producer side. Adds jobs and inspects their state."""
from __future__ import annotations

import json
import time
from typing import Any

import redis.asyncio as aioredis

from . import scripts
from .job import Job, JobOptions
from .keys import Keys


def _now_ms() -> int:
    return int(time.time() * 1000)


class Queue:
    def __init__(
        self,
        name: str,
        *,
        connection: aioredis.Redis | None = None,
        url: str = "redis://localhost:6379",
        prefix: str = "toro",
    ):
        self.name = name
        self.keys = Keys(name, prefix)
        self.redis = connection or aioredis.from_url(url, decode_responses=True)
        self._add_job = self.redis.register_script(scripts.ADD_JOB)
        self._retry_job = self.redis.register_script(scripts.RETRY_JOB)
        self._remove_job = self.redis.register_script(scripts.REMOVE_JOB)

    # States backed by a Redis LIST vs a ZSET — affects how we page through them.
    _LIST_STATES = {"wait", "active"}
    _ZSET_STATES = {"delayed", "completed", "failed"}

    async def add(self, name: str, data: Any = None, **opts: Any) -> Job:
        """Enqueue a job. Returns the created Job (with its server-assigned id)."""
        options = JobOptions(**opts)
        now = _now_ms()
        job_id = await self._add_job(
            keys=[self.keys.id, self.keys.wait, self.keys.delayed, self.keys.base],
            args=[
                name,
                json.dumps(data),
                json.dumps(options.to_dict()),
                now,
                options.delay,
            ],
        )
        job_id = str(job_id)
        return Job(id=job_id, name=name, data=data, opts=options, timestamp=now,
                   state="delayed" if options.delay > 0 else "wait")

    async def get_job(self, job_id: str) -> Job | None:
        h = await self.redis.hgetall(self.keys.job(job_id))
        if not h:
            return None
        return Job.from_hash(job_id, h)

    async def counts(self) -> dict[str, int]:
        """Quick snapshot of how many jobs sit in each state."""
        pipe = self.redis.pipeline()
        pipe.llen(self.keys.wait)
        pipe.llen(self.keys.active)
        pipe.zcard(self.keys.delayed)
        pipe.zcard(self.keys.completed)
        pipe.zcard(self.keys.failed)
        wait, active, delayed, completed, failed = await pipe.execute()
        return {
            "wait": wait, "active": active, "delayed": delayed,
            "completed": completed, "failed": failed,
        }

    async def get_jobs(self, state: str, start: int = 0, end: int = 20) -> list[Job]:
        """Page through job ids in a given state and hydrate them into Jobs."""
        key = getattr(self.keys, state, None)
        if key is None:
            raise ValueError(f"unknown state: {state}")
        if state in self._LIST_STATES:
            ids = await self.redis.lrange(key, start, end)
        else:
            # newest first for finished states; ascending time for delayed
            ids = await (
                self.redis.zrange(key, start, end)
                if state == "delayed"
                else self.redis.zrevrange(key, start, end)
            )
        jobs = []
        for job_id in ids:
            job = await self.get_job(job_id)
            if job is not None:
                jobs.append(job)
        return jobs

    async def retry_job(self, job_id: str) -> bool:
        """Move a failed job back to wait for another attempt."""
        res = await self._retry_job(
            keys=[self.keys.failed, self.keys.wait, self.keys.job(job_id)],
            args=[job_id],
        )
        return bool(res)

    async def remove_job(self, job_id: str) -> bool:
        """Delete a job from every state and drop its hash."""
        res = await self._remove_job(
            keys=[
                self.keys.wait, self.keys.active, self.keys.delayed,
                self.keys.completed, self.keys.failed, self.keys.job(job_id),
            ],
            args=[job_id],
        )
        return bool(res)

    async def close(self) -> None:
        await self.redis.aclose()
