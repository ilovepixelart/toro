"""Worker: the consumer side. Pulls jobs and runs a processor over them.

Reliability model (this is the core — see DESIGN.md):
  * A blocking BLMOVE atomically moves a job id from `wait` to `active`, so a
    job is never lost in the gap between "pop" and "start working".
  * On pickup the worker locks the job: `<id>:lock = <token> PX lockDuration`.
    Only the token owner can renew or finish it. A per-job renewer extends the
    lock every lockDuration/2.
  * If a worker dies, its lock expires; a background mark-and-sweep moves the
    job back to `wait` (or to `failed` after maxStalledCount). This gives an
    at-least-once guarantee: a handler may run more than once, but the
    token-guarded finish scripts ensure a result is committed exactly once.
  * A background task promotes due delayed jobs into `wait`.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import redis.asyncio as aioredis

from . import scripts
from .job import Job
from .keys import Keys

Processor = Callable[[Job], Awaitable[Any]]


def _now_ms() -> int:
    return int(time.time() * 1000)


class Worker:
    def __init__(
        self,
        name: str,
        processor: Processor,
        *,
        connection: aioredis.Redis | None = None,
        url: str = "redis://localhost:6379",
        prefix: str = "toro",
        concurrency: int = 1,
        block_timeout: float = 5.0,
        lock_duration: int = 30000,
        lock_renew_time: int | None = None,
        renew_locks: bool = True,
        stalled_interval: int = 30000,
        max_stalled_count: int = 1,
    ):
        self.name = name
        self.processor = processor
        self.keys = Keys(name, prefix)
        self.redis = connection or aioredis.from_url(url, decode_responses=True)
        self.concurrency = concurrency
        self.block_timeout = block_timeout

        # Reliability knobs.
        self.token = uuid.uuid4().hex
        self.lock_duration = lock_duration
        self.lock_renew_time = lock_renew_time or lock_duration // 2
        self.renew_locks = renew_locks
        self.stalled_interval = stalled_interval
        self.max_stalled_count = max_stalled_count

        self._running = False
        self._tasks: list[asyncio.Task] = []

        self._lock_job = self.redis.register_script(scripts.LOCK_JOB)
        self._extend_lock = self.redis.register_script(scripts.EXTEND_LOCK)
        self._move_to_completed = self.redis.register_script(scripts.MOVE_TO_COMPLETED)
        self._move_to_failed = self.redis.register_script(scripts.MOVE_TO_FAILED)
        self._move_stalled = self.redis.register_script(scripts.MOVE_STALLED)
        self._promote_delayed = self.redis.register_script(scripts.PROMOTE_DELAYED)

        # Simple event callbacks: worker.on("completed", fn)
        self._handlers: dict[str, list[Callable]] = {}

    def on(self, event: str, fn: Callable) -> None:
        self._handlers.setdefault(event, []).append(fn)

    def _emit(self, event: str, *args: Any) -> None:
        for fn in self._handlers.get(event, []):
            fn(*args)

    async def run(self) -> None:
        """Start processing until stop() is called. Awaitable forever."""
        self._running = True
        self._tasks = [
            asyncio.create_task(self._promote_loop()),
            *[asyncio.create_task(self._process_loop()) for _ in range(self.concurrency)],
        ]
        if self.stalled_interval > 0:
            self._tasks.append(asyncio.create_task(self._stalled_loop()))
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.redis.aclose()

    async def _promote_loop(self) -> None:
        while self._running:
            try:
                await self._promote_delayed(
                    keys=[self.keys.delayed, self.keys.wait, self.keys.base],
                    args=[_now_ms()],
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - best-effort background sweep
                pass
            await asyncio.sleep(1.0)

    async def _stalled_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.stalled_interval / 1000)
            try:
                failed, recovered = await self.check_stalled()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - best-effort background sweep
                continue
            for job_id in recovered:
                self._emit("stalled", job_id)
            for job_id in failed:
                self._emit("failed", job_id, RuntimeError("job stalled too many times"))

    async def check_stalled(self, throttle_ms: int | None = None) -> tuple[list[str], list[str]]:
        """Run one mark-and-sweep pass. Returns (failed_ids, recovered_ids).

        `throttle_ms=0` bypasses the cross-worker throttle (used by tests); by
        default the throttle is `stalled_interval` so concurrent workers don't
        all sweep at once.
        """
        throttle = self.stalled_interval if throttle_ms is None else throttle_ms
        res = await self._move_stalled(
            keys=[
                self.keys.stalled, self.keys.active, self.keys.wait,
                self.keys.failed, self.keys.stalled_check, self.keys.base,
            ],
            args=[self.max_stalled_count, _now_ms(), throttle],
        )
        failed = list(res[0]) if res else []
        recovered = list(res[1]) if res and len(res) > 1 else []
        return failed, recovered

    async def _process_loop(self) -> None:
        while self._running:
            try:
                job_id = await self.redis.blmove(
                    self.keys.wait, self.keys.active, self.block_timeout, "RIGHT", "LEFT"
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(0.1)
                continue
            if job_id is None:
                continue
            await self._handle(str(job_id))

    async def _handle(self, job_id: str) -> None:
        # Lock the job to this worker the instant after the blocking pop.
        attempts_made = await self._lock_job(
            keys=[self.keys.job(job_id), self.keys.lock(job_id), self.keys.stalled],
            args=[self.token, self.lock_duration, _now_ms(), job_id],
        )

        h = await self.redis.hgetall(self.keys.job(job_id))
        job = Job.from_hash(job_id, h)
        job.attempts_made = attempts_made

        # Keep the lock alive while the handler runs.
        renewer = (
            asyncio.create_task(self._renew_loop(job_id)) if self.renew_locks else None
        )
        try:
            result = await self.processor(job)
        except Exception as exc:
            await self._finish_failed(job, exc)
        else:
            await self._finish_completed(job, result)
        finally:
            if renewer is not None:
                renewer.cancel()

    async def _renew_loop(self, job_id: str) -> None:
        interval = self.lock_renew_time / 1000
        while True:
            await asyncio.sleep(interval)
            try:
                ok = await self._extend_lock(
                    keys=[self.keys.lock(job_id), self.keys.stalled],
                    args=[self.token, self.lock_duration, job_id],
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover
                ok = 0
            if not ok:
                # Lock lost: another worker now owns this job. Our eventual
                # finish will be rejected by the token guard.
                self._emit("lock-lost", job_id)
                return

    async def _finish_completed(self, job: Job, result: Any) -> None:
        res = await self._move_to_completed(
            keys=[
                self.keys.active, self.keys.completed,
                self.keys.job(job.id), self.keys.lock(job.id),
            ],
            args=[job.id, json.dumps(result), _now_ms(), self.token],
        )
        if res == 1:
            job.returnvalue = result
            self._emit("completed", job, result)
        else:
            self._emit("lock-lost", job.id)  # taken over; do not double-commit

    async def _finish_failed(self, job: Job, exc: Exception) -> None:
        backoff_ms = self._backoff_delay(job)
        res = await self._move_to_failed(
            keys=[
                self.keys.active, self.keys.wait, self.keys.delayed,
                self.keys.failed, self.keys.job(job.id), self.keys.lock(job.id),
            ],
            args=[
                job.id, str(exc), _now_ms(),
                job.attempts_made, job.opts.attempts, backoff_ms, self.token,
            ],
        )
        job.failed_reason = str(exc)
        if res == 1:
            self._emit("failed", job, exc)
        elif res == 0:
            self._emit("retrying", job, exc)
        else:
            self._emit("lock-lost", job.id)  # taken over; do not double-commit

    def _backoff_delay(self, job: Job) -> int:
        """Translate the job's backoff option into a delay in ms for the next try."""
        bo = job.opts.backoff
        if not bo:
            return 0
        if isinstance(bo, (int, float)):
            return int(bo)
        delay = bo.get("delay", 0)
        if bo.get("type") == "exponential":
            return int(delay * (2 ** (job.attempts_made - 1)))
        return int(delay)
