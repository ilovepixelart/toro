"""Worker: the consumer side. Pulls jobs and runs a processor over them.

Reliability model (this is the core — see DESIGN.md):
  * A blocking BLMOVE wakes the worker and moves a job id from `wait` to
    `active`. `MOVE_TO_ACTIVE` then locks + loads it.
  * Job acquisition (lock + load) funnels through ONE Lua routine, shared by the
    blocking path and by fetch-next. That routine is the seed of a future
    `moveToActive`: to add priorities/markers we change only which job it picks.
  * Fetch-next: the finish scripts commit the current job AND acquire the next
    one in the same round trip, so a busy worker loops without going back to the
    blocking pop. It only re-blocks when the queue drains.
  * On pickup the worker locks the job (`<id>:lock = <token> PX lockDuration`)
    and a renewer extends it. If a worker dies, its lock expires and a background
    mark-and-sweep recovers the job. Token-guarded finishes guarantee a result
    is committed exactly once even though a handler may run more than once.
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


def _pairs(flat: list | None) -> dict:
    """Turn a flat HGETALL array [k, v, k, v, ...] into a dict."""
    if not flat:
        return {}
    it = iter(flat)
    return dict(zip(it, it, strict=False))


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

        self._move_to_active = self.redis.register_script(scripts.MOVE_TO_ACTIVE)
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

    # ---- the hot path -----------------------------------------------------

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
            loaded = await self._acquire(str(job_id))
            # Keep processing as long as each finish hands us the next job.
            while loaded is not None and self._running:
                loaded = await self._handle(loaded)

    async def _acquire(self, job_id: str) -> tuple[str, dict] | None:
        """Lock + load a job already on `active` (the blocking-wakeup path)."""
        res = await self._move_to_active(
            keys=[self.keys.stalled, self.keys.base],
            args=[job_id, self.token, self.lock_duration, _now_ms()],
        )
        return self._loaded(res)

    def _loaded(self, res) -> tuple[str, dict] | None:
        if not res:
            return None
        fields = _pairs(res[0])
        if not fields:
            return None
        return (res[1], fields)

    async def _handle(self, loaded: tuple[str, dict]) -> tuple[str, dict] | None:
        job_id, fields = loaded
        job = Job.from_hash(job_id, fields)
        renewer = (
            asyncio.create_task(self._renew_loop(job_id)) if self.renew_locks else None
        )
        try:
            result = await self.processor(job)
        except Exception as exc:
            nxt = await self._finish_failed(job, exc)
        else:
            nxt = await self._finish_completed(job, result)
        finally:
            if renewer is not None:
                renewer.cancel()
        return nxt

    async def _finish_completed(self, job: Job, result: Any) -> tuple[str, dict] | None:
        res = await self._move_to_completed(
            keys=[
                self.keys.active, self.keys.completed, self.keys.job(job.id),
                self.keys.lock(job.id), self.keys.wait, self.keys.stalled, self.keys.base,
            ],
            args=[
                job.id, json.dumps(result), _now_ms(), self.token,
                self._fetch_flag(), self.lock_duration,
            ],
        )
        if isinstance(res, int):  # -2 lock lost / -3 not active
            self._emit("lock-lost", job.id)
            return None
        job.returnvalue = result
        self._emit("completed", job, result)
        return self._next_from(res)

    async def _finish_failed(self, job: Job, exc: Exception) -> tuple[str, dict] | None:
        res = await self._move_to_failed(
            keys=[
                self.keys.active, self.keys.wait, self.keys.delayed, self.keys.failed,
                self.keys.job(job.id), self.keys.lock(job.id), self.keys.stalled, self.keys.base,
            ],
            args=[
                job.id, str(exc), _now_ms(), job.attempts_made, job.opts.attempts,
                self._backoff_delay(job), self.token, self._fetch_flag(), self.lock_duration,
            ],
        )
        if isinstance(res, int):  # -2 lock lost / -3 not active
            self._emit("lock-lost", job.id)
            return None
        job.failed_reason = str(exc)
        self._emit("failed" if res[0] == 1 else "retrying", job, exc)
        return self._next_from(res)

    def _fetch_flag(self) -> str:
        # Don't fetch a next job while shutting down — let the queue drain cleanly.
        return "1" if self._running else "0"

    def _next_from(self, res) -> tuple[str, dict] | None:
        if isinstance(res, (list, tuple)) and len(res) >= 3:
            return (res[2], _pairs(res[1]))
        return None

    # ---- locks & recovery -------------------------------------------------

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
                self._emit("lock-lost", job_id)
                return

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
