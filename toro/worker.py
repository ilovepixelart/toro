"""Worker: the consumer side. Pulls jobs and runs a processor over them.

Reliability model (this is the core - see docs/architecture.md):
  * Jobs live in one `prioritized` ZSET (global priority order). A parked worker
    wakes on `BZPOPMIN` of a 0-scored base marker; the atomic claim is
    `MOVE_TO_ACTIVE` (`ZPOPMIN prioritized` → `active` → lock + load). The marker
    only wakes us - a missed marker can't strand a job, since the claim is atomic.
  * Job acquisition (claim + lock + load) funnels through ONE Lua routine, shared
    by the blocking-wakeup path and by fetch-next.
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
import contextlib
import json
import logging
import os
import socket
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, TypedDict, cast

from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from . import scripts
from ._replies import _str_list
from .connection import DEFAULT_BLOCK_TIMEOUT, confirm_subscribed, connect, read_timeout
from .job import Backoff, Job, JobContext
from .keys import Keys
from .scheduler import next_run

Processor = Callable[[Job], Awaitable[Any]]

logger = logging.getLogger(__name__)


class RateLimit(TypedDict):
    """The queue-wide token bucket: ``{"max": N, "duration": ms}`` - at most N
    job starts per duration, shared by every worker on the queue.
    """

    max: int
    duration: int


def _now_ms() -> int:
    return int(time.time() * 1000)


def _pairs(flat: list[str] | None) -> dict[str, str]:
    """Turn a flat HGETALL array [k, v, k, v, ...] into a dict."""
    if not flat:
        return {}
    it = iter(flat)
    return dict(zip(it, it, strict=False))


def pop_timeout(read_timeout_s: float | None, block_timeout: float) -> float:
    """How long the blocking pop may block: `block_timeout`, held under the
    connection's read timeout. A pop that outlasts the read raises instead of
    timing out quietly, the loop never reaches the claim, and a job whose wake was
    missed is stranded. A connection the worker built has room by construction; a
    caller-provided one can carry any read timeout.
    """
    if read_timeout_s is None:
        return block_timeout
    ceiling = max(read_timeout_s - 1.0, read_timeout_s / 2)
    return min(block_timeout, ceiling)


# How long a presence record outlives its worker's last heartbeat. Long enough that a
# dashboard opened the next day still finds a crashed worker and logs it as lost.
PRESENCE_TTL_MS = 24 * 60 * 60 * 1000


def compute_backoff(backoff: Backoff, attempts_made: int) -> int:
    """Delay (ms) before the next attempt. `backoff` is None/0, an int (fixed ms),
    or {"type": "fixed"|"exponential", "delay": ms}. Exponential doubles per attempt.
    Pure function so it can be unit-tested without a Redis-bound Worker.
    """
    if not backoff:
        return 0
    if isinstance(backoff, (int, float)):
        return int(backoff)
    delay = backoff.get("delay", 0)
    if backoff.get("type") == "exponential":
        return int(delay * (2 ** (attempts_made - 1)))
    return int(delay)


class Worker:
    """The consumer side: claims jobs, runs the processor, and recovers stalls."""

    def __init__(
        self,
        name: str,
        processor: Processor,
        *,
        connection: Redis | None = None,
        url: str = "redis://localhost:6379",
        prefix: str = "toro",
        concurrency: int = 1,
        rate_limit: RateLimit | None = None,
        global_concurrency: int | None = None,
        block_timeout: float = DEFAULT_BLOCK_TIMEOUT,
        lock_duration: int = 30000,
        lock_renew_time: int | None = None,
        renew_locks: bool = True,
        stalled_interval: int = 30000,
        max_stalled_count: int = 1,
        grace_period: float = 30.0,
        heartbeat_interval: int = 5000,
    ) -> None:
        self.name = name
        self.processor = processor
        self.keys = Keys(name, prefix)
        # Each process loop PARKS a connection inside BZPOPMIN, so the pool must
        # exceed the concurrency or loops starve waiting for connections. A
        # caller-provided connection must be sized accordingly by the caller.
        self.redis = connection or connect(
            url, max_connections=max(50, concurrency + 10), blocking_timeout=block_timeout
        )
        # A connection we opened is ours to give back when we stop; one handed to us
        # belongs to the caller, who may still be using it elsewhere.
        self._owns_connection = connection is None
        self.concurrency = concurrency
        # Queue-wide rate limit, shared by all workers via one token bucket in Redis.
        # `{"max": N, "duration": ms}` = at most N jobs per duration. All workers on a
        # queue should pass the SAME config so the shared bucket behaves consistently.
        if rate_limit is not None and (
            int(rate_limit.get("max", 0)) <= 0 or int(rate_limit.get("duration", 0)) <= 0
        ):
            raise ValueError("rate_limit needs {'max': positive, 'duration': positive ms}")
        self.rl_max = int(rate_limit["max"]) if rate_limit else 0
        self.rl_duration = int(rate_limit["duration"]) if rate_limit else 0
        # Queue-wide cap on jobs active at once, across every worker process. Like
        # rate_limit, all workers on a queue should pass the SAME value. bool is an
        # int subclass, so it is rejected by name: True would silently mean 1.
        if global_concurrency is not None and (
            isinstance(global_concurrency, bool)
            or not isinstance(global_concurrency, int)
            or global_concurrency <= 0
        ):
            raise ValueError("global_concurrency needs a positive integer")
        # int(): an int subclass (an IntEnum) would reach Redis as its repr, which
        # Lua reads as no number at all.
        self.global_concurrency = int(global_concurrency or 0)
        self.block_timeout = block_timeout
        self._pop_timeout = pop_timeout(read_timeout(self.redis), block_timeout)
        if self._pop_timeout < block_timeout:
            logger.warning(
                "block_timeout %.2fs does not fit under the connection's read timeout; "
                "idle workers re-poll every %.2fs instead",
                block_timeout,
                self._pop_timeout,
            )

        # Reliability knobs.
        self.token = uuid.uuid4().hex
        self.lock_duration = lock_duration
        self.lock_renew_time = lock_renew_time or lock_duration // 2
        self.renew_locks = renew_locks
        self.stalled_interval = stalled_interval
        self.max_stalled_count = max_stalled_count
        self.grace_period = grace_period
        self.heartbeat_interval = heartbeat_interval

        self._running = False
        self._tasks: list[asyncio.Task[None]] = []
        self._process_tasks: list[asyncio.Task[None]] = []

        # Presence + throughput for the "workers" view; flushed to Redis each heartbeat.
        self.started_at = 0
        self._processed = 0
        self._failed = 0
        self._cancelled = 0
        self._current: set[str] = set()
        # The processor task of each running job, so a cancellation can reach it, and
        # the jobs whose cancellation THIS worker asked for: a CancelledError that is
        # not in here is the worker shutting down, which must not commit a cancel.
        self._processors: dict[str, asyncio.Task[Any]] = {}
        self._cancelling: set[str] = set()
        # "running" until a graceful stop flips it to "stopping" - the dashboard shows
        # a live "draining" state, and a worker that then vanishes was mid-shutdown,
        # not a crash. (The only honest way to know graceful; absence can't say why.)
        self._state = "running"

        self._move_to_active = self.redis.register_script(scripts.MOVE_TO_ACTIVE)
        self._extend_lock = self.redis.register_script(scripts.EXTEND_LOCK)
        self._move_to_completed = self.redis.register_script(scripts.MOVE_TO_COMPLETED)
        self._move_to_failed = self.redis.register_script(scripts.MOVE_TO_FAILED)
        self._move_to_cancelled = self.redis.register_script(scripts.MOVE_TO_CANCELLED)
        self._move_stalled = self.redis.register_script(scripts.MOVE_STALLED)
        self._promote_delayed = self.redis.register_script(scripts.PROMOTE_DELAYED)
        self._add_scheduled = self.redis.register_script(scripts.ADD_SCHEDULED)

        # Simple event callbacks: worker.on("completed", fn)
        self._handlers: dict[str, list[Callable[..., Any]]] = {}

    def on(self, event: str, fn: Callable[..., Any]) -> None:
        self._handlers.setdefault(event, []).append(fn)

    def _emit(self, event: str, *args: Any) -> None:
        for fn in self._handlers.get(event, []):
            try:
                fn(*args)
            except Exception:  # noqa: PERF203 - per-callback isolation is the point
                # A user callback must never hurt the worker: the job outcome is
                # already committed by the time events fire, so log and move on.
                logger.exception("%r event handler raised", event)

    async def run(self) -> None:
        """Start processing until stop() is called. Awaitable forever."""
        self._running = True
        self.started_at = _now_ms()
        await self._write_heartbeat()  # register at once so the worker shows up immediately
        # Subscribed BEFORE the first claim: a job this worker is running has to be one
        # it can hear a cancellation for, or the request waits out a lock renewal.
        cancels = await self._subscribe_cancels()
        self._process_tasks = [
            asyncio.create_task(self._process_loop()) for _ in range(self.concurrency)
        ]
        bg = [
            asyncio.create_task(self._promote_loop()),
            asyncio.create_task(self._cancel_listener(cancels)),
        ]
        if self.stalled_interval > 0:
            bg.append(asyncio.create_task(self._stalled_loop()))
        if self.heartbeat_interval > 0:
            bg.append(asyncio.create_task(self._heartbeat_loop()))
        self._tasks = [*self._process_tasks, *bg]
        with contextlib.suppress(asyncio.CancelledError):
            # return_exceptions: one freak task failure must not crash run() and
            # take every other slot down with it (each loop also guards itself).
            results = await asyncio.gather(*self._tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    logger.error("worker task died: %r", res)

    async def stop(self, grace_period: float | None = None) -> None:
        """Graceful shutdown: stop fetching new jobs, let in-flight jobs finish
        (up to `grace_period` seconds), then cancel the rest and disconnect.
        """
        grace = self.grace_period if grace_period is None else grace_period
        self._running = False
        # Flip to "stopping" (shown as "draining" in the dashboard) and flush it now, so
        # this worker reads as shutting down in real time (a later vanish = graceful, not crash).
        self._state = "stopping"
        with contextlib.suppress(Exception):
            await self._write_heartbeat()
        # Wake an idle worker parked on BZPOPMIN so it notices the shutdown.
        with contextlib.suppress(Exception):  # pragma: no cover
            await self.redis.zadd(self.keys.marker, {"0": 0})
        # Let process loops drain their current job and exit on their own.
        if self._process_tasks:
            await asyncio.wait(self._process_tasks, timeout=grace)
        # Force-cancel anything left (jobs past the grace period + background loops).
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self._deregister()  # drop our presence record so we vanish at once
        await self.redis.aclose(close_connection_pool=self._owns_connection)

    # ---- presence / heartbeat ---------------------------------------------

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.heartbeat_interval / 1000)
            with contextlib.suppress(Exception):
                await self._write_heartbeat()

    async def _write_heartbeat(self) -> None:
        """Flush this worker's presence record and register it as live."""
        now = _now_ms()
        pipe = self.redis.pipeline(transaction=False)
        pipe.hset(
            self.keys.worker(self.token),
            mapping={
                "id": self.token,
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "queue": self.name,
                "concurrency": self.concurrency,
                "global_concurrency": self.global_concurrency,
                "started": self.started_at,
                "heartbeat": now,
                "processed": self._processed,
                "failed": self._failed,
                "current": json.dumps(sorted(self._current)),
                "state": self._state,
            },
        )
        pipe.zadd(self.keys.workers, {self.token: now})
        # Dead workers are pruned by whoever reads workers() - a dashboard. With no
        # reader, a worker killed without deregistering would leave its record for
        # good: so the record expires, and the index drops what has outlived it.
        pipe.pexpire(self.keys.worker(self.token), PRESENCE_TTL_MS)
        pipe.zremrangebyscore(self.keys.workers, "-inf", now - PRESENCE_TTL_MS)
        await pipe.execute()

    async def _deregister(self) -> None:
        await self._record_departure("stopped")  # graceful shutdown
        await self.redis.zrem(self.keys.workers, self.token)
        await self.redis.delete(self.keys.worker(self.token))

    async def _record_departure(self, reason: str) -> None:
        """Append to the capped death-log so the dashboard can show what left and why."""
        now = _now_ms()
        rec = json.dumps(
            {
                "id": self.token,
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "queue": self.name,
                "concurrency": self.concurrency,
                "processed": self._processed,
                "failed": self._failed,
                "started": self.started_at,
                "last_seen": now,
                "current": sorted(self._current),  # what it was running at the end
                "reason": reason,
                "at": now,
            }
        )
        await self.redis.lpush(self.keys.departed, rec)
        await self.redis.ltrim(self.keys.departed, 0, 49)

    # ---- the hot path -----------------------------------------------------

    async def _process_loop(self) -> None:
        # One guard around the WHOLE iteration: a transient Redis error, a corrupt
        # job hash, or anything else unexpected costs one beat, never the slot. A
        # job interrupted mid-flight stays locked in `active` until its lock
        # expires and the stalled sweep recovers it - the normal at-least-once path.
        while self._running:
            try:
                # The marker only wakes us; the real claim is the atomic
                # MOVE_TO_ACTIVE below. A timeout (None) is fine - we still try
                # to acquire, so a missed marker can never strand a job.
                woke = await self.redis.bzpopmin(self.keys.marker, self._pop_timeout)
                if not self._running:
                    # Shutting down - don't claim a new job. A marker we popped was
                    # a wake for a worker that still can, so hand it on: swallowed,
                    # the work it signalled waits out someone's block_timeout.
                    if woke:
                        await self.redis.zadd(self.keys.marker, {"0": 0})
                    break
                loaded = await self._acquire()
                # Keep processing as long as each finish hands us the next job. No
                # `_running` check here: a job in hand is already claimed, and stop()
                # can land during the very round trip that claimed it. Dropped, it
                # would sit locked in `active` until the sweep. Shutdown ends the
                # chain by itself: a stopping worker finishes with fetch=0.
                while loaded is not None:
                    loaded = await self._handle(loaded)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("process loop hiccup; the slot lives on")
                await asyncio.sleep(0.1)

    async def _acquire(self) -> tuple[str, dict[str, str]] | None:
        """Pop the highest-priority job into `active`, lock + load it."""
        res = await self._move_to_active(
            keys=[
                self.keys.prioritized,
                self.keys.active,
                self.keys.marker,
                self.keys.stalled,
                self.keys.base,
                self.keys.pc,
                self.keys.meta_paused,
                self.keys.limiter,
            ],
            args=[
                self.token,
                self.lock_duration,
                _now_ms(),
                self.rl_max,
                self.rl_duration,
                self.global_concurrency,
            ],
        )
        if res and res[0] == scripts.RL_SENTINEL:
            await self._on_rate_limited(int(res[1]))
            return None
        return self._loaded(res)

    async def _on_rate_limited(self, retry_ms: int) -> None:
        """Rate limited: wait until a token frees up (the job stays queued, no
        attempt consumed), then re-arm the marker so we re-check immediately.
        Capped at block_timeout so shutdown stays responsive on long waits.
        """
        self._emit("rate-limited", retry_ms)
        await asyncio.sleep(min(retry_ms, self.block_timeout * 1000) / 1000)
        if self._running:
            with contextlib.suppress(Exception):  # pragma: no cover
                await self.redis.zadd(self.keys.marker, {"0": 0})

    def _loaded(self, res: list[Any] | None) -> tuple[str, dict[str, str]] | None:
        if not res:
            return None
        fields = _pairs(res[0])
        if not fields:
            return None
        return (str(res[1]), fields)

    async def _handle(
        self, loaded: tuple[str, dict[str, str]]
    ) -> tuple[str, dict[str, str]] | None:
        job_id, fields = loaded
        job = Job.from_hash(job_id, fields)
        # Give the handler the ability to report progress and append logs.
        job._ctx = JobContext(  # noqa: SLF001  - the worker injects the job's runtime context
            redis=self.redis,
            job_key=self.keys.job(job_id),
            events_key=self.keys.events,
            logs_key=self.keys.logs(job_id),
            job_id=job_id,
            results_key=self.keys.results(job_id),
            cfail_key=self.keys.cfail(job_id),
        )
        # A scheduler job mints its successor on first pickup, so the schedule
        # stays on time regardless of how long (or whether) this run succeeds.
        if fields.get("schedulerId") and job.attempts_made == 1:
            await self._schedule_next(fields["schedulerId"])
        renewer = asyncio.create_task(self._renew_loop(job_id)) if self.renew_locks else None
        self._current.add(job_id)  # so the heartbeat reports what we're running

        # In its own task so a cancellation has something to land on: awaited inline,
        # there is nothing to stop but the process loop itself. Wrapped because a
        # processor is any awaitable, and only a coroutine can become a task.
        async def run_processor() -> Any:
            return await self.processor(job)

        task = asyncio.create_task(run_processor())
        self._processors[job_id] = task
        try:
            result = await task
        except asyncio.CancelledError:
            if job_id not in self._cancelling:
                raise  # the worker is going down, not a cancellation: let it through
            self._cancelled += 1
            nxt = await self._finish_cancelled(job)
        except Exception as exc:
            await self.redis.hset(self.keys.job(job_id), "stacktrace", traceback.format_exc())
            self._failed += 1
            nxt = await self._finish_failed(job, exc)
        else:
            self._processed += 1
            nxt = await self._finish_completed(job, result)
        finally:
            self._current.discard(job_id)
            self._processors.pop(job_id, None)
            self._cancelling.discard(job_id)
            if renewer is not None:
                renewer.cancel()
        return nxt

    def _request_cancel(self, job_id: str) -> None:
        """Stop a job this worker is running. Nothing to do if it is not ours, or if
        it already finished: the cancellation was simply too late.
        """
        task = self._processors.get(job_id)
        if task is None or task.done():
            return
        self._cancelling.add(job_id)
        task.cancel()

    async def _finish_cancelled(self, job: Job) -> tuple[str, dict[str, str]] | None:
        res = await self._move_to_cancelled(
            keys=[
                self.keys.active,
                self.keys.cancelled,
                self.keys.job(job.id),
                self.keys.lock(job.id),
                self.keys.prioritized,
                self.keys.marker,
                self.keys.base,
                self.keys.events,
            ],
            args=[job.id, _now_ms(), self.token, scripts.METRICS_RETENTION_MS],
        )
        if int(res) < 0:
            await self._finish_lost(job.id)
            return None
        self._emit("cancelled", job)
        return None

    async def _finish_lost(self, job_id: str) -> None:
        """Our finish committed nothing: the job was taken over or removed while we
        ran it. Its place in `active` was freed back then, and a removal wakes no one
        because the processor may still be running. It has ended now, so say there
        may be work: under a global concurrency cap a worker can be parked with jobs
        waiting, and nothing else would tell it this slot is really free.
        """
        self._emit("lock-lost", job_id)
        await self.redis.zadd(self.keys.marker, {"0": 0})

    async def _finish_completed(self, job: Job, result: Any) -> tuple[str, dict[str, str]] | None:
        res = await self._move_to_completed(
            keys=[
                self.keys.active,
                self.keys.completed,
                self.keys.job(job.id),
                self.keys.lock(job.id),
                self.keys.prioritized,
                self.keys.marker,
                self.keys.stalled,
                self.keys.base,
                self.keys.pc,
                self.keys.events,
                self.keys.meta_paused,
                self.keys.limiter,
            ],
            args=scripts.completed_args(
                job_id=job.id,
                returnvalue=json.dumps(result),
                now=_now_ms(),
                token=self.token,
                fetch=self._fetch_flag(),
                lock_duration=self.lock_duration,
                rl_max=self.rl_max,
                rl_duration=self.rl_duration,
                global_concurrency=self.global_concurrency,
            ),
        )
        if res in (scripts.LOCK_LOST, scripts.NOT_ACTIVE):  # finish script's int sentinel
            await self._finish_lost(job.id)
            return None
        job.returnvalue = result
        self._emit("completed", job, result)
        return self._next_from(res)

    async def _finish_failed(self, job: Job, exc: Exception) -> tuple[str, dict[str, str]] | None:
        res = await self._move_to_failed(
            keys=[
                self.keys.active,
                self.keys.prioritized,
                self.keys.delayed,
                self.keys.failed,
                self.keys.job(job.id),
                self.keys.lock(job.id),
                self.keys.marker,
                self.keys.stalled,
                self.keys.base,
                self.keys.pc,
                self.keys.events,
                self.keys.meta_paused,
                self.keys.limiter,
            ],
            args=scripts.failed_args(
                job_id=job.id,
                reason=str(exc),
                now=_now_ms(),
                attempts_made=job.attempts_made,
                max_attempts=job.opts.attempts,
                backoff=self._backoff_delay(job),
                token=self.token,
                fetch=self._fetch_flag(),
                lock_duration=self.lock_duration,
                rl_max=self.rl_max,
                rl_duration=self.rl_duration,
                global_concurrency=self.global_concurrency,
            ),
        )
        if res in (scripts.LOCK_LOST, scripts.NOT_ACTIVE):  # finish script's int sentinel
            await self._finish_lost(job.id)
            return None
        job.failed_reason = str(exc)
        self._emit("failed" if res[0] == scripts.OUTCOME_FAILED else "retrying", job, exc)
        return self._next_from(res)

    async def _schedule_next(self, scheduler_id: str) -> None:
        """Enqueue the next occurrence of a scheduler (idempotent, stops if removed)."""
        template = await self.redis.hgetall(self.keys.scheduler(scheduler_id))
        if not template or await self.redis.zscore(self.keys.repeat, scheduler_id) is None:
            return  # scheduler was removed - stop the chain
        every = int(template["every"]) if template.get("every") else None
        cron = cast("str | None", template.get("cron") or None)
        now = _now_ms()
        when = next_run(now, every=every, cron=cron)
        await self.redis.zadd(self.keys.repeat, {scheduler_id: when})
        opts = json.loads(template["opts"])
        await self._add_scheduled(
            keys=[self.keys.delayed, self.keys.base],
            args=[
                f"repeat:{scheduler_id}:{when}",
                template["name"],
                template["data"],
                template["opts"],
                now,
                when,
                opts.get("priority", 0),
                scheduler_id,
                opts.get("concurrencyKey") or "",
            ],
        )

    def _fetch_flag(self) -> str:
        # Don't fetch a next job while shutting down - let the queue drain cleanly.
        return "1" if self._running else "0"

    def _next_from(self, res: Any) -> tuple[str, dict[str, str]] | None:
        if isinstance(res, (list, tuple)) and len(res) >= 3:
            return (str(res[2]), _pairs(res[1]))
        return None

    # ---- locks & recovery -------------------------------------------------

    async def _subscribe_cancels(self) -> PubSub | None:
        """Subscribe to the events channel, confirmed. Returns None if Redis would not
        confirm: the lock renewal is the backstop, so a worker starts either way.
        """
        pubsub = self.redis.pubsub()
        try:
            await pubsub.subscribe(self.keys.cancel)
            await confirm_subscribed(pubsub)
        except Exception:  # pragma: no cover - the listener retries in the background
            logger.debug("cancel subscription not ready; the lock renewal backstops it")
            with contextlib.suppress(Exception):
                await pubsub.aclose()
            return None
        return pubsub

    async def _cancel_listener(self, pubsub: PubSub | None) -> None:
        """Hear cancellations as they are asked for, rather than at the next renewal.

        One subscription per worker, not per job: every worker hears every request and
        acts only on jobs it is running. The lock renewal is the backstop, so a stream
        that dies here costs latency, never a cancellation.
        """
        while self._running:
            if pubsub is None:
                await asyncio.sleep(1)
                pubsub = await self._subscribe_cancels()
                continue
            try:
                while self._running:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=self.block_timeout
                    )
                    if msg is None:
                        continue
                    # the channel carries nothing but job ids, so there is nothing to
                    # parse and nothing to filter: everything here is a cancellation
                    self._request_cancel(str(msg["data"]))
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - reconnect; the lock still backstops
                logger.debug("cancel listener lost its subscription; retrying")
                with contextlib.suppress(Exception):
                    await pubsub.aclose()
                pubsub = None
        with contextlib.suppress(Exception):
            await pubsub.aclose() if pubsub is not None else None

    async def _renew_loop(self, job_id: str) -> None:
        interval = self.lock_renew_time / 1000
        while True:
            await asyncio.sleep(interval)
            try:
                ok = await self._extend_lock(
                    keys=[self.keys.lock(job_id), self.keys.stalled, self.keys.job(job_id)],
                    args=[self.token, self.lock_duration, job_id],
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover
                ok = 0
            if not ok:
                self._emit("lock-lost", job_id)
                return
            if int(ok) == scripts.LOCK_CANCEL_REQUESTED:
                # the message never reached us (or there was none): this is the backstop
                self._request_cancel(job_id)
                return

    async def _promote_loop(self) -> None:
        while self._running:
            try:
                # Drain in bounded chunks: a full batch means more may be due, so
                # go again at once - each call blocks Redis ~ms, not the whole sweep.
                while self._running:
                    promoted = await self._promote_delayed(
                        keys=[
                            self.keys.delayed,
                            self.keys.prioritized,
                            self.keys.marker,
                            self.keys.base,
                            self.keys.pc,
                        ],
                        args=[_now_ms(), scripts.PROMOTE_BATCH],
                    )
                    if int(promoted) < scripts.PROMOTE_BATCH:
                        break
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
                self.keys.stalled,
                self.keys.active,
                self.keys.prioritized,
                self.keys.failed,
                self.keys.stalled_check,
                self.keys.base,
                self.keys.marker,
                self.keys.pc,
            ],
            args=[self.max_stalled_count, _now_ms(), throttle, scripts.METRICS_RETENTION_MS],
        )
        failed = _str_list(res[0]) if res else []
        recovered = _str_list(res[1]) if res and len(res) > 1 else []
        return failed, recovered

    def _backoff_delay(self, job: Job) -> int:
        return compute_backoff(job.opts.backoff, job.attempts_made)
