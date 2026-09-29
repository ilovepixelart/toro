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
import functools
import inspect
import json
import logging
import os
import random
import socket
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.commands.core import AsyncScript
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from . import scripts
from ._replies import _scored, _str_list
from .connection import (
    DEFAULT_BLOCK_TIMEOUT,
    confirm_subscribed,
    connect,
    pop_timeout,
    read_timeout,
)
from .job import FINISHED_STATES, Backoff, Job, JobContext
from .keys import Keys
from .limits import RateLimit, limit_fields
from .queue import stamp_data_model
from .scheduler import next_run

# A processor is awaited when it is a coroutine function and run in a thread when it
# is not, so both shapes are the public contract.
Processor = Callable[[Job], Awaitable[Any] | Any]

logger = logging.getLogger(__name__)


# Below this, a threshold is smaller than the jitter of the sleep that measures it:
# an ordinary overshoot would read as a blocked loop and an idle worker would warn.
MIN_BLOCKED_WARNING = 0.01

# How often a worker looks for a schedule whose queued occurrence was dropped. A
# repair for a rare loss, so seconds apart: every worker runs it on its own.
SCHEDULE_CHECK_S = 5.0

# The shortest block an idle slot sends when the due time it was told has passed.
# Never 0, which a blocking pop reads as "no timeout"; and not 0.001 either: Redis
# 6.2 parses the timeout into whole milliseconds by truncating a long double
# product, which turns 0.001 into 0 and blocks for good (Redis 7 rounds up). Ten
# milliseconds survives both and is under either server's timeout granularity.
MIN_BLOCK_S = 0.01


def _blocked_threshold(blocked_warning: float | None, lock_renew_time: int) -> float:
    """How long the loop may be unable to run anything before that is worth saying.

    Tied to the renewal rather than to a round number: lag approaching a renewal
    interval means a renewal is already late, and a late renewal is how the stalled
    sweep comes to run a job a second time. 0 turns the watchdog off.
    """
    if blocked_warning is None:
        return lock_renew_time / 1000 / 2
    if blocked_warning < 0 or 0 < blocked_warning < MIN_BLOCKED_WARNING:
        raise ValueError(
            f"blocked_warning is seconds: 0 to disable, otherwise at least "
            f"{MIN_BLOCKED_WARNING} (below that is the loop's own jitter)"
        )
    return float(blocked_warning)


def _is_async(processor: Processor) -> bool:
    """Whether this processor is awaited or handed to a thread.

    Asked by inspection rather than by calling it: calling a sync processor to see
    what comes back would run it inside the event loop, which is what running it in a
    thread exists to avoid. `asyncio.iscoroutinefunction` sees through a
    `functools.partial`; a class-based processor answers for its `__call__`.
    """
    target = processor
    while isinstance(target, functools.partial):
        # unwrapped by hand: iscoroutinefunction sees through a partial to what it
        # wraps, but then asks the wrong object about __call__ (the partial's own)
        target = target.func
    if asyncio.iscoroutinefunction(target):
        return True
    call = getattr(target, "__call__", None)  # noqa: B004
    return call is not None and bool(asyncio.iscoroutinefunction(call))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _pairs(flat: list[str] | None) -> dict[str, str]:
    """Turn a flat HGETALL array [k, v, k, v, ...] into a dict."""
    if not flat:
        return {}
    it = iter(flat)
    return dict(zip(it, it, strict=False))


def block_for(pop_timeout_s: float, due_ms: int | None, now_ms: int) -> float:
    """Bound an idle slot's block: the poll, cut short at the next due time the claim
    told it, so a delayed job is promoted when due and not at the next poll.

    Never below MIN_BLOCK_S: 0 would block for good, and so would 0.001 on Redis 6.2.
    """
    if due_ms is None:
        return pop_timeout_s
    return min(pop_timeout_s, max((due_ms - now_ms) / 1000, MIN_BLOCK_S))


# How long a presence record outlives its worker's last heartbeat. Long enough that a
# dashboard opened the next day still finds a crashed worker and logs it as lost.
PRESENCE_TTL_MS = 24 * 60 * 60 * 1000


def compute_backoff(
    backoff: Backoff, attempts_made: int, *, rand: Callable[[], float] = random.random
) -> int:
    """Delay (ms) before the next attempt. `backoff` is None/0, an int (fixed ms),
    or {"type": "fixed"|"exponential", "delay": ms, "max": ms, "jitter": 0..1}.
    Exponential doubles per attempt; `max` caps the delay; `jitter` adds up to that
    share of the delay at random, so jobs that failed together do not retry together.
    Pure function so it can be unit-tested without a Redis-bound Worker.
    """
    if not backoff:
        return 0
    if isinstance(backoff, (int, float)):
        return int(backoff)
    delay = float(backoff.get("delay", 0))
    if backoff.get("type") == "exponential":
        # A float overflows past 2**1023; by 2**62 the delay is astronomical whatever
        # the cap, which is applied after, so the exponent stops there.
        delay *= 2 ** min(attempts_made - 1, 62)
    cap = backoff.get("max")
    if cap:
        delay = min(delay, float(cap))
    jitter = backoff.get("jitter")
    if jitter:
        delay *= 1 + jitter * rand()
    return int(delay)


def _timed_out(job: Job) -> TimeoutError:
    return TimeoutError(f"the processor ran over the job's timeout of {job.opts.timeout} ms")


def _claim(job: Job) -> str:
    """Return the processedOn a run was claimed at, which its finish must still find."""
    return "" if job.processed_on is None else str(job.processed_on)


class Worker:
    """The consumer side: claims jobs, runs the processor, and recovers stalls."""

    # Set by stop(), cleared when run() returns: a stop() that lands before run() is
    # past its startup round trips finds nothing to cancel, so run() has to look.
    _stop_requested: bool = False

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
        blocked_warning: float | None = None,
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
        # The queue-wide cap and rate limit this worker was started with. They apply
        # only to a queue whose own limits were never set (Queue.set_limits): the
        # claim reads the queue's first. Passed on every claim, so the scripts need
        # no separate read for a queue that has none of its own.
        self.global_concurrency, self.rl_max, self.rl_duration = limit_fields(
            global_concurrency, rate_limit
        )
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
        self.blocked_warning = _blocked_threshold(blocked_warning, self.lock_renew_time)
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
        # Background loops in a failure episode: named so each one warns once.
        self._failing: set[str] = set()
        self._current: set[str] = set()
        # When the next delayed job is due, as the last claim that found nothing said:
        # an idle slot blocks until then. None when nothing is delayed (or unknown).
        self._due_ms: int | None = None
        # The processor task of each running job, so a cancellation can reach it, and
        # job id -> (its processor's task, the claim it is running). The claim fences
        # a cancellation against an id that has been reused since the request was made.
        self._processors: dict[str, tuple[asyncio.Task[Any], str]] = {}
        # job id -> the run THIS worker asked to stop: a CancelledError in a run that is
        # not in here is the worker shutting down, which must not commit a cancel. Keyed
        # to the run, not the id alone: another slot can claim the id again while the
        # cancelled run is still unwinding, and that run was not asked to stop.
        self._cancelling: dict[str, asyncio.Task[Any]] = {}
        # Held on the instance, not inside the listener: stop() cancels that task while
        # it waits on a message, so a close in its own `finally` may never be reached,
        # and a caller-owned pool is not disconnected for us. Same shape as Queue.
        self._cancel_pubsub: PubSub | None = None
        # Asked once, here, and not per job: calling the processor to find out what it
        # returns would run a sync one inside the loop.
        self._async_processor = _is_async(processor)
        # Created on the first sync job, so an all-async worker pays nothing for a
        # feature it never uses.
        self._executor: ThreadPoolExecutor | None = None
        # "running" until a graceful stop flips it to "stopping" - the dashboard shows
        # a live "draining" state, and a worker that then vanishes was mid-shutdown,
        # not a crash. (The only honest way to know graceful; absence can't say why.)
        self._state = "running"

        self._register_scripts()

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

    def _loop_failed(self, what: str, exc: Exception) -> None:
        """One warning per failure episode: a loop retries every interval, and a Redis
        outage would otherwise fill the log at that rate. Silence is not an option
        either: a worker whose sweeps had failed for an hour looked healthy.
        """
        if what in self._failing:
            return
        self._failing.add(what)
        logger.warning("%s failed and is retried every interval: %r", what, exc)

    def _loop_recovered(self, what: str) -> None:
        if what in self._failing:
            self._failing.discard(what)
            logger.info("%s recovered", what)

    async def run(self) -> None:
        """Start processing until stop() is called. Awaitable forever."""
        try:
            await self._run()
        finally:
            self._stop_requested = False  # a stopped worker may be run again

    async def _run(self) -> None:
        # Before anything is claimed: a worker writes more than a producer does, and
        # must not run a claim loop against a model it cannot read.
        await stamp_data_model(self._stamp, self.keys, self.name)
        self._running = True
        self._state = "running"  # a worker run again after stop() is no longer draining
        self.started_at = _now_ms()
        await self._write_heartbeat()  # register at once so the worker shows up immediately
        # Subscribed BEFORE the first claim: a job this worker is running has to be one
        # it can hear a cancellation for, or the request waits out a lock renewal.
        cancels = await self._subscribe_cancels()
        if self._stop_requested:
            # stop() landed during the round trips above, with nothing yet to cancel
            # and after its own cleanup: undo what startup redid, and go no further.
            await self._abandon_startup()
            return
        self._process_tasks = [
            asyncio.create_task(self._process_loop()) for _ in range(self.concurrency)
        ]
        bg = [
            asyncio.create_task(self._schedules_loop()),
            asyncio.create_task(self._cancel_listener(cancels)),
        ]
        if self.stalled_interval > 0:
            bg.append(asyncio.create_task(self._stalled_loop()))
        if self.heartbeat_interval > 0:
            bg.append(asyncio.create_task(self._heartbeat_loop()))
        if self.blocked_warning > 0:
            bg.append(asyncio.create_task(self._watchdog_loop()))
        self._tasks = [*self._process_tasks, *bg]
        try:
            # return_exceptions: one freak task failure must not crash run() and
            # take every other slot down with it (each loop also guards itself).
            # Shielded so a cancellation of run() itself lands here first.
            results = await asyncio.shield(asyncio.gather(*self._tasks, return_exceptions=True))
        except asyncio.CancelledError:
            # run() cancelled outright (a framework cancelling its tasks, Ctrl-C under
            # asyncio.run): end the loops as stop() does without the grace period, and
            # lower the flag first, so a slot can tell this from a job it was asked to
            # cancel where the task's own cancellation count is not there to tell it.
            self._running = False
            for t in self._tasks:
                t.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            raise  # a cancelled run() ends cancelled, as any task does
        for res in results:
            if isinstance(res, Exception):
                logger.error("worker task died: %r", res)

    async def stop(self, grace_period: float | None = None) -> None:
        """Graceful shutdown: stop fetching new jobs, let in-flight jobs finish
        (up to `grace_period` seconds), then cancel the rest and disconnect.
        """
        grace = self.grace_period if grace_period is None else grace_period
        self._stop_requested = True
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
        await self._close_cancel_pubsub()
        if self._executor is not None:
            # A thread cannot be cancelled, so a sync job still running keeps its
            # thread to the end; what this drops is the work that never started.
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None
        with contextlib.suppress(Exception):
            await self._deregister()  # drop our presence record so we vanish at once
        await self.redis.aclose(close_connection_pool=self._owns_connection)

    def _pool(self) -> ThreadPoolExecutor:
        """Return the worker's own threads, one per slot.

        Not the loop's default executor: that one is shared process-wide and sized
        `min(32, cpu + 4)`, so a worker with more slots than that would queue sync
        jobs behind a pool it does not control, and a job waiting for a thread waits
        holding its lock.
        """
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self.concurrency, thread_name_prefix=f"toro-{self.name}"
            )
        return self._executor

    # ---- presence / heartbeat ---------------------------------------------

    async def _watchdog_loop(self) -> None:
        """Warn when the event loop could not run for longer than a job can afford.

        Measures its own lateness: a sleep that returns late by more than the
        threshold is time the loop spent unable to run anything at all, whatever the
        cause, including causes inside a dependency. One warning per episode, because
        the point is to name the processor, not to fill the log.
        """
        interval = max(MIN_BLOCKED_WARNING / 2, min(1.0, self.blocked_warning / 2))
        warned = False
        while self._running:
            before = time.monotonic()
            await asyncio.sleep(interval)
            lag = time.monotonic() - before - interval
            if lag < self.blocked_warning:
                warned = False
                continue
            if not warned:
                jobs = sorted(self._current)
                logger.warning(
                    "event loop was blocked for %.1fs (threshold %.1fs); jobs in flight: %s. "
                    "A processor that blocks the loop stops lock renewal, and the stalled "
                    "sweep re-runs its jobs elsewhere.",
                    lag,
                    self.blocked_warning,
                    ", ".join(jobs) or "none",
                )
                self._emit("blocked", lag, jobs)
            warned = True

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.heartbeat_interval / 1000)
            try:
                await self._write_heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._loop_failed("the heartbeat", exc)
            else:
                self._loop_recovered("the heartbeat")

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
                "cancelled": self._cancelled,
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

    async def _abandon_startup(self) -> None:
        """Take back what run() set up after stop() had already cleaned up: the cancel
        subscription and the presence record the first heartbeat wrote (stop() already
        recorded the departure), then close the connections those reopened.
        """
        self._running = False
        await self._close_cancel_pubsub()
        with contextlib.suppress(Exception):
            await self.redis.zrem(self.keys.workers, self.token)
            await self.redis.delete(self.keys.worker(self.token))
        await self.redis.aclose(close_connection_pool=self._owns_connection)

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
                "cancelled": self._cancelled,
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
        pause = 0.1
        while self._running:
            try:
                # The claim comes first: it promotes the delayed jobs that are due and,
                # finding nothing, says when the next one is due. Then the marker only
                # wakes us; the real claim is the atomic MOVE_TO_ACTIVE. A timeout
                # (None) is fine - we claim again, so a missed marker can never strand
                # a job, and a delayed job is promoted at its due time by whichever
                # comes first, the wake or the block ending.
                loaded = await self._acquire()
                # Keep processing as long as each finish hands us the next job. No
                # `_running` check here: a job in hand is already claimed, and stop()
                # can land during the very round trip that claimed it. Dropped, it
                # would sit locked in `active` until the sweep. Shutdown ends the
                # chain by itself: a stopping worker finishes with fetch=0.
                while loaded is not None:
                    loaded = await self._handle(loaded)
                if not self._running:
                    break
                timeout = block_for(self._pop_timeout, self._due_ms, _now_ms())
                woke = await self.redis.bzpopmin(self.keys.marker, timeout)
                self._loop_recovered("a claim")
                pause = 0.1
                if woke and not self._running:
                    # Shutting down - don't claim a new job. A marker we popped was
                    # a wake for a worker that still can, so hand it on: swallowed,
                    # the work it signalled waits out someone's block_timeout.
                    await self.redis.zadd(self.keys.marker, {"0": 0})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # One warning per outage, not a traceback per slot every tenth of a
                # second: with Redis down, every slot lands here at once. The pause
                # doubles up to the block timeout and resets on the next round trip.
                self._loop_failed("a claim", exc)
                await asyncio.sleep(pause)
                pause = min(pause * 2, self.block_timeout)

    def _register_scripts(self) -> None:
        """Register the Lua scripts this worker runs (a local call; Redis is not touched)."""
        register = self.redis.register_script
        self._move_to_active = register(scripts.MOVE_TO_ACTIVE)
        self._extend_lock = register(scripts.EXTEND_LOCK)
        self._move_to_completed = register(scripts.MOVE_TO_COMPLETED)
        self._move_to_failed = register(scripts.MOVE_TO_FAILED)
        self._move_to_cancelled = register(scripts.MOVE_TO_CANCELLED)
        self._release_job = register(scripts.RELEASE_JOB)
        self._move_stalled = register(scripts.MOVE_STALLED)
        self._add_scheduled = register(scripts.ADD_SCHEDULED)
        self._stamp = register(scripts.STAMP_MODEL)
        self._update_progress = register(scripts.UPDATE_PROGRESS)
        self._append_log = register(scripts.APPEND_LOG)

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
        """Read the claim's answer: the job, or None.

        A claim that found nothing may say when the next delayed job is due. Every
        answer replaces the last one, so a stale due time never shortens the block
        after the job has been taken.
        """
        self._due_ms = None
        if not res:
            return None
        if res[0] == scripts.DUE_SENTINEL:
            self._due_ms = int(float(res[1]))
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
        if fields.get("cancel"):
            # Claimed with a cancellation already pending: the stalled sweep re-queues
            # a job whose worker died, flag and all. Running it from the top only to
            # stop it at the first renewal repeats whatever the processor does before
            # its first await. The claim hands us the whole hash, so we know here.
            # No processor ran, so there is nothing in `_cancelling` to track.
            return await self._finish_cancelled(job)
        # Give the handler the ability to report progress and append logs.
        job._ctx = JobContext(  # noqa: SLF001  - the worker injects the job's runtime context
            redis=self.redis,
            job_key=self.keys.job(job_id),
            events_key=self.keys.events,
            logs_key=self.keys.logs(job_id),
            job_id=job_id,
            results_key=self.keys.results(job_id),
            cfail_key=self.keys.cfail(job_id),
            ccancel_key=self.keys.ccancel(job_id),
            update_progress=self._update_progress,
            append_log=self._append_log,
        )
        # A scheduler job mints its successor when it is picked up, so the schedule
        # stays on time regardless of how long (or whether) this run succeeds. Any
        # attempt may do it (see _schedule_next): a first attempt that died before
        # minting leaves it to the run that recovers it.
        if fields.get("schedulerId"):
            await self._schedule_next(fields["schedulerId"], job_id)
        renewer = asyncio.create_task(self._renew_loop(job_id)) if self.renew_locks else None
        self._current.add(job_id)  # so the heartbeat reports what we're running

        # In its own task so a cancellation has something to land on: awaited inline,
        # there is nothing to stop but the process loop itself. Wrapped because a
        # processor is any awaitable, and only a coroutine can become a task.
        async def run_processor() -> Any:
            if self._async_processor:
                return await self.processor(job)
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(self._pool(), self.processor, job)
            # The inspection can be wrong (a decorator that hides a coroutine
            # function), and then the thread hands back an un-started coroutine. The
            # job's "result" would be a coroutine object that fails to serialize.
            return await result if inspect.isawaitable(result) else result

        task = asyncio.create_task(run_processor())
        self._processors[job_id] = (task, str(fields.get("processedOn", "")))
        try:
            nxt = await self._outcome(job, task)
        finally:
            # Only this run's own entries: another slot may have re-claimed the job
            # after this run lost its lock, and that run is still going.
            if self._processors.get(job_id, (None, ""))[0] is task:
                self._current.discard(job_id)
                self._processors.pop(job_id)
            if self._cancelling.get(job_id) is task:
                del self._cancelling[job_id]
            if renewer is not None:
                renewer.cancel()
        return nxt

    async def _outcome(
        self, job: Job, task: asyncio.Task[Any]
    ) -> tuple[str, dict[str, str]] | None:
        """Commit whatever the processor's task came back with.

        A job this worker asked to stop ends `cancelled` however its processor
        unwound. A cleanup that raises on the way out is not a failure to retry, and a
        processor that caught the cancellation and returned did not complete the work:
        its lock is still held and it is still in `active`, so either commit would
        otherwise succeed and stand.
        """
        timer, expired = self._arm_timeout(job, task)
        try:
            result = await task
        except asyncio.CancelledError:  # NOSONAR
            # Absorbing this one IS the feature: a cancellation that killed the process
            # loop would take the worker's slot with it, so the usual "always re-raise"
            # rule cannot hold here. It IS re-raised in every case that is not ours.
            return await self._cancelled_outcome(job, task, expired[0])
        except Exception as exc:
            if self._cancelling.get(job.id) is not task:
                return await self._processing_failed(job, exc)
            result = None  # a cleanup that raised while cancelled: committed below
        finally:
            if timer is not None:
                timer.cancel()
        if expired[0]:
            # The processor caught the timer's cancellation and returned (or its cleanup
            # raised): the run still ended on the job's timeout, not with a result.
            return await self._processing_failed(job, _timed_out(job))
        if self._cancelling.get(job.id) is task:
            return await self._finish_cancelled(job)
        try:
            committed = await self._finish_completed(job, result)
        except (TypeError, ValueError) as exc:
            # A result the queue cannot store is the processor's bug, and it has to
            # end the job like any other error would. Left to escape the commit, the
            # job stays `active` holding its lock until the stalled sweep re-runs it,
            # burning an attempt on work that fails the same way every time.
            return await self._processing_failed(job, exc)
        self._processed += 1
        return committed

    def _arm_timeout(
        self, job: Job, task: asyncio.Task[Any]
    ) -> tuple[asyncio.TimerHandle | None, list[bool]]:
        """Start the job's timeout, if it has one: a timer that cancels the processor's
        task and raises a flag, which tells that cancellation from every other one, so
        a TimeoutError the processor raises itself is still its own failure. The timer
        and a cancel request are one signal: whichever comes first unwinds the run and
        decides how it ends, and the other is not delivered into its cleanup. A sync
        processor's thread cannot be taken back: no timer.
        """
        expired = [False]
        timeout = job.opts.timeout if self._async_processor else None
        if not timeout:
            return None, expired

        def expire() -> None:
            if self._cancelling.get(job.id) is task:
                return  # a cancel request is already unwinding it: one signal, not two
            expired[0] = True
            # Registered as the run's one cancellation, so a cancel request landing
            # during the unwind is not a second one (see _request_cancel).
            self._cancelling[job.id] = task
            task.cancel()

        return asyncio.get_running_loop().call_later(timeout / 1000, expire), expired

    async def _cancelled_outcome(
        self, job: Job, task: asyncio.Task[Any], expired: bool
    ) -> tuple[str, dict[str, str]] | None:
        """Commit what a processor task that ended in CancelledError means.

        Ours only when the PROCESSOR is what was stopped. The same error arrives
        when this WORKER is being stopped, and absorbing that one commits a job
        and carries on through a shutdown. Cancelling the awaiting task cancels
        the awaited one too, so the inner task cannot tell them apart; the
        outer task's own pending-cancellation count can (3.11+, with the
        shutdown flag as the fallback: run() clears it before cancelling us).
        """
        outer = asyncio.current_task()
        asked = getattr(outer, "cancelling", None)
        stopping = asked() > 0 if asked is not None else not self._running
        if stopping:
            if self._async_processor:
                # The processor's task has unwound (awaiting it is what raised), so the
                # job goes straight back to the queue. A sync processor's thread runs
                # on and keeps the job; the sweep takes it later.
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._release(job), 2.0)
            raise asyncio.CancelledError
        if expired:
            return await self._processing_failed(job, _timed_out(job))
        if self._cancelling.get(job.id) is not task:
            # Nobody cancelled this run and the worker is not stopping: the
            # processor raised it itself (it awaited something that was
            # cancelled), which is the processor failing like any other error.
            stray = RuntimeError("the processor raised CancelledError; the job was not cancelled")
            return await self._processing_failed(job, stray)
        return await self._finish_cancelled(job)

    def _request_cancel(self, job_id: str, claim: str | None = None) -> None:
        """Stop a job this worker is running, once.

        `claim` names the run the request was meant for. Without it the caller already
        knows (a lock renewal answered for the job in hand); with it, a request for an
        earlier run of a reused id is ignored rather than killing its successor.

        Nothing to do if the job is not ours, or if it already asked to stop: the
        `cancel` field stays set while the job is active, so the lock keeps reporting
        it and the request can arrive again and again. A second `cancel()` would land
        inside the processor's cleanup and abort the unwinding the first one promised.

        Nothing to do either if the task is finished, though a cancellation that lands
        between the processor returning and the task being marked done still wins:
        CPython discards the result and the job commits `cancelled`.

        A SYNC processor is recorded and not interrupted: see the comment below.
        """
        running = self._processors.get(job_id)
        if running is None:
            return
        task, mine = running
        if self._cancelling.get(job_id) is task:
            return
        if task.done() or (claim is not None and claim != mine):
            return
        self._cancelling[job_id] = task
        if not self._async_processor:
            # A thread cannot be interrupted, so cancelling the await would free this
            # slot and commit a terminal state that hands on the concurrency key,
            # while the work itself carried on: the pool would have one thread fewer
            # than the worker has slots, and the next job on that key would start
            # beside work that never stopped. The request stands, and the job ends
            # `cancelled` when its thread returns.
            return
        task.cancel()

    async def _finish_cancelled(self, job: Job) -> tuple[str, dict[str, str]] | None:
        res = await self._committed(
            self._move_to_cancelled,
            [
                self.keys.active,
                self.keys.cancelled,
                self.keys.job(job.id),
                self.keys.lock(job.id),
                self.keys.prioritized,
                self.keys.marker,
                self.keys.base,
                self.keys.events,
            ],
            lambda _fetch: [
                job.id,
                _now_ms(),
                self.token,
                scripts.METRICS_RETENTION_MS,
                _claim(job),
                self.lock_duration,  # how long the answer is kept for a re-send
            ],
        )
        if int(res) < 0:
            # its lock is gone, so nothing was committed: a removal took the job, or
            # another worker did. Counting it would report a cancellation the queue
            # has no record of.
            await self._finish_lost(job.id)
            return None
        self._cancelled += 1
        self._emit("cancelled", job)
        return None

    async def _release(self, job: Job) -> None:
        """Hand a job cut off by a shutdown back to the queue at once, its lock dropped.

        Left in `active`, it waited out its lock and a sweep pass before running again,
        and that recovery counted toward `max_stalled_count`: at the default of 1, a
        job cut off by two deploys in a row was failed for good.
        """
        res = await self._release_job(
            keys=[
                self.keys.active,
                self.keys.prioritized,
                self.keys.job(job.id),
                self.keys.lock(job.id),
                self.keys.marker,
                self.keys.base,
                self.keys.pc,
            ],
            args=[job.id, self.token, _claim(job)],
        )
        if int(res) < 0:
            await self._finish_lost(job.id)  # another run owns it: nothing to hand back

    async def _committed(
        self,
        script: AsyncScript,
        keys: list[str],
        args: Callable[[str], list[Any]],
    ) -> Any:
        """Run a finish script, re-sending it through a Redis blip while the lease holds.

        A finish that ran but whose reply was lost answers a re-send as it answered
        the first time, with the job it fetched (see recallFinish in the scripts),
        whether this loop re-sent it or the client did on a new connection; one that
        never ran runs now. So a blip of a few seconds becomes a late commit rather
        than a re-run of the job after the sweep, and nothing is fetched twice: a
        re-send from here never asks for the next job itself. The lease was renewed
        at most lock_renew_time ago, so it holds for lock_duration less that; past it
        the finish is given up as before.
        """
        deadline = time.monotonic() + max(0.0, (self.lock_duration - self.lock_renew_time) / 1000)
        pause = 0.2
        fetch = self._fetch_flag()
        while True:
            try:
                res = await script(keys=keys, args=args(fetch))
            except (RedisConnectionError, RedisTimeoutError) as exc:
                if time.monotonic() + pause > deadline:
                    raise
                self._loop_failed("a finish", exc)
                await asyncio.sleep(pause)
                pause = min(pause * 2, 2.0)
                fetch = "0"
                continue
            self._loop_recovered("a finish")
            return res

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
        returnvalue = json.dumps(result)
        res = await self._committed(
            self._move_to_completed,
            [
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
            lambda fetch: scripts.completed_args(
                job_id=job.id,
                returnvalue=returnvalue,
                now=_now_ms(),
                token=self.token,
                fetch=fetch,
                lock_duration=self.lock_duration,
                rl_max=self.rl_max,
                rl_duration=self.rl_duration,
                global_concurrency=self.global_concurrency,
                claim=_claim(job),
            ),
        )
        if res in (scripts.LOCK_LOST, scripts.NOT_ACTIVE):  # finish script's int sentinel
            await self._finish_lost(job.id)
            return None
        job.returnvalue = result
        self._emit("completed", job, result)
        return self._next_from(res)

    async def _processing_failed(
        self, job: Job, exc: Exception
    ) -> tuple[str, dict[str, str]] | None:
        """Record the traceback being handled and fail the job with `exc`."""
        self._failed += 1
        return await self._finish_failed(job, exc, traceback.format_exc())

    async def _finish_failed(
        self, job: Job, exc: Exception, stacktrace: str = ""
    ) -> tuple[str, dict[str, str]] | None:
        backoff = self._backoff_delay(job)
        res = await self._committed(
            self._move_to_failed,
            [
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
            lambda fetch: [
                *scripts.failed_args(
                    job_id=job.id,
                    reason=str(exc),
                    now=_now_ms(),
                    attempts_made=job.attempts_made,
                    max_attempts=job.opts.attempts,
                    backoff=backoff,
                    token=self.token,
                    fetch=fetch,
                    lock_duration=self.lock_duration,
                    rl_max=self.rl_max,
                    rl_duration=self.rl_duration,
                    global_concurrency=self.global_concurrency,
                    claim=_claim(job),
                ),
                stacktrace,  # ARGV[15], written only past the guards
            ],
        )
        if res in (scripts.LOCK_LOST, scripts.NOT_ACTIVE):  # finish script's int sentinel
            await self._finish_lost(job.id)
            return None
        job.failed_reason = str(exc)
        self._emit("failed" if res[0] == scripts.OUTCOME_FAILED else "retrying", job, exc)
        return self._next_from(res)

    async def _schedule_next(self, scheduler_id: str, occurrence_id: str) -> None:
        """Enqueue the next occurrence of a scheduler (idempotent, stops if removed).

        The next slot follows this occurrence's own slot, the time in its id, as well
        as the clock: an occurrence run early (promoted, or claimed by a worker whose
        clock is ahead) would otherwise compute its own slot again, collide with its
        own id, and enqueue nothing.
        """
        template = await self.redis.hgetall(self.keys.scheduler(scheduler_id))
        scheduled = await self.redis.zscore(self.keys.repeat, scheduler_id)
        if not template or scheduled is None:
            return  # scheduler was removed - stop the chain
        slot = int(occurrence_id.rsplit(":", 1)[1])
        if int(scheduled) != slot:
            return  # the chain has moved past this occurrence: an earlier attempt minted
        every = int(template["every"]) if template.get("every") else None
        cron = cast("str | None", template.get("cron") or None)
        now = _now_ms()
        when = next_run(max(now, slot), every=every, cron=cron)
        # XX: only a schedule still registered moves on to its next slot. One removed
        # between the reads above and this write would otherwise come back as an
        # entry with no template behind it, and run once more.
        moved = await self.redis.zadd(self.keys.repeat, {scheduler_id: when}, xx=True, ch=True)
        if not moved:
            return
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
                scripts.METRICS_RETENTION_MS,
            ],
        )

    def _fetch_flag(self) -> str:
        # Don't fetch a next job while shutting down - let the queue drain cleanly.
        return "1" if self._running else "0"

    def _next_from(self, res: Any) -> tuple[str, dict[str, str]] | None:
        """Read what a finish fetched next: the claim's answer after the outcome."""
        return self._loaded(list(res[1:])) if isinstance(res, (list, tuple)) else None

    # ---- locks & recovery -------------------------------------------------

    async def _subscribe_cancels(self) -> PubSub | None:
        """Subscribe to the cancel channel, confirmed. Returns None if Redis would not
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
        self._cancel_pubsub = pubsub
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
                    # "<jobId>:<claim>". Split from the RIGHT: a scheduler occurrence
                    # id carries colons of its own.
                    jid, _, claim = str(msg["data"]).rpartition(":")
                    if jid:
                        self._request_cancel(jid, claim)
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - reconnect; the lock still backstops
                logger.debug("cancel listener lost its subscription; retrying")
                await self._close_cancel_pubsub()
                pubsub = None

    async def _close_cancel_pubsub(self) -> None:
        """Give the subscription's connection back. On a pool the caller owns, nothing
        else will: `aclose()` there leaves it checked out and still subscribed.
        """
        pubsub, self._cancel_pubsub = self._cancel_pubsub, None
        if pubsub is not None:
            with contextlib.suppress(Exception):
                await pubsub.aclose()

    async def _renew_loop(self, job_id: str) -> None:
        interval = self.lock_renew_time / 1000
        held_until = time.monotonic() + self.lock_duration / 1000
        while True:
            await asyncio.sleep(interval)
            try:
                ok = await self._extend_lock(
                    keys=[self.keys.lock(job_id), self.keys.stalled, self.keys.job(job_id)],
                    args=[self.token, self.lock_duration, job_id],
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                # Redis could not be asked, which says nothing about the lock: it is
                # still ours until the lease from the last renewal runs out.
                if time.monotonic() < held_until:
                    continue
                ok = 0
            else:
                held_until = time.monotonic() + self.lock_duration / 1000
            if int(ok) == scripts.LOCK_JOB_GONE:
                # removed while we ran it: there is nothing left to finish, and the
                # message that would have said so never arrived
                self._request_cancel(job_id)
                self._emit("lock-lost", job_id)
                return
            if not ok:
                self._emit("lock-lost", job_id)
                return
            if int(ok) == scripts.LOCK_CANCEL_REQUESTED:
                # The message never reached us, or there was none: this is the backstop.
                # Keep renewing afterwards, because the processor is now unwinding and
                # a cleanup that outlives the lock would be re-run by the stalled sweep
                # on another worker. Asking twice is a no-op (see _request_cancel).
                self._request_cancel(job_id)

    async def _schedules_loop(self) -> None:
        """Repair schedules every SCHEDULE_CHECK_S, from startup on.

        Delayed jobs need no loop: the claim promotes the ones that are due, and an
        idle slot blocks until the next due time the claim told it.
        """
        while self._running:
            try:
                await self._resume_orphaned_schedules()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._loop_failed("the schedule check", exc)
            else:
                self._loop_recovered("the schedule check")
            await asyncio.sleep(SCHEDULE_CHECK_S)

    async def _resume_orphaned_schedules(self) -> None:
        """Enqueue the next occurrence of a schedule whose queued one was dropped.

        Only a picked-up occurrence enqueues the next, so one cancelled, removed or
        cleaned out before its slot ended the schedule silently, while schedulers()
        still listed it. Only remove_scheduler ends a schedule: a slot that has passed
        with no occurrence behind it is picked up here, at the next slot after now.
        """
        overdue = _scored(
            await self.redis.zrangebyscore(self.keys.repeat, "-inf", _now_ms(), withscores=True)
        )
        for scheduler_id, score in overdue:
            occurrence_id = f"repeat:{scheduler_id}:{int(score)}"
            # gone, or finished without enqueuing a successor (a cancelled one never
            # ran); one still queued or running enqueues it when it is picked up
            state = await self.redis.hget(self.keys.job(occurrence_id), "state")
            if state is None or state in FINISHED_STATES:
                await self._schedule_next(scheduler_id, occurrence_id)

    async def _stalled_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.stalled_interval / 1000)
            try:
                failed, recovered = await self.check_stalled()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._loop_failed("the stalled-job sweep", exc)
                continue
            self._loop_recovered("the stalled-job sweep")
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
        # attempts_made counts the runs that finished before this one; the run that is
        # failing now is the next ordinal, and the backoff is that attempt's.
        return compute_backoff(job.opts.backoff, job.attempts_made + 1)
