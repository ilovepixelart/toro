"""Queue: the producer side. Adds jobs and inspects their state."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, cast

from redis.asyncio import Redis

from . import scripts
from .connection import connect
from .errors import JobFailedError
from .job import Job, JobOptions, JobState
from .keys import Keys
from .scheduler import next_run, valid_cron


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clamp_priority(p: int) -> int:
    return max(0, min(int(p), scripts.PRIORITY_OFFSET))


class Queue:
    """The producer side: add jobs, schedule them, and inspect queue state."""

    def __init__(
        self,
        name: str,
        *,
        connection: Redis | None = None,
        url: str = "redis://localhost:6379",
        prefix: str = "toro",
        default_job_options: dict | None = None,
    ) -> None:
        self.name = name
        # Defaults merged into every add() (per-call options win) — e.g.
        # default_job_options={"remove_on_complete": 1000} so you don't repeat it.
        self.default_job_options = dict(default_job_options or {})
        self.keys = Keys(name, prefix)
        # NB: created with decode_responses=True, so every command returns str —
        # redis-py's async client isn't generic over that, hence the casts below.
        self.redis = connection or connect(url)
        self._add_job = self.redis.register_script(scripts.ADD_JOB)
        self._retry_job = self.redis.register_script(scripts.RETRY_JOB)
        self._remove_job = self.redis.register_script(scripts.REMOVE_JOB)
        self._add_scheduled = self.redis.register_script(scripts.ADD_SCHEDULED)
        self._promote_job = self.redis.register_script(scripts.PROMOTE_JOB)

    async def add(
        self,
        name: str,
        data: Any = None,
        *,
        job_id: str | None = None,
        deduplication: dict | None = None,
        **opts: Any,
    ) -> Job:
        """Enqueue a job. Returns the created Job (with its id).

        `priority`: higher = more urgent (global order across the whole queue);
        the default 0 is the least-urgent band, processed FIFO among itself.

        `job_id`: a custom id. Adding a second job with an id that already exists
        is IDEMPOTENT — it's ignored, not duplicated (id-based dedup). Once the job
        is removed, the id is free to reuse. Must be a non-empty, non-all-digits
        string (all-digit ids collide with auto-generated ones).

        `deduplication`: `{"id": str, "ttl": ms}` — a throttle window. While the
        ttl is live, repeat adds with the same dedup id are ignored and the
        already-queued job's id is returned. Self-expiring; independent of job_id.
        """
        options = JobOptions(**{**self.default_job_options, **opts})
        options.priority = _clamp_priority(options.priority)
        if job_id is not None:
            job_id = str(job_id)
            if not job_id or job_id.isdigit():
                raise ValueError(
                    "custom job_id must be a non-empty, non-all-digits string "
                    "(digits collide with auto-generated ids) — try e.g. 'order-123'"
                )
        dedup_id, dedup_ttl = "", 0
        if deduplication is not None:
            dedup_id = str(deduplication.get("id") or "")
            dedup_ttl = int(deduplication.get("ttl") or 0)
            if not dedup_id or dedup_ttl <= 0:
                raise ValueError("deduplication needs {'id': str, 'ttl': positive ms}")
        now = _now_ms()
        new_id = str(
            await self._add_job(
                keys=[
                    self.keys.id,
                    self.keys.prioritized,
                    self.keys.marker,
                    self.keys.delayed,
                    self.keys.base,
                    self.keys.pc,
                ],
                args=[
                    name,
                    json.dumps(data),
                    json.dumps(options.to_dict()),
                    now,
                    options.delay,
                    options.priority,
                    job_id or "",
                    dedup_id,
                    dedup_ttl,
                ],
            )
        )
        # Signal the change so a live dashboard refreshes on enqueue, not only when a
        # job finishes (completed/failed publish from the Lua side). result() and the
        # dashboard both tolerate this event type.
        await self.redis.publish(self.keys.events, json.dumps({"jobId": new_id, "event": "added"}))
        return Job(
            id=new_id,
            name=name,
            data=data,
            opts=options,
            timestamp=now,
            state="delayed" if options.delay > 0 else "wait",
            _queue=self,
        )

    async def result(self, job_id: str, *, timeout: float = 30.0) -> Any:
        """Wait for a job to finish; return its return value, or raise JobFailedError.

        Subscribes before checking state, so it won't miss the outcome of a job
        that finishes while we wait. Works even if the job hash was auto-removed,
        as long as result() was awaited before the job finished.
        """
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.keys.events)
        try:
            job = await self.get_job(job_id)
            if job is not None and job.state == "completed":
                return job.returnvalue
            if job is not None and job.state == "failed":
                raise JobFailedError(job.failed_reason)
            loop = asyncio.get_running_loop()  # correct idiom inside a coroutine
            deadline = loop.time() + timeout
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(f"job {job_id} did not finish within {timeout}s")
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=remaining)
                if msg is None:
                    continue
                data = json.loads(msg["data"])
                if str(data.get("jobId")) != str(job_id):
                    continue
                event = data.get("event")
                if event == "completed":
                    return data.get("result")
                if event == "failed":
                    raise JobFailedError(data.get("reason"))
                # ignore non-terminal events (e.g. "added") and keep waiting
        finally:
            await pubsub.aclose()

    # ---- schedulers (cron / repeatable) -----------------------------------

    async def add_scheduler(
        self,
        scheduler_id: str,
        *,
        every: int | None = None,
        cron: str | None = None,
        name: str | None = None,
        data: Any = None,
        priority: int = 0,
        **job_opts: Any,
    ) -> str:
        """Register a repeatable schedule. Exactly one of `every` (ms) or `cron`.

        Stores a scheduler record and enqueues the first occurrence as a delayed
        job; each occurrence mints its successor when a worker picks it up.
        Re-calling with the same id updates the schedule.
        """
        scheduler_id = str(scheduler_id)
        if not scheduler_id or ":" in scheduler_id or any(ord(c) < 0x20 for c in scheduler_id):
            # it's interpolated into Redis keys ({base}repeat:<id>) and the occurrence
            # id (repeat:<id>:<when>); ':' or control chars let one scheduler collide
            # with another's keys — same class of guard as custom job_id.
            raise ValueError(
                "scheduler_id must be a non-empty string with no ':' or control "
                "characters (it's used as a Redis key segment) — try e.g. 'nightly-rollup'"
            )
        if (every is None) == (cron is None):
            raise ValueError("pass exactly one of `every` or `cron`")
        if cron is not None and not valid_cron(cron):
            # fail at enqueue, not later inside a worker's _schedule_next (a silent
            # scheduler that errors on the backend)
            raise ValueError(f"invalid cron expression: {cron!r}")
        opts = JobOptions(priority=_clamp_priority(priority), **job_opts).to_dict()
        template = {
            "name": name or scheduler_id,
            "every": str(every) if every else "",
            "cron": cron or "",
            "data": json.dumps(data),
            "opts": json.dumps(opts),
        }
        # redis-py's hset overloads don't resolve a plain dict[str, str] mapping.
        await self.redis.hset(self.keys.scheduler(scheduler_id), mapping=template)  # ty: ignore[no-matching-overload]
        when = next_run(_now_ms(), every=every, cron=cron)
        await self.redis.zadd(self.keys.repeat, {scheduler_id: when})
        await self._enqueue_occurrence(scheduler_id, when, template)
        return scheduler_id

    async def _enqueue_occurrence(self, scheduler_id: str, when: int, template: dict) -> None:
        opts = json.loads(template["opts"])
        await self._add_scheduled(
            keys=[self.keys.delayed, self.keys.base],
            args=[
                f"repeat:{scheduler_id}:{when}",
                template["name"],
                template["data"],
                template["opts"],
                _now_ms(),
                when,
                opts.get("priority", 0),
                scheduler_id,
            ],
        )

    async def remove_scheduler(self, scheduler_id: str) -> None:
        """Stop a schedule and drop its pending occurrence."""
        score = await self.redis.zscore(self.keys.repeat, scheduler_id)
        await self.redis.zrem(self.keys.repeat, scheduler_id)
        await self.redis.delete(self.keys.scheduler(scheduler_id))
        if score is not None:
            await self.remove_job(f"repeat:{scheduler_id}:{int(score)}")

    async def trigger_scheduler(self, scheduler_id: str) -> bool:
        """Enqueue one immediate occurrence of a scheduler (a manual 'run now').

        Carries the scheduler's configured options (priority/attempts/backoff/
        auto-removal) so a manual run matches a scheduled one — but runs immediately
        (`delay` is omitted, not taken from the stored opts).
        """
        t = await self.redis.hgetall(self.keys.scheduler(scheduler_id))
        if not t:
            return False
        name = cast("str", t.get("name", scheduler_id))
        opts = JobOptions.from_dict(json.loads(t.get("opts") or "{}"))
        await self.add(
            name,
            json.loads(t.get("data") or "null"),
            attempts=opts.attempts,
            backoff=opts.backoff,
            priority=opts.priority,
            remove_on_complete=opts.remove_on_complete,
            remove_on_fail=opts.remove_on_fail,
        )
        return True

    async def schedulers(self) -> list[dict]:
        """List active schedulers (for the dashboard)."""
        entries = cast(
            "list[tuple[str, float]]",
            await self.redis.zrange(self.keys.repeat, 0, -1, withscores=True),
        )
        if not entries:
            return []
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for sid, _ in entries:
            pipe.hgetall(self.keys.scheduler(sid))
        templates = cast("list[dict[str, str]]", await pipe.execute())
        out = []
        for (sid, when), t in zip(entries, templates, strict=False):
            out.append(
                {
                    "id": sid,
                    "name": t.get("name", sid),
                    "next": int(when),
                    "every": int(t["every"]) if t.get("every") else None,
                    "cron": t.get("cron") or None,
                }
            )
        return out

    async def get_job(self, job_id: str) -> Job | None:
        h = await self.redis.hgetall(self.keys.job(job_id))
        if not h:
            return None
        return Job.from_hash(job_id, h)

    async def get_logs(self, job_id: str, start: int = 0, end: int = -1) -> list[str]:
        return cast("list[str]", await self.redis.lrange(self.keys.logs(job_id), start, end))

    async def counts(self) -> dict[str, int]:
        """Quick snapshot of how many jobs sit in each state. `wait` = waiting
        jobs in the prioritized set.
        """
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        pipe.zcard(self.keys.prioritized)
        pipe.llen(self.keys.active)
        pipe.zcard(self.keys.delayed)
        pipe.zcard(self.keys.completed)
        pipe.zcard(self.keys.failed)
        wait, active, delayed, completed, failed = await pipe.execute()
        return {
            "wait": wait,
            "active": active,
            "delayed": delayed,
            "completed": completed,
            "failed": failed,
        }

    async def workers(self, *, stale_after: int = 30_000) -> list[dict]:
        """Live workers, from the presence records their heartbeats write. An entry
        with no heartbeat for `stale_after` ms is treated as dead and pruned here,
        so a crashed worker (which never deregistered) disappears on its own.
        """
        now = _now_ms()
        ids = cast("list[str]", await self.redis.zrange(self.keys.workers, 0, -1))
        if not ids:
            return []
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for wid in ids:
            pipe.hgetall(self.keys.worker(wid))
        hashes = cast("list[dict]", await pipe.execute())
        live: list[dict] = []
        dead: list[tuple[str, dict]] = []
        for wid, h in zip(ids, hashes, strict=True):
            heartbeat = int(h.get("heartbeat", 0)) if h else 0
            if not h or now - heartbeat > stale_after:
                dead.append((wid, h or {}))
                continue
            live.append(
                {
                    "id": wid,
                    "host": h.get("host", "?"),
                    "pid": int(h.get("pid", 0)),
                    "queue": h.get("queue", self.name),
                    "concurrency": int(h.get("concurrency", 0)),
                    "started": int(h.get("started", 0)),
                    "heartbeat": heartbeat,
                    "processed": int(h.get("processed", 0)),
                    "failed": int(h.get("failed", 0)),
                    "current": json.loads(h.get("current", "[]")),
                    "state": h.get("state", "running"),
                }
            )
        if dead:
            # A stale worker crashed/was killed without deregistering — log it as
            # "lost" (vs a graceful "stopped") before pruning, so its death is visible.
            # Kept transactional: record-then-prune must be atomic, else a partial
            # failure leaves a worker re-recorded (duplicate death) or pruned silently.
            pipe = self.redis.pipeline()
            for wid, h in dead:
                if h:
                    pipe.lpush(
                        self.keys.departed,
                        json.dumps(
                            {
                                "id": wid,
                                "host": h.get("host", "?"),
                                "pid": int(h.get("pid", 0)),
                                "queue": h.get("queue", self.name),
                                "concurrency": int(h.get("concurrency", 0)),
                                "processed": int(h.get("processed", 0)),
                                "failed": int(h.get("failed", 0)),
                                "started": int(h.get("started", 0)),
                                "last_seen": int(h.get("heartbeat", 0)),
                                "current": json.loads(h.get("current", "[]")),
                                "reason": "lost",
                                "at": now,
                            }
                        ),
                    )
            pipe.ltrim(self.keys.departed, 0, 49)
            pipe.zrem(self.keys.workers, *[w for w, _ in dead])
            pipe.delete(*(self.keys.worker(w) for w, _ in dead))
            await pipe.execute()
        live.sort(key=lambda w: w["started"])
        return live

    async def departed_workers(self, limit: int = 20) -> list[dict]:
        """Recent worker departures, newest first — graceful stops ("stopped") and
        lost heartbeats ("lost"). A bounded death-log so the dashboard can show what
        left, when, and why, instead of workers silently vanishing.
        """
        raw = cast("list[str]", await self.redis.lrange(self.keys.departed, 0, limit - 1))
        return [json.loads(r) for r in raw]

    async def clear_departed(self) -> int:
        """Drop the recorded worker departures (the post-mortem log). Returns the count
        cleared. Live workers re-appear via their heartbeats; this only clears history.
        """
        n = await self.redis.llen(self.keys.departed)
        await self.redis.delete(self.keys.departed)
        return n

    async def get_jobs(self, state: JobState, start: int = 0, end: int = 20) -> list[Job]:
        """Page through job ids in a given state and hydrate them into Jobs.
        `wait` returns jobs in global priority order (most urgent first).
        """
        if state in ("wait", "prioritized"):
            ids = await self.redis.zrange(self.keys.prioritized, start, end)
        elif state == "active":
            ids = await self.redis.lrange(self.keys.active, start, end)
        elif state == "delayed":
            ids = await self.redis.zrange(self.keys.delayed, start, end)
        elif state in ("completed", "failed"):
            ids = await self.redis.zrevrange(getattr(self.keys, state), start, end)
        else:
            raise ValueError(f"unknown state: {state}")
        if not ids:
            return []
        # Hydrate the whole page in one round trip instead of one HGETALL per job.
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for job_id in ids:
            pipe.hgetall(self.keys.job(cast("str", job_id)))
        hashes = await pipe.execute()
        return [
            Job.from_hash(cast("str", jid), h) for jid, h in zip(ids, hashes, strict=False) if h
        ]

    async def retry_job(self, job_id: str) -> bool:
        """Move a failed job back to the queue for another attempt."""
        res = await self._retry_job(
            keys=[
                self.keys.failed,
                self.keys.prioritized,
                self.keys.marker,
                self.keys.job(job_id),
                self.keys.pc,
            ],
            args=[job_id],
        )
        return bool(res)

    async def remove_job(self, job_id: str) -> bool:
        """Delete a job from every state and drop its hash."""
        res = await self._remove_job(
            keys=[
                self.keys.prioritized,
                self.keys.active,
                self.keys.delayed,
                self.keys.completed,
                self.keys.failed,
                self.keys.job(job_id),
            ],
            args=[job_id],
        )
        return bool(res)

    async def promote_job(self, job_id: str) -> bool:
        """Move a delayed job into the queue to run now."""
        res = await self._promote_job(
            keys=[
                self.keys.delayed,
                self.keys.prioritized,
                self.keys.marker,
                self.keys.job(job_id),
                self.keys.pc,
            ],
            args=[job_id],
        )
        return bool(res)

    async def _ids(self, state: JobState, limit: int) -> list[str]:
        if state in ("wait", "prioritized"):
            return cast("list[str]", await self.redis.zrange(self.keys.prioritized, 0, limit - 1))
        if state == "active":
            return cast("list[str]", await self.redis.lrange(self.keys.active, 0, limit - 1))
        if state in ("delayed", "completed", "failed"):
            zset = getattr(self.keys, state)
            return cast("list[str]", await self.redis.zrange(zset, 0, limit - 1))
        raise ValueError(f"unknown state: {state}")

    async def search(self, state: JobState, query: str, scan_limit: int = 500) -> list[Job]:
        """Substring-search `name`/`data` within a state's most recent `scan_limit`
        jobs (Redis hashes aren't queryable, so this is a bounded scan + filter).
        Returns the matches; the caller should surface the scan bound honestly.
        """
        ids = await self._ids(state, scan_limit)
        if not ids:
            return []
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for job_id in ids:
            pipe.hgetall(self.keys.job(job_id))
        hashes = await pipe.execute()
        q = query.lower()
        out = []
        for job_id, h in zip(ids, hashes, strict=False):
            if h and (q in h.get("name", "").lower() or q in h.get("data", "").lower()):
                out.append(Job.from_hash(job_id, h))
        return out

    async def retry_all_failed(self, limit: int = 1000) -> int:
        """Re-queue every failed job. Returns how many were retried."""
        ids = await self._ids("failed", limit)
        for job_id in ids:
            await self.retry_job(job_id)
        return len(ids)

    async def clean(self, state: JobState, limit: int = 1000) -> int:
        """Remove every job in a state (up to `limit`). Returns how many were removed.

        Pipelines the per-job removals — one round trip per batch, not one per job —
        so clearing a large state stays fast (thousands of jobs in well under a second).
        """
        ids = await self._ids(state, limit)
        if not ids:
            return 0
        sha = await self.redis.script_load(scripts.REMOVE_JOB)  # ensure loaded for EVALSHA
        pipe = self.redis.pipeline(transaction=False)
        for job_id in ids:
            pipe.evalsha(
                sha,
                6,
                self.keys.prioritized,
                self.keys.active,
                self.keys.delayed,
                self.keys.completed,
                self.keys.failed,
                self.keys.job(job_id),
                job_id,
            )
        await pipe.execute()
        return len(ids)

    # ---- queue control ----------------------------------------------------

    async def pause(self) -> None:
        """Stop workers from claiming new jobs (in-flight jobs still finish)."""
        await self.redis.set(self.keys.meta_paused, "1")

    async def resume(self) -> None:
        """Resume claiming, and wake idle workers."""
        await self.redis.delete(self.keys.meta_paused)
        await self.redis.zadd(self.keys.marker, {"0": 0})

    async def is_paused(self) -> bool:
        return bool(await self.redis.exists(self.keys.meta_paused))

    async def close(self) -> None:
        await self.redis.aclose()
