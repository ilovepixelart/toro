"""Queue: the producer side. Adds jobs and inspects their state."""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import time
from typing import Any, TypedDict, cast

from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from . import scripts
from .connection import connect
from .errors import JobFailedError
from .flow import MAX_FLOW_NODES, FlowChild, FlowView, count_nodes, node_options, to_tree
from .flow import clamp_priority as _clamp_priority
from .job import Deduplication, Job, JobOptions, JobState, decode_results
from .keys import Keys
from .scheduler import next_run, valid_cron


def _now_ms() -> int:
    return int(time.time() * 1000)


def _str_list(reply: Any) -> list[str]:
    """Type a Redis list/zset reply (decode_responses is on) as list[str]."""
    return cast("list[str]", reply)


def _str_dict(reply: Any) -> dict[str, str]:
    """Type a Redis hash reply (decode_responses is on) as dict[str, str]."""
    return cast("dict[str, str]", reply)


def _hash_replies(reply: Any) -> list[dict[str, str]]:
    """Type a pipeline's list of hash replies as list[dict[str, str]]."""
    return cast("list[dict[str, str]]", reply)


class MetricsPoint(TypedDict):
    """One minute of queue activity: counts + summed processing duration."""

    timestamp: int  # minute bucket start (ms since epoch)
    added: int  # jobs enqueued this minute (the leading signal; dedup hits don't count)
    completed: int
    failed: int  # terminal failures only (retries don't count, stall-failures do)
    ms: int  # summed processing duration of jobs finished this minute


class NameMetrics(TypedDict):
    """One job name's totals over a metrics window."""

    name: str
    completed: int
    failed: int
    ms: int  # summed processing duration
    p50: int  # duration percentiles (ms, bucket upper bounds) - successful
    p95: int  # jobs only, 0 when nothing completed in the window
    p99: int


def bucket_upper_ms(idx: int) -> int:
    """Upper bound (ms) of histogram bucket `idx` - see scripts.HIST_*."""
    return int(scripts.HIST_BASE_MS * scripts.HIST_GROWTH**idx)


def bucket_estimate_ms(idx: int) -> int:
    """Estimate the representative duration for bucket `idx`: the geometric
    mean of its bounds. Reporting this instead of the upper bound halves the
    worst-case error (±22% instead of +50%) and removes the systematic upward
    bias - the same choice DDSketch makes. True values can sit anywhere in
    the bucket, so any single number is an estimate either way.
    """
    return round(bucket_upper_ms(idx) / math.sqrt(scripts.HIST_GROWTH))


def _percentile(buckets: list[int], q: float) -> int:
    """Read the q-quantile duration from histogram bucket counts (nearest-rank
    on the cumulative counts, reported as the bucket's geometric mean).
    """
    total = sum(buckets)
    if total == 0:
        return 0
    # the epsilon guards float artifacts: 20 * 0.95 == 19.000000000000004
    target = max(1, math.ceil(total * q - 1e-9))
    cum = 0
    for idx, count in enumerate(buckets):
        cum += count
        if cum >= target:
            return bucket_estimate_ms(idx)
    return bucket_estimate_ms(len(buckets) - 1)  # pragma: no cover - cum reaches total above


class Queue:
    """The producer side: add jobs, schedule them, and inspect queue state."""

    def __init__(
        self,
        name: str,
        *,
        connection: Redis | None = None,
        url: str = "redis://localhost:6379",
        prefix: str = "toro",
        default_job_options: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        # Defaults merged into every add() (per-call options win) - e.g.
        # default_job_options={"remove_on_complete": 1000} so you don't repeat it.
        self.default_job_options = dict(default_job_options or {})
        self.keys = Keys(name, prefix)
        # NB: created with decode_responses=True, so every command returns str -
        # redis-py's async client isn't generic over that, hence the casts below.
        self.redis = connection or connect(url)
        self._add_job = self.redis.register_script(scripts.ADD_JOB)
        self._add_flow_script = self.redis.register_script(scripts.ADD_FLOW)
        self._retry_job = self.redis.register_script(scripts.RETRY_JOB)
        self._remove_job = self.redis.register_script(scripts.REMOVE_JOB)
        self._add_scheduled = self.redis.register_script(scripts.ADD_SCHEDULED)
        self._promote_job = self.redis.register_script(scripts.PROMOTE_JOB)
        # result() plumbing: ALL waiters share ONE events subscription; a
        # dispatcher task routes each terminal event to the futures registered
        # for that jobId. One pubsub per waiter would cost waiters x events
        # client work and cap concurrent waiters at the connection pool size.
        self._result_waiters: dict[str, list[asyncio.Future[Any]]] = {}
        self._events_pubsub: PubSub | None = None
        self._events_task: asyncio.Task[None] | None = None
        self._dispatcher_lock = asyncio.Lock()

    async def add(
        self,
        name: str,
        data: Any = None,
        *,
        job_id: str | None = None,
        deduplication: Deduplication | None = None,
        **opts: Any,
    ) -> Job:
        """Enqueue a job. Returns the created Job (with its id).

        `priority`: higher = more urgent (global order across the whole queue);
        the default 0 is the least-urgent band, processed FIFO among itself.

        `job_id`: a custom id. Adding a second job with an id that already exists
        is IDEMPOTENT - it's ignored, not duplicated (id-based dedup). Once the job
        is removed, the id is free to reuse. Must be a non-empty, non-all-digits
        string (all-digit ids collide with auto-generated ones).

        `deduplication`: `{"id": str, "ttl": ms}` - a throttle window. While the
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
                    "(digits collide with auto-generated ids) - try e.g. 'order-123'"
                )
        dedup_id, dedup_ttl = "", 0
        if deduplication is not None:
            dedup_id = str(deduplication.get("id") or "")
            dedup_ttl = int(deduplication.get("ttl") or 0)
            if not dedup_id or dedup_ttl <= 0:
                raise ValueError("deduplication needs {'id': str, 'ttl': positive ms}")
            # same rule as scheduler ids: the id becomes a Redis key segment,
            # so ':' or control characters would let distinct ids collide
            # (and silently drop jobs that share the accidental key)
            if ":" in dedup_id or any(ord(c) < 0x20 for c in dedup_id):
                raise ValueError(
                    "deduplication id must not contain ':' or control characters "
                    "(it is used as a Redis key segment)"
                )
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
                    self.keys.events,
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
                    scripts.METRICS_RETENTION_MS,
                ],
            )
        )
        # The "added" event publishes from inside ADD_JOB (so a live dashboard
        # refreshes on enqueue without a second round trip here).
        return Job(
            id=new_id,
            name=name,
            data=data,
            opts=options,
            timestamp=now,
            state="delayed" if options.delay > 0 else "wait",
            _queue=self,
        )

    async def add_flow(
        self,
        name: str,
        data: Any = None,
        *,
        children: list[FlowChild],
        **opts: Any,
    ) -> Job:
        """Atomically enqueue a parent job plus its `children` (a flow).

        Children run first (in parallel; nest `FlowChild`s for deeper trees);
        the parent is parked in `waiting-children` until every child settles,
        then runs and can pull `job.children_results()` / `job.failed_children()`.
        A child's terminal failure follows its `on_fail` policy (default: the
        parent fails immediately). Returns the parent Job; `await job.result()`
        resolves when the whole flow does. See docs/flows-design.md.
        """
        if not children:
            raise ValueError("a flow needs at least one child - use add() for a single job")
        # The root is a FlowChild too: same validation (incl. "no delay on a
        # node with children" - the parent runs when its children settle).
        root = FlowChild(name, data, children=list(children), **opts)
        if (n := count_nodes(root)) > MAX_FLOW_NODES:
            raise ValueError(f"flow has {n} nodes; the limit is {MAX_FLOW_NODES}")
        # Built (and validated) BEFORE the script runs: an option error must
        # never surface after the flow is already live in Redis.
        options = node_options(root, self.default_job_options)
        now = _now_ms()
        tree = to_tree(root, self.default_job_options)
        parent_id = str(
            await self._add_flow_script(
                keys=[self.keys.id, self.keys.base],
                args=[now, json.dumps(tree), scripts.METRICS_RETENTION_MS],
            )
        )
        return Job(
            id=parent_id,
            name=name,
            data=data,
            opts=options,
            timestamp=now,
            state="waiting-children",
            _queue=self,
        )

    async def _hydrate_flow(self, job_id: str, depth: int) -> dict[str, Any] | None:
        """BFS-hydrate a flow tree into ``{"job": Job, "children": [<same>]}``,
        root-down, one pipelined round trip per level - O(depth), not O(nodes).
        Shared by get_flow() and flow_view(). None when the root is gone; child
        hashes that vanished mid-walk are skipped. `depth` bounds the walk.
        """
        nodes: dict[str, dict[str, Any]] = {}
        parent_of: dict[str, str] = {}
        level = [job_id]
        for _ in range(depth + 1):
            pipe = self.redis.pipeline(transaction=False)  # read fan-out per level
            for jid in level:
                pipe.hgetall(self.keys.job(jid))
            next_level: list[str] = []
            for jid, h in zip(level, _hash_replies(await pipe.execute()), strict=True):
                if not h:
                    continue  # removed mid-walk (or a stale children entry)
                nodes[jid] = {"job": Job.from_hash(jid, h), "children": []}
                if (pid := parent_of.get(jid)) is not None:
                    nodes[pid]["children"].append(nodes[jid])
                for cid in json.loads(h["children"]) if h.get("children") else []:
                    parent_of[cid] = jid
                    next_level.append(cid)
            level = next_level
            if not level:
                break
        return nodes.get(job_id)

    async def get_flow(self, job_id: str, *, depth: int = 10) -> dict[str, Any] | None:
        """Read a flow tree back: ``{"job": Job, "children": [<same shape>]}``.

        Hydrates breadth-first, one pipelined round trip per level (the same
        shape as get_jobs' page hydration) - O(depth) round trips, not
        O(nodes). `depth` bounds the walk. None when the job doesn't exist.
        """
        return await self._hydrate_flow(job_id, depth)

    async def flow_view(self, job_id: str, *, depth: int = 10) -> FlowView | None:
        """Project a whole flow for a dashboard: the `get_flow` tree plus the
        parent's collected `results` and tolerated `failures`.

        One call where a detail view otherwise makes three (get_flow +
        children_results + failed_children): the tree in O(depth) round trips,
        then a single pipelined read of the parent's `:results` / `:cfail`
        hashes. None when the job doesn't exist.
        """
        tree = await self._hydrate_flow(job_id, depth)
        if tree is None:
            return None
        pipe = self.redis.pipeline(transaction=False)
        pipe.hgetall(self.keys.results(job_id))
        pipe.hgetall(self.keys.cfail(job_id))
        raw_results, raw_cfail = await pipe.execute()
        return FlowView(
            tree=tree,
            results=decode_results(_str_dict(raw_results)),
            failures=_str_dict(raw_cfail),
        )

    async def children_results(self, job_id: str) -> dict[str, Any]:
        """Read a flow parent's collected child results (child id -> value) -
        the queue-side read, for dashboards; processors use
        `job.children_results()`.
        """
        return decode_results(_str_dict(await self.redis.hgetall(self.keys.results(job_id))))

    async def failed_children(self, job_id: str) -> dict[str, str]:
        """Read child id -> failure reason recorded under ``on_fail="continue"``."""
        return _str_dict(await self.redis.hgetall(self.keys.cfail(job_id)))

    async def flow_progress(self, parent_ids: list[str]) -> dict[str, tuple[int, int]]:
        """For each flow parent id, ``(completed_children, failed_children)`` -
        cheap pipelined HLEN reads of the ``:results`` / ``:cfail`` hashes (just
        the counts, no values). Lets a dashboard show fan-in progress for a page
        of parked parents without hydrating each tree.
        """
        if not parent_ids:
            return {}
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for pid in parent_ids:
            pipe.hlen(self.keys.results(pid))
            pipe.hlen(self.keys.cfail(pid))
        res = await pipe.execute()
        return {pid: (int(res[2 * i]), int(res[2 * i + 1])) for i, pid in enumerate(parent_ids)}

    async def result(self, job_id: str, *, timeout: float = 30.0) -> Any:
        """Wait for a job to finish; return its return value, or raise JobFailedError.

        Registers with the shared dispatcher BEFORE checking state, so it won't
        miss the outcome of a job that finishes while we wait. Works even if the
        job hash was auto-removed, as long as result() was awaited before the
        job finished.
        """
        job_id = str(job_id)
        await self._ensure_dispatcher()
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._result_waiters.setdefault(job_id, []).append(fut)
        try:
            job = await self.get_job(job_id)
            if job is not None and job.state == "completed":
                return job.returnvalue
            if job is not None and job.state == "failed":
                raise JobFailedError(job.failed_reason)
            try:
                return await asyncio.wait_for(fut, timeout)
            except (TimeoutError, asyncio.TimeoutError):
                raise TimeoutError(f"job {job_id} did not finish within {timeout}s") from None
        finally:
            waiters = self._result_waiters.get(job_id)
            if waiters is not None:
                if fut in waiters:
                    waiters.remove(fut)
                if not waiters:
                    del self._result_waiters[job_id]

    async def _ensure_dispatcher(self) -> None:
        """Start the shared events listener (or restart it after a crash)."""
        if self._events_task is not None and not self._events_task.done():
            return
        async with self._dispatcher_lock:
            if self._events_task is not None and not self._events_task.done():
                return  # someone else won the race while we awaited the lock
            if self._events_pubsub is not None:  # a crashed listener's leftovers
                with contextlib.suppress(Exception):
                    await self._events_pubsub.aclose()
            pubsub = self.redis.pubsub()
            await pubsub.subscribe(self.keys.events)
            self._events_pubsub = pubsub
            self._events_task = asyncio.create_task(self._dispatch_events(pubsub))

    async def _dispatch_events(self, pubsub: PubSub) -> None:
        """Consume the shared events subscription and route each message."""
        try:
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
                if msg is not None:
                    self._route_event(msg["data"])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The subscription died (e.g. connection loss after retries): fail
            # the current waiters fast rather than letting them sit out their
            # timeouts; the next result() call starts a fresh dispatcher.
            for waiters in self._result_waiters.values():
                for fut in waiters:
                    if not fut.done():
                        fut.set_exception(exc)

    def _route_event(self, raw: str) -> None:
        """Resolve the futures waiting on a terminal event's jobId."""
        try:
            data = json.loads(raw)
        except ValueError:
            return
        event = data.get("event")
        if event not in ("completed", "failed"):
            return  # non-terminal (e.g. "added", "progress")
        for fut in self._result_waiters.get(str(data.get("jobId")), []):
            if fut.done():
                continue
            if event == "completed":
                fut.set_result(data.get("result"))
            else:
                fut.set_exception(JobFailedError(data.get("reason")))

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
            # with another's keys - same class of guard as custom job_id.
            raise ValueError(
                "scheduler_id must be a non-empty string with no ':' or control "
                "characters (it's used as a Redis key segment) - try e.g. 'nightly-rollup'"
            )
        if (every is None) == (cron is None):
            raise ValueError("pass exactly one of `every` or `cron`")
        if every is not None and int(every) <= 0:
            # 0 would otherwise surface as a confusing "needs either" error and a
            # negative interval as garbage grid math - fail clearly at the source.
            raise ValueError("`every` must be a positive number of milliseconds")
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

    async def _enqueue_occurrence(
        self, scheduler_id: str, when: int, template: dict[str, str]
    ) -> None:
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
        auto-removal) so a manual run matches a scheduled one - but runs immediately
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

    async def schedulers(self) -> list[dict[str, Any]]:
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
        templates = _hash_replies(await pipe.execute())
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
        h = _str_dict(await self.redis.hgetall(self.keys.job(job_id)))
        if not h:
            return None
        return Job.from_hash(job_id, h)

    async def get_logs(self, job_id: str, start: int = 0, end: int = -1) -> list[str]:
        return _str_list(await self.redis.lrange(self.keys.logs(job_id), start, end))

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
        pipe.zcard(self.keys.waiting_children)
        wait, active, delayed, completed, failed, waiting_children = await pipe.execute()
        return {
            "wait": wait,
            "active": active,
            "delayed": delayed,
            "completed": completed,
            "failed": failed,
            "waiting-children": waiting_children,
        }

    async def metrics(self, *, minutes: int = 60) -> list[MetricsPoint]:
        """Per-minute completed/failed counts and summed processing duration,
        oldest first, ending at the current minute. Gaps are zero-filled so the
        series is chart-ready. Counters are written atomically inside the finish
        scripts; buckets expire after `scripts.METRICS_RETENTION_MS` (8h), so
        asking for more history than that just returns zeros.
        """
        minutes = max(1, minutes)
        now_minute = _now_ms() // 60_000 * 60_000
        stamps = [now_minute - 60_000 * i for i in range(minutes - 1, -1, -1)]
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for ts in stamps:
            pipe.hgetall(self.keys.metrics_bucket(ts))
        hashes = _hash_replies(await pipe.execute())
        return [
            MetricsPoint(
                timestamp=ts,
                added=int(h.get("added", 0)),
                completed=int(h.get("completed", 0)),
                failed=int(h.get("failed", 0)),
                ms=int(h.get("ms", 0)),
            )
            for ts, h in zip(stamps, hashes, strict=True)
        ]

    async def metrics_by_name(self, *, minutes: int = 60) -> list[NameMetrics]:
        """Per-job-name totals over the window, failures first - the triage
        order ("which job is responsible"), not the volume order. Names come
        from the per-name fields the finish scripts write into the same
        minute buckets ("completed:<name>", "failed:<name>", "ms:<name>").
        """
        minutes = max(1, minutes)
        now_minute = _now_ms() // 60_000 * 60_000
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for i in range(minutes):
            pipe.hgetall(self.keys.metrics_bucket(now_minute - 60_000 * i))
        totals: dict[str, dict[str, int]] = {}
        hists: dict[str, list[int]] = {}
        for h in _hash_replies(await pipe.execute()):
            for field, value in h.items():
                kind, sep, rest = field.partition(":")
                if not sep:
                    continue  # queue-level field, not a per-name one
                if kind in ("completed", "failed", "ms"):
                    totals.setdefault(rest, {"completed": 0, "failed": 0, "ms": 0})
                    totals[rest][kind] += int(value)
                elif kind == "h":
                    # "h:<name>:<idx>" - names may contain colons, idx never does
                    name, _, idx = rest.rpartition(":")
                    hists.setdefault(name, [0] * scripts.HIST_BUCKETS)[int(idx)] += int(value)
        out = []
        for n, t in totals.items():
            buckets = hists.get(n, [])
            out.append(
                NameMetrics(
                    name=n,
                    completed=t["completed"],
                    failed=t["failed"],
                    ms=t["ms"],
                    p50=_percentile(buckets, 0.50),
                    p95=_percentile(buckets, 0.95),
                    p99=_percentile(buckets, 0.99),
                )
            )
        return sorted(out, key=lambda t: (-t["failed"], -t["completed"], t["name"]))

    async def percentiles(self, *, minutes: int = 60) -> dict[str, int]:
        """Queue-level p50/p95/p99 (ms) over the window - every job name's
        histogram merged into one. Successful jobs only; 0s when idle.
        """
        minutes = max(1, minutes)
        now_minute = _now_ms() // 60_000 * 60_000
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for i in range(minutes):
            pipe.hgetall(self.keys.metrics_bucket(now_minute - 60_000 * i))
        merged = [0] * scripts.HIST_BUCKETS
        for h in _hash_replies(await pipe.execute()):
            for field, value in h.items():
                if field.startswith("h:"):
                    merged[int(field.rpartition(":")[2])] += int(value)
        return {
            "p50": _percentile(merged, 0.50),
            "p95": _percentile(merged, 0.95),
            "p99": _percentile(merged, 0.99),
        }

    async def latency(self) -> int:
        """Age (ms) of the next-to-run waiting job - 0 when nothing is waiting.

        The queue-health headline number: depth says how much is queued,
        latency says how far behind the workers actually are.
        """
        head = _str_list(await self.redis.zrange(self.keys.prioritized, 0, 0))
        if not head:
            return 0
        ts = await self.redis.hget(self.keys.job(head[0]), "timestamp")
        if not ts:  # the head job was removed between the two reads
            return 0
        return max(0, _now_ms() - int(ts))

    async def workers(self, *, stale_after: int = 30_000) -> list[dict[str, Any]]:
        """Live workers, from the presence records their heartbeats write. An entry
        with no heartbeat for `stale_after` ms is treated as dead and pruned here,
        so a crashed worker (which never deregistered) disappears on its own.
        """
        now = _now_ms()
        ids = _str_list(await self.redis.zrange(self.keys.workers, 0, -1))
        if not ids:
            return []
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for wid in ids:
            pipe.hgetall(self.keys.worker(wid))
        hashes = _hash_replies(await pipe.execute())
        live: list[dict[str, Any]] = []
        dead: list[tuple[str, dict[str, Any]]] = []
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
            # A stale worker crashed/was killed without deregistering - log it as
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

    async def departed_workers(self, limit: int = 20) -> list[dict[str, Any]]:
        """Recent worker departures, newest first - graceful stops ("stopped") and
        lost heartbeats ("lost"). A bounded death-log so the dashboard can show what
        left, when, and why, instead of workers silently vanishing.
        """
        raw = _str_list(await self.redis.lrange(self.keys.departed, 0, limit - 1))
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
        elif state == "waiting-children":
            ids = await self.redis.zrange(self.keys.waiting_children, start, end)
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
        """Move a failed job back to the queue for another attempt.

        Flow-aware: a failed flow parent whose children haven't all settled
        re-parks in `waiting-children` instead of running with partial results;
        a retried child re-joins its parked parent's barrier. Retrying parent
        and children in any order (retry_all_failed does) recovers the flow.
        """
        res = await self._retry_job(
            keys=[
                self.keys.failed,
                self.keys.prioritized,
                self.keys.marker,
                self.keys.job(job_id),
                self.keys.pc,
                self.keys.base,
            ],
            args=[job_id, _now_ms()],
        )
        return bool(res)

    async def remove_job(self, job_id: str) -> bool:
        """Delete a job from every state and drop its hash.

        A flow parent takes its whole subtree with it (children included,
        even mid-flight); removing a pending child releases its parent when
        nothing else is left to wait for.
        """
        res = await self._remove_job(
            keys=[
                self.keys.prioritized,
                self.keys.active,
                self.keys.delayed,
                self.keys.completed,
                self.keys.failed,
                self.keys.waiting_children,
                self.keys.base,
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

    async def _ids(self, state: JobState, limit: int, *, newest: bool = False) -> list[str]:
        """Ids in a state. `newest=True` mirrors get_jobs()'s ordering - finished
        states come newest-first (what a dashboard shows as "recent"); the other
        states have one natural order (priority / claim / due-time) either way.
        """
        if state in ("wait", "prioritized"):
            return _str_list(await self.redis.zrange(self.keys.prioritized, 0, limit - 1))
        if state == "active":
            return _str_list(await self.redis.lrange(self.keys.active, 0, limit - 1))
        if state == "waiting-children":
            return _str_list(await self.redis.zrange(self.keys.waiting_children, 0, limit - 1))
        if state in ("delayed", "completed", "failed"):
            zset = getattr(self.keys, state)
            if newest and state in ("completed", "failed"):
                return _str_list(await self.redis.zrevrange(zset, 0, limit - 1))
            return _str_list(await self.redis.zrange(zset, 0, limit - 1))
        raise ValueError(f"unknown state: {state}")

    async def search(self, state: JobState, query: str, scan_limit: int = 500) -> list[Job]:
        """Substring-search `name`/`data` within a state's most recent `scan_limit`
        jobs (Redis hashes aren't queryable, so this is a bounded scan + filter).
        Scans the same end of the set get_jobs() pages - newest first for finished
        states. Returns the matches; the caller should surface the scan bound honestly.
        """
        ids = await self._ids(state, scan_limit, newest=True)
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
        """Re-queue every failed job. Returns how many were retried.

        Pipelines the per-job RETRY_JOB scripts - one round trip per batch, not
        one per job (same shape as clean(); ~17x faster than a serial loop).
        When `limit` truncates, the newest failures go first - the ones a
        dashboard is showing.
        """
        ids = await self._ids("failed", limit, newest=True)
        if not ids:
            return 0
        sha = await self.redis.script_load(scripts.RETRY_JOB)  # ensure loaded for EVALSHA
        now = _now_ms()
        pipe = self.redis.pipeline(transaction=False)
        for job_id in ids:
            pipe.evalsha(
                sha,
                6,
                self.keys.failed,
                self.keys.prioritized,
                self.keys.marker,
                self.keys.job(job_id),
                self.keys.pc,
                self.keys.base,
                job_id,
                now,
            )
        res = await pipe.execute()
        return sum(1 for r in res if r)

    async def _subtree_ids(self, root_id: str) -> list[str]:
        """Every job id in a flow's subtree, root first (breadth-first, unbounded
        unlike get_flow's display depth). One pipelined round trip per level;
        bounded by the flow's node cap. The `children` hash field is the static
        full child list, so a fully-failed flow is still walkable.
        """
        ids: list[str] = []
        level = [root_id]
        while level:
            ids.extend(level)
            pipe = self.redis.pipeline(transaction=False)  # read fan-out per level
            for jid in level:
                pipe.hget(self.keys.job(jid), "children")
            level = [
                cid for raw in await pipe.execute() for cid in (json.loads(raw) if raw else [])
            ]
        return ids

    async def retry_flow(self, parent_id: str) -> int:
        """Re-drive a whole failed flow: retry every failed job in the parent's
        subtree (the parent and all its descendants) in one call. Returns how
        many jobs were actually retried.

        Order is handled for you: the parent is retried first, so a parent that
        failed eagerly re-parks its barrier before its failed children re-join
        it - the flow converges back to running. Completed children are left
        untouched (their collected results survive); non-failed nodes are a
        no-op, so this also retries just the failed children of an in-flight
        flow. See `retry_job` for the single-job semantics this builds on.
        """
        ids = await self._subtree_ids(parent_id)
        if not ids:
            return 0
        sha = await self.redis.script_load(scripts.RETRY_JOB)  # ensure loaded for EVALSHA
        now = _now_ms()
        pipe = self.redis.pipeline(transaction=False)
        for job_id in ids:  # root-first: a re-parked parent is ready when its children retry
            pipe.evalsha(
                sha,
                6,
                self.keys.failed,
                self.keys.prioritized,
                self.keys.marker,
                self.keys.job(job_id),
                self.keys.pc,
                self.keys.base,
                job_id,
                now,
            )
        res = await pipe.execute()
        return sum(1 for r in res if r)

    async def clean(self, state: JobState, limit: int = 1000) -> int:
        """Remove every job in a state (up to `limit`, oldest first - when the
        limit truncates, old history goes before recent results). Returns how
        many were removed.

        Removing a flow parent removes its whole subtree, so
        clean("waiting-children") cancels every parked flow outright - children
        included, even ones currently running.

        Pipelines the per-job removals - one round trip per batch, not one per job -
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
                7,
                self.keys.prioritized,
                self.keys.active,
                self.keys.delayed,
                self.keys.completed,
                self.keys.failed,
                self.keys.waiting_children,
                self.keys.base,
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
        if self._events_task is not None:
            self._events_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._events_task
            self._events_task = None
        if self._events_pubsub is not None:
            await self._events_pubsub.aclose()
            self._events_pubsub = None
        # Fail anyone still awaiting result() fast, rather than leaving them to
        # sit out their timeout against a closed connection.
        for waiters in self._result_waiters.values():
            for fut in waiters:
                if not fut.done():
                    fut.set_exception(RuntimeError("queue closed while waiting for a result"))
        await self.redis.aclose()
