"""Queue: the producer side. Adds jobs and inspects their state."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import functools
import json
import math
import time
from collections.abc import Callable, Coroutine
from dataclasses import asdict, dataclass
from typing import Any, ParamSpec, TypedDict, TypeVar, cast

from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from . import scripts
from ._replies import _hash_replies, _scored, _str_dict, _str_list
from .connection import confirm_subscribed, connect
from .errors import IncompatibleDataModelError, JobCancelledError, JobFailedError, PartialFlushError
from .flow import MAX_FLOW_NODES, FlowChild, FlowView, count_nodes, node_options, to_tree
from .flow import clamp_priority as _clamp_priority
from .job import FINISHED_STATES, Deduplication, Job, JobOptions, JobState, decode_results
from .keys import Keys
from .openmetrics import OUTCOMES, TOTAL_FIELDS, render
from .scheduler import next_run, valid_cron

# A job id travels in URLs and log lines, so it is bounded like anything else a
# stranger writes.
MAX_JOB_ID_CHARS = 256
# A job name is a LABEL: it is rendered on every row, and the per-minute metrics keep
# a field per distinct value for eight hours. Bounded for the same reasons.
MAX_JOB_NAME_CHARS = 128


_P = ParamSpec("_P")
_R = TypeVar("_R")


def _writes(method: Callable[_P, Coroutine[Any, Any, _R]]) -> Callable[_P, Coroutine[Any, Any, _R]]:
    """Mark an entry point that writes, and check the data model before it does.

    Once per process, not once per call: the version changes during an upgrade, not
    during a call. The mark is what a test reads to find a write path that forgot.
    Typed through, because the package ships `py.typed` and a decorator that erased
    `add`'s signature would take every caller's type checking with it.
    """

    @functools.wraps(method)
    async def guarded(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        await cast("Queue", args[0])._stamp_model()  # noqa: SLF001 - its own method
        return await method(*args, **kwargs)

    guarded.__toro_writes__ = True  # ty: ignore[unresolved-attribute]
    return guarded


def _job_name(name: object) -> str:
    """Check a job name: a label a person reads and a metrics series is kept under,
    not a place to put a payload or an id.
    """
    if (
        not isinstance(name, str)
        or not name
        or len(name) > MAX_JOB_NAME_CHARS
        or any(ord(c) < 0x20 for c in name)
    ):
        msg = (
            f"job name must be a non-empty string of at most {MAX_JOB_NAME_CHARS} "
            f"characters with no control characters"
        )
        raise ValueError(msg)
    return name


def _now_ms() -> int:
    return int(time.time() * 1000)


async def stamp_data_model(stamp: Any, keys: Keys, name: str) -> None:
    """Stamp an unmarked queue with this library's data-model version, and refuse a
    queue whose model is newer than this library understands.

    Shared by the producer and the worker because both write, and a rolling upgrade
    puts two libraries on one queue by design.
    """
    found = int(await stamp(keys=[keys.meta], args=[scripts.DATA_MODEL_VERSION]))
    if found > scripts.DATA_MODEL_VERSION:
        raise IncompatibleDataModelError(name, found, scripts.DATA_MODEL_VERSION)


@dataclass(frozen=True)
class _Staged:
    """One enqueue, prepared but not sent.

    Staging is where validation, option merging and JSON encoding happen, so a caller
    that stages a batch and then sends it knows that nothing reached Redis if any of
    it was wrong.
    """

    script: Any
    keys: list[str]
    args: list[Any]
    build: Callable[[Any], Job]


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


class FlowMetricsPoint(TypedDict):
    """One minute of flow activity: whole flows that settled, counted once at
    the root (nested sub-flows don't count). Duration is tracked separately in a
    histogram (see flow_percentiles), not per minute.
    """

    timestamp: int  # minute bucket start (ms since epoch)
    completed: int  # root flows that finished this minute
    failed: int  # root flows that failed this minute


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


# The score below which a finished job has settled: a running flow's finished
# children sit at scripts.LIVE_SCORE and above (see scripts.recordFinished). A
# ZSET bound, exclusive.
SETTLED = f"({scripts.LIVE_SCORE}"


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
        # default_job_options={"remove_on_complete": 100} so you don't repeat it.
        self.default_job_options = dict(default_job_options or {})
        self.keys = Keys(name, prefix)
        # NB: created with decode_responses=True, so every command returns str -
        # redis-py's async client isn't generic over that, hence the casts below.
        self.redis = connection or connect(url)
        # A connection we opened is ours to give back on close(); one handed to us
        # belongs to the caller, who may still be using it elsewhere.
        self._owns_connection = connection is None
        self._add_job = self.redis.register_script(scripts.ADD_JOB)
        self._add_flow_script = self.redis.register_script(scripts.ADD_FLOW)
        self._retry_job = self.redis.register_script(scripts.RETRY_JOB)
        self._remove_job = self.redis.register_script(scripts.REMOVE_JOB)
        self._cancel_job = self.redis.register_script(scripts.CANCEL_JOB)
        self._add_scheduled = self.redis.register_script(scripts.ADD_SCHEDULED)
        self._stamp = self.redis.register_script(scripts.STAMP_MODEL)
        # Asked once per process, on the first WRITE. Not on a read: a dashboard opens
        # a queue for every name it is given, and stamping on a read would create the
        # ones nobody has used yet.
        self._model_checked = False
        # Tasks reading a result back for a waiter whose event could not carry it.
        self._read_backs: set[asyncio.Task[None]] = set()
        self._promote_job = self.redis.register_script(scripts.PROMOTE_JOB)
        self._list_roots = self.redis.register_script(scripts.LIST_ROOTS)
        self._roots_counts_script = self.redis.register_script(scripts.ROOTS_COUNTS)
        # result() plumbing: ALL waiters share ONE events subscription; a
        # dispatcher task routes each terminal event to the futures registered
        # for that jobId. One pubsub per waiter would cost waiters x events
        # client work and cap concurrent waiters at the connection pool size.
        self._result_waiters: dict[str, list[asyncio.Future[Any]]] = {}
        self._events_pubsub: PubSub | None = None
        self._events_task: asyncio.Task[None] | None = None
        self._dispatcher_lock = asyncio.Lock()

    def _custom_job_id(self, job_id: object) -> str:
        """Validate a custom job id: it becomes the job's Redis key, `<base><id>`."""
        job_id = str(job_id)
        if not job_id or job_id.isdigit():
            raise ValueError(
                "custom job_id must be a non-empty, non-all-digits string "
                "(digits collide with auto-generated ids) - try e.g. 'order-123'"
            )
        # A job id is a path segment in every dashboard that shows it. One that cannot
        # be put in a URL is a job nobody can open or remove, because the page that
        # would list it is the page that breaks; a control character does the same to
        # a log line. The length cap is the same idea as clipping a payload.
        if "/" in job_id or any(ord(c) < 0x20 for c in job_id) or len(job_id) > MAX_JOB_ID_CHARS:
            raise ValueError(
                f"custom job_id must have no '/' or control characters and be at most "
                f"{MAX_JOB_ID_CHARS} characters: it is a path segment wherever it is shown"
            )
        conflict = self.keys.job_id_conflict(job_id)
        if conflict:
            # the job's hash would BE that key: a queue broken with WRONGTYPE, or an
            # add() that finds the key and returns as if the job already existed
            raise ValueError(
                f"custom job_id {job_id!r} is reserved: it is {conflict} - try e.g. 'job-{job_id}'"
            )
        return job_id

    @_writes
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
        string (all-digit ids collide with auto-generated ones) that does not land
        on another key of the queue (`Keys.job_id_conflict`).

        `deduplication`: `{"id": str, "ttl": ms}` - a throttle window. While the
        ttl is live, repeat adds with the same dedup id are ignored and the
        already-queued job's id is returned. Self-expiring; independent of job_id.
        """
        staged = self._stage_add(name, data, job_id=job_id, deduplication=deduplication, opts=opts)
        return staged.build(await staged.script(keys=staged.keys, args=staged.args))

    def _stage_add(
        self,
        name: str,
        data: Any,
        *,
        job_id: str | None,
        deduplication: Deduplication | None,
        opts: dict[str, Any],
    ) -> _Staged:
        """Everything `add()` does except the round trip. Shared with `pending()`,
        which stages a batch and sends it in one.
        """
        _job_name(name)
        options = JobOptions(**{**self.default_job_options, **opts})
        options.priority = _clamp_priority(options.priority)
        if job_id is not None:
            job_id = self._custom_job_id(job_id)
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

        def build(reply: Any) -> Job:
            new_id, state = _str_list(reply)
            # The "added" event publishes from inside ADD_JOB (so a live dashboard
            # refreshes on enqueue without a second round trip here).
            return Job(
                id=new_id,
                name=name,
                data=data,
                opts=options,
                timestamp=now,
                # The script's answer, not a guess: an add that found the key taken
                # parks the job in `held`, and one that hit a dedup window or replayed
                # an id answers for the job already there, in whatever state it is in.
                state=cast("JobState", state) if state else None,
                _queue=self,
            )

        return _Staged(
            script=self._add_job,
            keys=[
                self.keys.id,
                self.keys.prioritized,
                self.keys.marker,
                self.keys.delayed,
                self.keys.base,
                self.keys.pc,
                self.keys.events,
            ],
            args=scripts.add_job_args(
                name=name,
                data=json.dumps(data),
                opts=json.dumps(options.to_dict()),
                now=now,
                delay=options.delay,
                priority=options.priority,
                job_id=job_id or "",
                dedup_id=dedup_id,
                dedup_ttl=dedup_ttl,
                concurrency_key=options.concurrency_key or "",
            ),
            build=build,
        )

    @_writes
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
        staged = self._stage_flow(name, data, children=children, opts=opts)
        return staged.build(await staged.script(keys=staged.keys, args=staged.args))

    def _stage_flow(
        self, name: str, data: Any, *, children: list[FlowChild], opts: dict[str, Any]
    ) -> _Staged:
        """Everything `add_flow()` does except the round trip."""
        _job_name(name)
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

        def build(reply: Any) -> Job:
            return Job(
                id=str(reply),
                name=name,
                data=data,
                opts=options,
                timestamp=now,
                state="waiting-children",
                _queue=self,
            )

        return _Staged(
            script=self._add_flow_script,
            keys=[self.keys.id, self.keys.base],
            args=[now, json.dumps(tree), scripts.METRICS_RETENTION_MS],
            build=build,
        )

    async def _stamp_model(self) -> None:
        """Adopt this queue's data model, or refuse it, once per process.

        The version changes during an upgrade, not during a call, so checking per call
        would buy nothing and cost the hot path a round trip.
        """
        if self._model_checked:
            return
        await stamp_data_model(self._stamp, self.keys, self.name)
        self._model_checked = True

    def pending(self) -> PendingJobs:
        """Collect jobs now; send them when the write they belong to has committed.

        A job enqueued before its transaction commits refers to a row that a rollback
        may take away, and the worker then fails on a row that never existed. Collect
        the adds instead and flush them once the commit returns:

            pending = queue.pending()
            pending.add("welcome", {"user_id": user.id})
            await session.commit()
            await pending.flush()

        It defers; it does not guarantee. A process that dies between the commit and
        the flush sends nothing, and closing that needs an outbox table and a relay.
        What this closes is the half people hit: a job about a row that was rolled
        back. See docs/producing.md.
        """
        return PendingJobs(self)

    @staticmethod
    def _hydrate_level(
        level: list[str],
        replies: list[dict[str, str]],
        nodes: dict[str, dict[str, Any]],
        parent_of: dict[str, str],
    ) -> list[str]:
        """Build the node for each id in one BFS level, link it under its parent,
        and return the next level's child ids. Hashes that vanished mid-walk are
        skipped. Mutates `nodes`/`parent_of` (the walk's shared accumulators).
        """
        next_level: list[str] = []
        for jid, h in zip(level, replies, strict=True):
            if not h:
                continue  # removed mid-walk (or a stale children entry)
            nodes[jid] = {"job": Job.from_hash(jid, h), "children": []}
            if (pid := parent_of.get(jid)) is not None:
                nodes[pid]["children"].append(nodes[jid])
            for cid in json.loads(h["children"]) if h.get("children") else []:
                parent_of[cid] = jid
                next_level.append(cid)
        return next_level

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
            level = self._hydrate_level(
                level, _hash_replies(await pipe.execute()), nodes, parent_of
            )
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
        pipe.hgetall(self.keys.ccancel(job_id))
        raw_results, raw_cfail, raw_ccancel = await pipe.execute()
        return FlowView(
            tree=tree,
            results=decode_results(_str_dict(raw_results)),
            failures=_str_dict(raw_cfail),
            cancellations=_str_dict(raw_ccancel),
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

    async def flow_progress(self, parent_ids: list[str]) -> dict[str, tuple[int, int, int]]:
        """For each flow parent id, ``(completed, failed, cancelled)`` children -
        cheap pipelined HLEN reads of the ``:results`` / ``:cfail`` / ``:ccancel``
        hashes (just the counts, no values). Lets a dashboard show fan-in progress
        for a page of parked parents without hydrating each tree.
        """
        if not parent_ids:
            return {}
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for pid in parent_ids:
            pipe.hlen(self.keys.results(pid))
            pipe.hlen(self.keys.cfail(pid))
            pipe.hlen(self.keys.ccancel(pid))
        res = await pipe.execute()
        return {
            pid: (int(res[3 * i]), int(res[3 * i + 1]), int(res[3 * i + 2]))
            for i, pid in enumerate(parent_ids)
        }

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
            if job is not None and job.state == "cancelled":
                raise JobCancelledError(job_id, job.cancel_reason)
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
            try:
                await pubsub.subscribe(self.keys.events)
                await confirm_subscribed(pubsub)
            except BaseException:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()  # it owns a connection by now
                raise
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
        if event not in ("completed", "failed", "cancelled"):
            return  # non-terminal (e.g. "added", "progress")
        job_id = str(data.get("jobId"))
        for fut in self._result_waiters.get(job_id, []):
            if fut.done():
                continue
            if event == "completed":
                if "result" in data:
                    fut.set_result(data["result"])
                else:
                    # too large or too deep to travel in the event: read it back from
                    # the hash, where the finish script already wrote it
                    self._read_back(job_id, fut)
            elif event == "cancelled":
                fut.set_exception(JobCancelledError(job_id, data.get("reason")))
            else:
                fut.set_exception(JobFailedError(data.get("reason")))

    def _read_back(self, job_id: str, fut: asyncio.Future[Any]) -> None:
        """Resolve a waiter from the job's stored return value.

        The task is held in a set of its own: the loop keeps no reference to a task
        nobody awaits, and a garbage-collected one leaves the waiter hanging forever.
        """

        async def read() -> None:
            job = await self.get_job(job_id)
            if not fut.done():
                fut.set_result(job.returnvalue if job else None)

        task = asyncio.create_task(read())
        self._read_backs.add(task)
        task.add_done_callback(self._read_backs.discard)

    # ---- schedulers (cron / repeatable) -----------------------------------

    @_writes
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
        # The queue's defaults go INTO the template: a worker mints every later
        # occurrence from it, and a worker never sees the producer's defaults.
        merged: dict[str, Any] = {
            **self.default_job_options,
            **job_opts,
            "priority": _clamp_priority(priority),
        }
        opts = JobOptions(**merged).to_dict()
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
                opts.get("concurrencyKey") or "",
                scripts.METRICS_RETENTION_MS,
            ],
        )

    @_writes
    async def remove_scheduler(self, scheduler_id: str) -> None:
        """Stop a schedule and drop its pending occurrence."""
        score = await self.redis.zscore(self.keys.repeat, scheduler_id)
        await self.redis.zrem(self.keys.repeat, scheduler_id)
        await self.redis.delete(self.keys.scheduler(scheduler_id))
        if score is not None:
            await self.remove_job(f"repeat:{scheduler_id}:{int(score)}")

    @_writes
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
        stored = JobOptions.from_dict(json.loads(t.get("opts") or "{}"))
        # Every option the template stored, taken off the options object itself so an
        # option added later cannot be left behind here. Two exceptions: `delay`, since
        # a manual run is now; and retention the template leaves unset (every scheduler
        # registered by an earlier release), which stays the queue's call - passed on as
        # an explicit None it would override `default_job_options`.
        skip = {"delay"} | {
            k for k in ("remove_on_complete", "remove_on_fail") if getattr(stored, k) is None
        }
        opts: dict[str, Any] = {k: v for k, v in asdict(stored).items() if k not in skip}
        await self.add(name, json.loads(t.get("data") or "null"), **opts)
        return True

    async def schedulers(self) -> list[dict[str, Any]]:
        """List active schedulers (for the dashboard)."""
        entries = _scored(await self.redis.zrange(self.keys.repeat, 0, -1, withscores=True))
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
        pipe.zcard(self.keys.held)
        pipe.zcard(self.keys.cancelled)
        (
            wait,
            active,
            delayed,
            completed,
            failed,
            waiting_children,
            held,
            cancelled,
        ) = await pipe.execute()
        return {
            "wait": wait,
            "active": active,
            "delayed": delayed,
            "completed": completed,
            "failed": failed,
            "waiting-children": waiting_children,
            "held": held,
            "cancelled": cancelled,
        }

    async def lifetime_totals(self) -> dict[str, int]:
        """Counters since the queue was created, one per outcome, present at zero.

        A dashboard serving several queues renders them together, so it needs the
        numbers rather than one queue's finished text: a render per queue concatenated
        declares every family twice. This is that seam, so nothing outside has to know
        which key the totals live in.
        """
        raw = _str_dict(await self.redis.hgetall(self.keys.totals))
        totals = dict.fromkeys(OUTCOMES, 0)
        totals.update({k: int(v) for k, v in raw.items() if k in TOTAL_FIELDS})
        return totals

    async def metrics_text(self) -> str:
        """OpenMetrics text for this queue: lifetime counters and current depth.

        A reader, not a collector: both halves come from Redis at scrape time, so N
        replicas scraped independently report the same numbers and no background task
        has to be running for the figures to be right. The depth half is `counts()`:
        every job in the state it is in, which is not what a dashboard's tab badges
        count (those are flow roots, with parked parents folded into active).
        """
        # Depth first, counters second: the two reads have an await between them, and
        # a job that finishes in the gap must show in the counter rather than only in
        # the gauge. Depth above its own counter is impossible in the data.
        depths = await self.counts()
        return render(self.name, await self.lifetime_totals(), depths)

    async def _metric_buckets(self, minutes: int) -> list[tuple[int, dict[str, str]]]:
        """Fetch the last `minutes` per-minute metric buckets, oldest first, in
        one pipelined round trip as (timestamp, hash) pairs. Missing buckets come
        back as empty dicts (zero-fill). The shared read behind every metrics
        method; buckets expire after `scripts.METRICS_RETENTION_MS` (8h).
        """
        minutes = max(1, minutes)
        now_minute = _now_ms() // 60_000 * 60_000
        stamps = [now_minute - 60_000 * i for i in range(minutes - 1, -1, -1)]
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for ts in stamps:
            pipe.hgetall(self.keys.metrics_bucket(ts))
        return list(zip(stamps, _hash_replies(await pipe.execute()), strict=True))

    @staticmethod
    def _percentiles_from(buckets: list[tuple[int, dict[str, str]]], prefix: str) -> dict[str, int]:
        """Merge the duration-histogram fields named ``<prefix><idx>`` across the
        buckets into one histogram, then read p50/p95/p99 - the same maths for
        per-job (`h:`) and whole-flow (`fh:`) durations.
        """
        merged = [0] * scripts.HIST_BUCKETS
        for _ts, h in buckets:
            for field, value in h.items():
                if field.startswith(prefix):
                    merged[int(field.rpartition(":")[2])] += int(value)
        return {q: _percentile(merged, p) for q, p in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99))}

    async def metrics(self, *, minutes: int = 60) -> list[MetricsPoint]:
        """Per-minute completed/failed counts and summed processing duration,
        oldest first, ending at the current minute. Gaps are zero-filled so the
        series is chart-ready. Counters are written atomically inside the finish
        scripts; buckets expire after `scripts.METRICS_RETENTION_MS` (8h), so
        asking for more history than that just returns zeros.
        """
        return [
            MetricsPoint(
                timestamp=ts,
                added=int(h.get("added", 0)),
                completed=int(h.get("completed", 0)),
                failed=int(h.get("failed", 0)),
                ms=int(h.get("ms", 0)),
            )
            for ts, h in await self._metric_buckets(minutes)
        ]

    async def metrics_by_name(self, *, minutes: int = 60) -> list[NameMetrics]:
        """Per-job-name totals over the window, failures first - the triage
        order ("which job is responsible"), not the volume order. Names come
        from the per-name fields the finish scripts write into the same
        minute buckets ("completed:<name>", "failed:<name>", "ms:<name>").
        """
        totals: dict[str, dict[str, int]] = {}
        hists: dict[str, list[int]] = {}
        for _ts, h in await self._metric_buckets(minutes):
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
        return self._percentiles_from(await self._metric_buckets(minutes), "h:")

    async def flow_metrics(self, *, minutes: int = 60) -> list[FlowMetricsPoint]:
        """Per-minute whole-flow completed/failed counts, oldest first and zero-
        filled like metrics(). One unit per root flow - nested sub-flows don't
        count. Buckets expire after `scripts.METRICS_RETENTION_MS` (8h).
        """
        return [
            FlowMetricsPoint(
                timestamp=ts,
                completed=int(h.get("flows:completed", 0)),
                failed=int(h.get("flows:failed", 0)),
            )
            for ts, h in await self._metric_buckets(minutes)
        ]

    async def flow_percentiles(self, *, minutes: int = 60) -> dict[str, int]:
        """End-to-end flow-duration p50/p95/p99 in ms over the window.

        Enqueue to root completion, merged from the "fh:<idx>" histogram;
        completed flows only, 0s when none finished. Distinct from percentiles(),
        which is each job's own runtime - this is the whole flow's wall clock.
        """
        return self._percentiles_from(await self._metric_buckets(minutes), "fh:")

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
                    "global_concurrency": int(h.get("global_concurrency", 0)),
                    "started": int(h.get("started", 0)),
                    "heartbeat": heartbeat,
                    "processed": int(h.get("processed", 0)),
                    "failed": int(h.get("failed", 0)),
                    "cancelled": int(h.get("cancelled", 0)),
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

    @_writes
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
        if state == "active":
            ids = await self.redis.lrange(self.keys.active, start, end)
        elif state in FINISHED_STATES:
            count = -1 if end < 0 else end - start + 1  # -1: to the end, as ZRANGE reads it
            ids = await self._newest_finished(self._finished_zset(state), start, count)
        else:
            ids = await self.redis.zrange(self._state_zset(state), start, end)
        return await self._hydrate_ids(_str_list(ids))

    async def _newest_finished(self, key: str, start: int, count: int) -> list[str]:
        """Page a finished set newest first: what has settled, then a running flow's
        finished children, which score above SETTLED and are not "recent". A negative
        `count` means to the end, which is how Redis reads it too.
        """
        settled = _str_list(await self.redis.zrevrangebyscore(key, SETTLED, "-inf", start, count))
        if 0 <= count == len(settled):
            return settled
        skip = max(0, start - int(await self.redis.zcount(key, "-inf", SETTLED)))
        live = self.redis.zrevrangebyscore(
            key, "+inf", scripts.LIVE_SCORE, skip, -1 if count < 0 else count - len(settled)
        )
        return settled + _str_list(await live)

    async def _hydrate_ids(self, ids: list[str]) -> list[Job]:
        """Load a list of job ids into Jobs in one pipelined round trip (one
        HGETALL per id, not one call per id). Ids whose hash has vanished are
        skipped. The shared tail of every id-list listing (get_jobs, roots).
        """
        if not ids:
            return []
        pipe = self.redis.pipeline(transaction=False)  # read fan-out; no MULTI/EXEC needed
        for job_id in ids:
            pipe.hgetall(self.keys.job(job_id))
        hashes = await pipe.execute()
        return [Job.from_hash(jid, h) for jid, h in zip(ids, hashes, strict=False) if h]

    def _roots_zset(self, state: JobState) -> tuple[str, bool]:
        """Map a ZSET-backed state to (key, newest_first) for the roots diff.
        `active` is a LIST and is handled separately by the caller.
        """
        if state in FINISHED_STATES:  # finished states read newest-first
            return self._finished_zset(state), True
        return self._state_zset(state), False

    async def get_jobs_roots(
        self, state: JobState, start: int = 0, end: int = 20
    ) -> tuple[int, list[Job]]:
        """Page through the ROOT jobs of a state - flow children (any job with a
        parentId) are excluded, since a root-first dashboard shows them only in
        the parent's tree - and return (exact total roots, hydrated page).

        Roots-only and unbounded: paging deep needs no scan cap. `start`/`end`
        are inclusive like get_jobs(); `end < 0` means "to the end". The ZSET
        states diff against the children index in one atomic script (order
        preserved); `active` is a small LIST, filtered in Python.
        """
        if state == "active":
            ids = _str_list(await self.redis.lrange(self.keys.active, 0, -1))
            roots = [j for j in await self._hydrate_ids(ids) if not j.parent_id]
            stop = None if end < 0 else end + 1
            return len(roots), roots[start:stop]
        zset, newest = self._roots_zset(state)
        total, ids = await self._list_roots(
            keys=[zset, self.keys.children, self.keys.roots_scratch],
            args=[start, end, 1 if newest else 0],
        )
        return int(total), await self._hydrate_ids(_str_list(ids))

    async def roots_counts(self) -> dict[str, int]:
        """Exact roots-only count per state - the root-first counterpart of
        counts(). One atomic script diffs each state set against the children
        index (`active`, a LIST, is counted by membership). `wait` is the
        prioritized set; `waiting-children` is the parked flow parents (each a
        root unless itself nested).
        """
        res = await self._roots_counts_script(
            keys=[
                self.keys.prioritized,
                self.keys.delayed,
                self.keys.completed,
                self.keys.failed,
                self.keys.waiting_children,
                self.keys.active,
                self.keys.held,
                self.keys.children,
                self.keys.roots_scratch,
                self.keys.cancelled,
            ],
        )
        (wait, delayed, completed, failed, waiting_children, held, cancelled, active) = (
            int(x) for x in res
        )
        return {
            "wait": wait,
            "active": active,
            "delayed": delayed,
            "completed": completed,
            "failed": failed,
            "waiting-children": waiting_children,
            "held": held,
            "cancelled": cancelled,
        }

    def _retry_job_keys(self, job_id: str) -> list[str]:
        """Build the six KEYS the RETRY_JOB script takes - one definition so
        retry_job, retry_all_failed and retry_flow can never drift apart.
        """
        return [
            self.keys.failed,
            self.keys.prioritized,
            self.keys.marker,
            self.keys.job(job_id),
            self.keys.pc,
            self.keys.base,
        ]

    @_writes
    async def retry_job(self, job_id: str) -> bool:
        """Move a failed job back to the queue for another attempt.

        Flow-aware: retrying a flow PARENT re-drives its whole failed subtree, not
        the parent alone. The parent re-parks on every non-completed child
        (completed children keep their collected results; failed and still-pending
        ones stay in the barrier) and each failed descendant is re-queued root-first
        - so a parent that failed because a child failed recovers in one call
        instead of stranding on that still-failed child. A retried child re-joins
        its parked parent's barrier. (retry_all_failed and retry_flow drive the
        per-job script directly, so this convenience does not change them.)

        The parent path keys off being a flow parent, not off being failed: called
        on a parent that is not itself failed (e.g. still in-flight in
        `waiting-children` with a `continue`-failed child), it re-drives the
        subtree's failed nodes instead of being a no-op as it is for a non-failed
        plain job. Pass a leaf child id to retry just that one job.
        """
        job = await self.get_job(job_id)
        if job is not None and job.children_ids:  # a flow parent: recover the subtree
            return await self.retry_flow(job_id) > 0
        res = await self._retry_job(keys=self._retry_job_keys(job_id), args=[job_id, _now_ms()])
        return bool(res)

    def _remove_job_keys(self) -> list[str]:
        """Build the KEYS the REMOVE_JOB script takes (every state set plus the base;
        the job id rides in as an ARGV) - one definition so remove_job and clean can't
        drift apart. `held` is last: the base sat there before the state existed.
        """
        return [
            self.keys.prioritized,
            self.keys.active,
            self.keys.delayed,
            self.keys.completed,
            self.keys.failed,
            self.keys.waiting_children,
            self.keys.base,
            self.keys.held,
            self.keys.cancelled,
            self.keys.cancel,
        ]

    @_writes
    async def remove_job(self, job_id: str) -> bool:
        """Delete a job from every state and drop its hash.

        A flow parent takes its whole subtree with it (children included,
        even mid-flight); removing a pending child releases its parent when
        nothing else is left to wait for.
        """
        res = await self._remove_job(keys=self._remove_job_keys(), args=[job_id, _now_ms()])
        return bool(res)

    @_writes
    async def cancel_job(self, job_id: str, *, reason: str | None = None) -> bool:
        """Stop a job wherever it is. True when there was something to stop.

        `reason` is recorded on every job the call stops, the subtree included, and
        reaches whoever is waiting on `result()`. "Who stopped this and why" is the
        first question asked of a cancelled job.

        A job that has not started ends here and now. A RUNNING job is asked to stop:
        its worker owns the processor, so only the worker can cancel it, which it does
        as soon as it hears (over the events channel, or at its next lock renewal).
        Either way the job ends in `cancelled`, which is not a failure and is not
        retried. A job that has already finished, or that is gone, returns False.
        """
        res = await self._cancel_job(
            keys=[
                self.keys.prioritized,
                self.keys.delayed,
                self.keys.held,
                self.keys.waiting_children,
                self.keys.cancelled,
                self.keys.base,
                self.keys.events,
                self.keys.cancel,
            ],
            args=[str(job_id), _now_ms(), scripts.METRICS_RETENTION_MS, reason or ""],
        )
        return bool(res)

    @_writes
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
        if state == "active":
            return _str_list(await self.redis.lrange(self.keys.active, 0, limit - 1))
        if state in FINISHED_STATES:
            zset = self._finished_zset(state)
            if newest:
                return await self._newest_finished(zset, 0, limit)
            # oldest first, and settled only: a running flow's finished children are
            # not history to clean
            return _str_list(await self.redis.zrangebyscore(zset, "-inf", SETTLED, 0, limit))
        return _str_list(await self.redis.zrange(self._state_zset(state), 0, limit - 1))

    def _finished_zset(self, state: JobState) -> str:
        """Name the ZSET a finished state lists from; all of them read newest-first."""
        if state == "completed":
            return self.keys.completed
        return self.keys.failed if state == "failed" else self.keys.cancelled

    def _state_zset(self, state: JobState) -> str:
        """Name the ZSET a non-active, non-finished state lists from.

        The one such mapping: `get_jobs`, `_ids` and `_roots_zset` all read it, so a
        new state joins the listings by being added here and nowhere else.
        """
        if state in ("wait", "prioritized"):
            return self.keys.prioritized
        if state == "delayed":
            return self.keys.delayed
        if state == "held":
            return self.keys.held
        if state == "waiting-children":
            return self.keys.waiting_children
        msg = f"unknown state: {state}"
        raise ValueError(msg)

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

    @_writes
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
            pipe.evalsha(sha, 6, *self._retry_job_keys(job_id), job_id, now)
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

    @_writes
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
            pipe.evalsha(sha, 6, *self._retry_job_keys(job_id), job_id, now)
        res = await pipe.execute()
        return sum(1 for r in res if r)

    @_writes
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
        now = _now_ms()
        pipe = self.redis.pipeline(transaction=False)
        keys = self._remove_job_keys()
        for job_id in ids:
            pipe.evalsha(sha, len(keys), *keys, job_id, now)
        await pipe.execute()
        return len(ids)

    # ---- queue control ----------------------------------------------------

    @_writes
    async def pause(self) -> None:
        """Stop workers from claiming new jobs (in-flight jobs still finish)."""
        await self.redis.set(self.keys.meta_paused, "1")

    @_writes
    async def resume(self) -> None:
        """Resume claiming, and wake idle workers."""
        await self.redis.delete(self.keys.meta_paused)
        await self.redis.zadd(self.keys.marker, {"0": 0})

    async def is_paused(self) -> bool:
        return bool(await self.redis.exists(self.keys.meta_paused))

    async def close(self) -> None:
        # Every step in a finally: a pub/sub close that raises would otherwise skip
        # the connection close and leak the pool, which is the one thing this method
        # exists to prevent.
        try:
            if self._events_task is not None:
                self._events_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._events_task
                self._events_task = None
            if self._events_pubsub is not None:
                with contextlib.suppress(Exception):
                    await self._events_pubsub.aclose()
                self._events_pubsub = None
            # Fail anyone still awaiting result() fast, rather than leaving them to
            # sit out their timeout against a closed connection.
            for waiters in self._result_waiters.values():
                for fut in waiters:
                    if not fut.done():
                        fut.set_exception(RuntimeError("queue closed while waiting for a result"))
        finally:
            await self.redis.aclose(close_connection_pool=self._owns_connection)


class PendingJobs:
    """Jobs collected during a transaction, sent only once it has committed.

    Built by `Queue.pending()`. The adds have the same signatures they have on the
    queue and record what to send; `flush()` sends the batch in one round trip.

    It defers, it does not guarantee: a process that dies between the commit and the
    flush sends nothing. Closing that needs an outbox table and a relay, which is a
    database integration and a different product. What this closes is the common half,
    a job about a row that was rolled back.
    """

    def __init__(self, queue: Queue) -> None:
        self._queue = queue
        # Staged at flush, not here: an id is a counter value, and a job that is never
        # sent must not consume one. Held as the call, so nothing is encoded until the
        # caller says the write went through.
        self._calls: list[Callable[[], _Staged]] = []

    def __len__(self) -> int:
        return len(self._calls)

    def add(
        self,
        name: str,
        data: Any = None,
        *,
        job_id: str | None = None,
        deduplication: Deduplication | None = None,
        **opts: Any,
    ) -> None:
        """Collect one job. Same arguments as `Queue.add`; nothing is sent yet."""
        # A copy, because between collecting a job and sending it is exactly where a
        # caller fills in an id or scrubs a secret, and `Queue.add` encodes at the
        # call, so it snapshots. Holding the caller's object would send what it became.
        snapshot = copy.deepcopy(data)
        self._calls.append(
            lambda: self._queue._stage_add(  # noqa: SLF001 - the queue's own staging
                name, snapshot, job_id=job_id, deduplication=deduplication, opts=opts
            )
        )

    def add_flow(
        self, name: str, data: Any = None, *, children: list[FlowChild], **opts: Any
    ) -> None:
        """Collect one flow. Same arguments as `Queue.add_flow`; nothing is sent yet."""
        snapshot, tree = copy.deepcopy(data), copy.deepcopy(children)  # as in `add`
        self._calls.append(
            lambda: self._queue._stage_flow(  # noqa: SLF001 - the queue's own staging
                name, snapshot, children=tree, opts=opts
            )
        )

    def discard(self) -> None:
        """Throw the batch away: the transaction rolled back, so nothing happened."""
        self._calls.clear()

    async def flush(self) -> list[Job]:
        """Send everything collected, in order, and return the jobs.

        The batch is taken before the first await, so a hook that fires twice, or two
        flushes in flight, cannot both send it. Staging happens next and for the whole
        batch, so an option error or a value that will not encode raises with nothing
        sent and the batch still in hand.

        Redis has no rollback: if a script fails for one job the others are already
        enqueued. That raises `PartialFlushError`, naming what was sent, and leaves
        exactly what did not send in the buffer, so a retry cannot double anything.

        A flush that never reaches Redis at all (a dead connection) keeps the whole
        batch, because nothing can say how much of it landed: a retry may duplicate,
        which is the direction an at-least-once queue errs in.
        """
        calls, self._calls = self._calls, []
        if not calls:
            return []
        try:
            staged = [stage() for stage in calls]
            await self._queue._stamp_model()  # noqa: SLF001 - the queue's own check
        except BaseException:
            self._calls = calls + self._calls  # nothing was sent; the batch stands
            raise
        try:
            async with self._queue.redis.pipeline(transaction=False) as pipe:
                for item in staged:
                    await item.script(keys=item.keys, args=item.args, client=pipe)
                replies = await pipe.execute(raise_on_error=False)
        except BaseException:
            # The connection died somewhere in there and nothing can say how much of
            # the batch landed. Keeping it means a retry may duplicate; dropping it
            # means the jobs are gone with no record of what they were, after the
            # transaction they belong to has committed. This queue is at-least-once
            # by design, so duplicating beats losing.
            self._calls = calls + self._calls
            raise
        sent: list[Job] = []
        failed: list[tuple[Callable[[], _Staged], BaseException]] = []
        for call, item, reply in zip(calls, staged, replies, strict=True):
            if isinstance(reply, BaseException):
                failed.append((call, reply))
            else:
                sent.append(item.build(reply))
        if failed:
            # in front, so a batch collected after this one still goes out behind it
            self._calls = [call for call, _ in failed] + self._calls
            raise PartialFlushError(sent, [error for _, error in failed])
        return sent
