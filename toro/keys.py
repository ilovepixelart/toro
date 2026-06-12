"""Central place that knows how Redis keys are laid out for a queue.

Keeping this in one spot means the Lua scripts and the Python side can never
disagree about where a list/zset/hash lives.
"""

from __future__ import annotations


class Keys:
    """Computes the Redis key names for one queue from its prefix + name."""

    def __init__(self, queue_name: str, prefix: str = "toro") -> None:
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
    def meta_paused(self) -> str:
        # Existence flag: when set, workers stop claiming new jobs.
        return f"{self.base}meta-paused"

    @property
    def events(self) -> str:
        # Pub/sub channel for job outcomes (completed/failed) — drives result() and live UI.
        return f"{self.base}events"

    @property
    def limiter(self) -> str:
        # Token-bucket hash {tokens, ts} — the queue-wide rate limit, shared by all workers.
        return f"{self.base}limiter"

    @property
    def stalled(self) -> str:
        return f"{self.base}stalled"

    @property
    def stalled_check(self) -> str:
        return f"{self.base}stalled-check"

    @property
    def repeat(self) -> str:
        # ZSET of scheduler ids -> next-run timestamp.
        return f"{self.base}repeat"

    def scheduler(self, scheduler_id: str) -> str:
        # HASH holding a scheduler's template (name, every/cron, data, opts).
        return f"{self.base}repeat:{scheduler_id}"

    @property
    def workers(self) -> str:
        # ZSET of live worker ids -> last-heartbeat timestamp (ms). Stale entries
        # are pruned lazily on read; powers the dashboard's "who's running" view.
        return f"{self.base}workers"

    def worker(self, worker_id: str) -> str:
        # HASH with a worker's presence record (host, pid, concurrency, counts, ...).
        return f"{self.base}worker:{worker_id}"

    @property
    def departed(self) -> str:
        # Capped LIST of recent worker departures: graceful stop ("stopped") or a
        # lost heartbeat ("lost" = crashed/killed). Gives the dashboard death history.
        return f"{self.base}departed"

    def metrics_bucket(self, minute_ms: int) -> str:
        # HASH of per-minute counters: added/completed/failed/ms at queue level
        # plus per-name fields ("completed:<name>", ...). Written inside the
        # add/finish scripts, self-expiring after METRICS_RETENTION_MS.
        return f"{self.base}metrics:{minute_ms}"

    def job(self, job_id: str | int) -> str:
        return f"{self.base}{job_id}"

    def lock(self, job_id: str | int) -> str:
        return f"{self.base}{job_id}:lock"

    def logs(self, job_id: str | int) -> str:
        return f"{self.base}{job_id}:logs"
