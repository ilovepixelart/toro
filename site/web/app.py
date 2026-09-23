"""The toro website, served by the stack it documents.

FastAPI renders every page on the server, htmx swaps the fragments that change,
and the queue on the landing page is a real toro queue on a real Redis with real
workers. The dashboard at /dashboard is matador itself, mounted the same way the
documentation tells you to mount it.

Nothing here is a mock: killing a worker cancels its task without a graceful
stop, exactly as a crashed process would, and the jobs it was holding come back
through toro's own stalled sweep.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
from collections.abc import AsyncIterator
from pathlib import Path
from textwrap import dedent
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from matador import create_app
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer
from toro import FlowChild, Queue, Worker

HERE = Path(__file__).parent
REDIS_URL = os.environ.get("TORO_SITE_REDIS", "redis://localhost:6379")
PREFIX = "torosite"
QUEUE = "demo"

# Short on purpose: a visitor should see a stalled job recovered within a few
# seconds of killing a worker, not within the 30s a production lock would use.
LOCK_SECONDS = 4.0
STALLED_SWEEP = 2.0

JOB_NAMES = [
    ("welcome-email", 0.9),
    ("invoice-pdf", 1.6),
    ("thumbnail", 0.7),
    ("webhook-delivery", 0.5),
    ("nightly-digest", 2.1),
    ("search-reindex", 1.2),
]

_templates = Jinja2Templates(directory=str(HERE / "templates"))


def _highlight(source: str) -> str:
    """Colour code on the server, the way matador colours job payloads.

    Pygments ships with matador, so this costs no new dependency and no client
    JavaScript: the browser receives finished HTML.
    """
    return highlight(source.strip(), PythonLexer(), HtmlFormatter(nowrap=True))


_templates.env.filters["py"] = _highlight


def _asset_v() -> int:
    """Newest mtime across the static assets, so a rebuilt stylesheet is fetched
    rather than served from cache. matador does the same; leaving it out is why
    the first restyle appeared to do nothing."""
    files = (HERE / "static").rglob("*")
    return int(max((f.stat().st_mtime for f in files if f.is_file()), default=0))


_templates.env.globals["asset_v"] = _asset_v

# The use-case browser. Every snippet is lifted from toro's README, so the page
# cannot drift from the API it is advertising without the README drifting too.
EXAMPLES: dict[str, tuple[str, str]] = {
    "retries": (
        "Retries with backoff",
        dedent("""\
            await queue.add(
                "report", data,
                attempts=5,
                backoff={"type": "exponential", "delay": 1000},
            )"""),
    ),
    "priority": (
        "Priority and delay",
        'await queue.add("report", data, priority=10, delay=5000)',
    ),
    "dedup": (
        "Idempotent enqueue",
        dedent("""\
            # a second add with the same id is ignored, not queued twice
            await queue.add("charge", data, job_id="order-1234")"""),
    ),
    "cron": (
        "Cron and repeatable",
        dedent("""\
            # every occurrence schedules the next, so a missed tick cannot compound
            await queue.add_scheduler("nightly-rollup", cron="0 0 * * *")"""),
    ),
    "ratelimit": (
        "Rate limit",
        dedent("""\
            # at most 100 jobs a second across every worker on the queue
            worker = Worker(
                "emails", process,
                rate_limit={"max": 100, "duration": 1000},
            )"""),
    ),
    "transaction": (
        "Enqueue with a transaction",
        dedent("""\
            # a job enqueued before its transaction commits points at a row a
            # rollback may take away
            pending = queue.pending()

            session.add(user)
            await session.flush()                           # the row gets its id
            pending.add("welcome", {"user_id": user.id})    # nothing sent yet

            await session.commit()
            await pending.flush()                           # one pipelined write"""),
    ),
    "sync": (
        "Sync processor",
        dedent("""\
            # a plain def is a processor too: it runs in the worker's own threads,
            # so the loop stays free to renew locks and answer heartbeats
            def handle(job):
                return requests.get(job.data["url"]).json()

            await Worker("fetch", handle, concurrency=8).run()"""),
    ),
    "cancel": (
        "Cancel a running job",
        dedent("""\
            async def process(job):
                upload = await open_upload(job.data["path"])
                try:
                    await upload.stream()    # cancellation is raised here
                finally:
                    await upload.abort()     # and this still runs

            # from anywhere: the job ends in `cancelled`, not `failed`
            await queue.cancel_job(job_id, reason="user closed the tab")"""),
    ),
    "progress": (
        "Progress and logs",
        dedent("""\
            async def process(job):
                for i, row in enumerate(rows):
                    await job.update_progress(round(i / len(rows) * 100))
                    await job.log(f"imported {row.id}")
                return {"rows": len(rows)}"""),
    ),
    "flows": (
        "Flows",
        dedent("""\
            from toro import FlowChild as c

            # children fan out, the parent runs on their results
            report = await queue.add_flow(
                "report", {"q": 3},
                children=[c("fetch", {"shard": i}) for i in range(3)],
            )"""),
    ),
    "result": (
        "Wait for a result",
        dedent("""\
            job = await queue.add("resize", {"src": "a.png"})
            print(await job.result(timeout=30))"""),
    ),
}


async def process(job: Any) -> dict[str, Any]:
    """A demo job: takes a plausible amount of time, and sometimes fails."""
    await asyncio.sleep(job.data.get("takes", 1.0))
    if job.data.get("doomed") and job.attempts_made < 2:
        raise RuntimeError("upstream returned 503")
    return {"ok": True}


class Demo:
    """Owns the queue, the workers and the producer behind the landing page."""

    def __init__(self) -> None:
        self.queue = Queue(QUEUE, url=REDIS_URL, prefix=PREFIX)
        self.workers: dict[str, tuple[Worker, asyncio.Task[None]]] = {}
        self.producer: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self._clear()
        for name in ("worker-1", "worker-2"):
            self._spawn(name)
        # A repeatable job, so the schedulers panel shows the feature the page
        # claims rather than an empty state.
        with contextlib.suppress(Exception):
            await self.queue.add_scheduler(
                "hourly-rollup", every=45_000, name="search-reindex", data={"takes": 1.2}
            )
        self.producer = asyncio.create_task(self._produce())

    def _spawn(self, name: str) -> None:
        worker = Worker(
            QUEUE,
            process,
            url=REDIS_URL,
            prefix=PREFIX,
            concurrency=3,
            lock_duration=LOCK_SECONDS,
            stalled_interval=STALLED_SWEEP,
        )
        self.workers[name] = (worker, asyncio.create_task(worker.run()))

    async def _produce(self) -> None:
        """Keep the queue busy enough to look alive, idle enough to read.

        Every so often it enqueues a flow instead of a plain job, because the
        page advertises fan-out/fan-in and a demo that never runs one is a demo
        of something else.
        """
        while True:
            with contextlib.suppress(Exception):
                if random.random() < 0.18:
                    await self._flow()
                else:
                    name, takes = random.choice(JOB_NAMES)
                    await self.queue.add(
                        name,
                        {"takes": takes, "doomed": random.random() < 0.18},
                        attempts=3,
                        remove_on_complete=40,
                        remove_on_fail=20,
                    )
            await asyncio.sleep(random.uniform(0.6, 1.8))

    async def _flow(self) -> None:
        """One parent, three children: the shape the landing page describes."""
        await self.queue.add_flow(
            "send-digest",
            {"takes": 0.6, "user": random.randint(100, 999)},
            children=[
                FlowChild("render-header", {"takes": 0.4}),
                FlowChild("render-body", {"takes": 0.9}),
                FlowChild("render-footer", {"takes": 0.3, "doomed": random.random() < 0.3}),
            ],
            attempts=2,
            remove_on_complete=20,
        )

    async def crash(self, name: str) -> int:
        """Kill a worker the way a crash kills one: no graceful stop, no unlock.

        Returns how many jobs it was holding, which is the number toro's stalled
        sweep has to hand back.
        """
        entry = self.workers.pop(name, None)
        if entry is None:
            return 0
        worker, task = entry
        held = len(getattr(worker, "_processors", {}) or {})
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        return held

    async def revive(self, name: str) -> None:
        if name not in self.workers:
            self._spawn(name)

    async def snapshot(self) -> dict[str, Any]:
        counts = await self.queue.counts()
        live = await self.queue.workers()
        return {
            "counts": counts,
            "workers": sorted(self.workers),
            "live": len(live),
            "total": (await self.queue.lifetime_totals()),
        }

    async def _clear(self) -> None:
        keys = await self.queue.redis.keys(self.queue.keys.base + "*")
        if keys:
            await self.queue.redis.delete(*keys)

    async def stop(self) -> None:
        if self.producer:
            self.producer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.producer
        for worker, task in self.workers.values():
            with contextlib.suppress(Exception):
                await worker.stop()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        await self.queue.close()


demo = Demo()


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await demo.start()
    try:
        yield
    finally:
        await demo.stop()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
# NOT name="static": matador's templates resolve their assets with
# url_for("static", ...), and a route of that name on the outer app wins, so the
# dashboard's own JavaScript would 404. Found by mounting it here.
app.mount("/assets", StaticFiles(directory=str(HERE / "static")), name="assets")

# The dashboard is not a screenshot: it is matador, mounted exactly as the docs
# say to mount it, reading the same queue the landing page is filling.
app.mount(
    "/dashboard",
    create_app([QUEUE], url=REDIS_URL, prefix=PREFIX, require_same_origin=False),
)


@app.get("/", response_class=HTMLResponse)
async def landing(request: Request) -> HTMLResponse:
    return _templates.TemplateResponse(request, "index.html", await _context())


@app.get("/examples/{key}", response_class=HTMLResponse)
async def example(request: Request, key: str) -> HTMLResponse:
    """One snippet, chosen on the server. The browser receives finished HTML."""
    chosen = key if key in EXAMPLES else next(iter(EXAMPLES))
    return _templates.TemplateResponse(
        request,
        "partials/example.html",
        {"examples": EXAMPLES, "chosen": chosen},
    )


@app.get("/pulse", response_class=HTMLResponse)
async def pulse(request: Request) -> HTMLResponse:
    """The live strip, re-fetched by htmx. One fragment, no client state."""
    return _templates.TemplateResponse(request, "partials/pulse.html", await _context())


@app.post("/demo/crash/{name}", response_class=HTMLResponse)
async def crash(request: Request, name: str) -> HTMLResponse:
    held = await demo.crash(name)
    context = await _context()
    context["flash"] = f"{name} died holding {held} job{'s' if held != 1 else ''}"
    return _templates.TemplateResponse(request, "partials/pulse.html", context)


@app.post("/demo/revive/{name}", response_class=HTMLResponse)
async def revive(request: Request, name: str) -> HTMLResponse:
    await demo.revive(name)
    context = await _context()
    context["flash"] = f"{name} back up"
    return _templates.TemplateResponse(request, "partials/pulse.html", context)


async def _context() -> dict[str, Any]:
    state = await demo.snapshot()
    return {
        "examples": EXAMPLES,
        "chosen": next(iter(EXAMPLES)),
        "counts": state["counts"],
        "workers": state["workers"],
        "totals": state["total"],
        "all_workers": ["worker-1", "worker-2"],
        "flash": None,
    }
