"""The toro website.

One set of Jinja templates, rendered two ways: this app for editing them with a
reload, and `build_static.py` for what GitHub Pages serves. Both render the same
templates from the same example table, so the published page cannot drift from
the one written against.
"""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer

HERE = Path(__file__).parent
# Read by build_static.py too, so the live render and the published one link to
# the same places.
DOCS_URL = "https://github.com/ilovepixelart/toro/tree/main/docs"
MATADOR_URL = "https://github.com/ilovepixelart/matador"

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


_templates.env.globals.update(asset_v=_asset_v, docs_url=DOCS_URL, matador_url=MATADOR_URL)

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


app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
app.mount("/assets", StaticFiles(directory=str(HERE / "static")), name="assets")


@app.get("/", response_class=HTMLResponse)
async def landing(request: Request) -> HTMLResponse:
    return _templates.TemplateResponse(request, "index.html", _context())


@app.get("/examples/{key}", response_class=HTMLResponse)
async def example(request: Request, key: str) -> HTMLResponse:
    """One snippet, chosen on the server. The browser receives finished HTML."""
    return _templates.TemplateResponse(request, "partials/example.html", _context(key))


def _context(chosen: str | None = None) -> dict[str, object]:
    return {"examples": EXAMPLES, "chosen": chosen if chosen in EXAMPLES else next(iter(EXAMPLES))}
