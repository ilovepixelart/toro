"""Render the site to static HTML, the way GitHub Pages would serve it.

The live queue panel cannot exist on a static host, so `static=True` swaps it for
a still. Everything else - including the htmx example switcher, which only ever
does a GET for HTML - is written out as files and keeps working.
"""

from __future__ import annotations

import pathlib
import shutil
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))

from jinja2 import Environment, FileSystemLoader

from web.app import EXAMPLES, _highlight

HERE = pathlib.Path(__file__).parent
DIST = HERE / "dist"
# A project page lives at /<repo>/, so every absolute path needs that prefix.
BASE = sys.argv[1] if len(sys.argv) > 1 else ""


def main() -> None:
    if DIST.exists():
        shutil.rmtree(DIST)
    (DIST / "examples").mkdir(parents=True)

    env = Environment(
        loader=FileSystemLoader(HERE / "web" / "templates"),
        autoescape=True,
    )
    env.filters["py"] = _highlight
    env.globals["asset_v"] = lambda: 1
    env.globals["static"] = True
    env.globals["base"] = BASE
    # No demo instance is deployed yet, so nothing links to one. When there is,
    # this becomes its URL and the dashboard links point at it.
    env.globals["demo_url"] = "https://github.com/ilovepixelart/toro#readme"

    counts = {"wait": 0, "active": 2, "completed": 128, "failed": 3}
    common = {
        "examples": EXAMPLES,
        "chosen": next(iter(EXAMPLES)),
        "counts": counts,
        "workers": ["worker-1", "worker-2"],
        "all_workers": ["worker-1", "worker-2"],
        "totals": {"completed": 128, "failed": 3},
        "flash": None,
        "request": None,
    }

    (DIST / "index.html").write_text(env.get_template("index.html").render(**common))
    for key in EXAMPLES:
        html = env.get_template("partials/example.html").render(**common | {"chosen": key})
        (DIST / "examples" / f"{key}.html").write_text(html)

    shutil.copytree(HERE / "web" / "static", DIST / "assets")
    files = sorted(p.relative_to(DIST).as_posix() for p in DIST.rglob("*") if p.is_file())
    print(f"wrote {len(files)} files to dist/")
    for f in files[:12]:
        print("  ", f)


main()
