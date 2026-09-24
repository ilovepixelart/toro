"""Render the site to static HTML, the way GitHub Pages would serve it.

The htmx example switcher only ever does a GET for HTML, which a file on disk
answers as well as a route, so `static=True` changes nothing but the extension it
asks for.
"""

from __future__ import annotations

import pathlib
import re
import shutil
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))

from jinja2 import Environment, FileSystemLoader

from web.app import DOCS_URL, EXAMPLES, MATADOR_URL, SITE_URL, _asset_v, _highlight

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
    env.globals["asset_v"] = _asset_v
    env.globals["static"] = True
    env.globals["base"] = BASE
    env.globals["docs_url"] = DOCS_URL
    env.globals["matador_url"] = MATADOR_URL
    env.globals["site_url"] = SITE_URL

    common = {"examples": EXAMPLES, "chosen": next(iter(EXAMPLES))}

    (DIST / "index.html").write_text(env.get_template("index.html").render(**common))
    for key in EXAMPLES:
        html = env.get_template("partials/example.html").render(**common | {"chosen": key})
        (DIST / "examples" / f"{key}.html").write_text(html)

    shutil.copytree(HERE / "web" / "static", DIST / "assets")
    files = sorted(p.relative_to(DIST).as_posix() for p in DIST.rglob("*") if p.is_file())
    print(f"wrote {len(files)} files to dist/")
    _verify(files)


def _verify(files: list[str]) -> None:
    """Fail the build on a link that goes nowhere.

    A static site has no server to notice a 404, and the one thing worse than a
    missing page is a published page that quietly points at one. This runs on
    every build so nobody has to remember to check.
    """
    known = {f"{BASE}/{f}" for f in files} | {f"/{f}" for f in files}
    broken: list[str] = []
    placeholders: list[str] = []
    for page in DIST.rglob("*.html"):
        html = page.read_text()
        for url in re.findall(r'(?:href|src|hx-get|hx-post)="([^"]+)"', html):
            if url.startswith(("http://", "https://", "data:", "#", "mailto:")):
                if "example.com" in url or "demo.example" in url:
                    placeholders.append(f"{page.name}: {url}")
                continue
            if url.split("?")[0] not in known:
                broken.append(f"{page.name}: {url}")
    for label, found in (("broken local links", broken), ("placeholder URLs", placeholders)):
        if found:
            print(f"\n{label}:")
            for item in sorted(set(found)):
                print("   ", item)
    if broken or placeholders:
        raise SystemExit(1)
    print(f"verified: every local link resolves, no placeholder URLs ({len(files)} files)")


main()
