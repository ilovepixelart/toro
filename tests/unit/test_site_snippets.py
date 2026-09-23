"""Unit: the code on the website is code this library actually has.

A landing page is read as a promise about the API. Nothing stops a snippet from
drifting into something plausible that does not exist: `job.children_values()`
read perfectly well and was never a method. Parsing the snippets and checking
every call against the real objects is the only way that stays true.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import pathlib
import re
import subprocess
import sys
import textwrap
from typing import Any

import pytest

import toro
from toro import FlowChild, Job, JobOptions, Queue, Worker

SITE = pathlib.Path(__file__).resolve().parents[2] / "site"
INDEX = SITE / "web" / "templates" / "index.html"

# Receiver name in the snippets -> the class it stands for.
RECEIVERS: dict[str, type] = {"queue": Queue, "job": Job, "worker": Worker}
# Constructors the snippets call directly.
CONSTRUCTORS: dict[str, Any] = {"Queue": Queue, "Worker": Worker, "FlowChild": FlowChild}


def _snippets() -> dict[str, str]:
    """Every Python snippet the page renders: the template's `set` blocks and
    the example browser's own table."""
    blocks = re.findall(r"\{%\s*set\s+(\w+)\s*=\s*'''(.*?)'''\s*%\}", INDEX.read_text(), re.DOTALL)
    out: dict[str, str] = {
        name: body
        for name, body in blocks
        if any(token in body for token in ("import", "await", "def "))
    }
    # Read the example table rather than import it: the site has its own
    # dependencies (FastAPI, Jinja) that the library's test environment does not.
    app = ast.parse((SITE / "web" / "app.py").read_text())
    for node in ast.walk(app):
        if not isinstance(node, ast.AnnAssign | ast.Assign):
            continue
        targets = [node.target] if isinstance(node, ast.AnnAssign) else node.targets
        if not any(isinstance(t, ast.Name) and t.id == "EXAMPLES" for t in targets):
            continue
        for key, value in zip(node.value.keys, node.value.values, strict=True):
            source = value.elts[1]
            if isinstance(source, ast.Call):  # dedent("""...""")
                source = source.args[0]
            out[f"example:{key.value}"] = textwrap.dedent(source.value)
    return out


def _tree(source: str) -> ast.Module:
    """Snippets use top-level `await`, which only parses inside a function."""
    return ast.parse("async def _():\n" + textwrap.indent(source, "    "))


def _accepts(target: Any) -> set[str]:
    """Parameter names taken from the source, not from the live object.

    The suite's own conftest replaces `Worker.__init__` with `(*args, **kw)` to
    shorten timeouts, so a runtime signature here would accept anything and this
    check would pass on a snippet that cannot run.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(target)))
    node = tree.body[0]
    if isinstance(node, ast.ClassDef):
        node = next(
            (n for n in node.body if isinstance(n, ast.FunctionDef) and n.name == "__init__"),
            None,
        )
        if node is None:
            return set()
    args = node.args
    names = {a.arg for a in [*args.posonlyargs, *args.args, *args.kwonlyargs]} - {"self"}
    if args.kwarg is not None:
        # `**opts` on Queue.add means the job options are the real contract
        names |= {f.name for f in dataclasses.fields(JobOptions)}
    return names


SNIPPETS = _snippets()


def test_the_page_has_snippets_to_check():
    assert SNIPPETS, "found no Python snippets in the page"


@pytest.mark.parametrize("name", sorted(SNIPPETS))
def test_every_method_the_snippet_calls_exists(name: str):
    unknown: list[str] = []
    for node in ast.walk(_tree(SNIPPETS[name])):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        receiver = node.func.value
        if isinstance(receiver, ast.Name) and receiver.id in RECEIVERS:
            cls = RECEIVERS[receiver.id]
            if not hasattr(cls, node.func.attr):
                unknown.append(f"{receiver.id}.{node.func.attr}() is not on {cls.__name__}")
    assert unknown == [], f"{name}: {unknown}"


@pytest.mark.parametrize("name", sorted(SNIPPETS))
def test_every_keyword_the_snippet_passes_is_accepted(name: str):
    wrong: list[str] = []
    for node in ast.walk(_tree(SNIPPETS[name])):
        if not isinstance(node, ast.Call):
            continue
        target = None
        if isinstance(node.func, ast.Name) and node.func.id in CONSTRUCTORS:
            target = CONSTRUCTORS[node.func.id]
        elif (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in RECEIVERS
        ):
            target = getattr(RECEIVERS[node.func.value.id], node.func.attr, None)
        if target is None:
            continue
        allowed = _accepts(target)
        name_of = getattr(target, "__name__", str(target))
        wrong.extend(
            f"{name_of}(... {keyword.arg}=) not accepted"
            for keyword in node.keywords
            if keyword.arg and keyword.arg not in allowed
        )
    assert wrong == [], f"{name}: {wrong}"


@pytest.mark.parametrize("name", sorted(SNIPPETS))
def test_every_name_the_snippet_imports_from_toro_is_public(name: str):
    missing: list[str] = []
    for node in ast.walk(_tree(SNIPPETS[name])):
        if isinstance(node, ast.ImportFrom) and node.module == "toro":
            missing.extend(a.name for a in node.names if a.name not in toro.__all__)
    assert missing == [], f"{name} imports names toro does not export: {missing}"


def test_the_test_count_on_the_page_is_the_real_one():
    """The page prints a test count as evidence. A number that drifts is worse
    than no number, so it is checked against what pytest actually collects."""
    claimed = re.search(r"\('Tests', '(\d+)\+?,", INDEX.read_text())
    assert claimed, "the page no longer states a test count"
    collected = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider"],
        cwd=INDEX.parents[3],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    total = re.search(r"(\d+)/(\d+) tests collected", collected) or re.search(
        r"(\d+) tests collected", collected
    )
    assert total, f"could not read the collected count from: {collected[-200:]}"
    actual, floor = int(total.group(1)), int(claimed.group(1))
    # A floor, not an exact count: every new test would otherwise edit the page.
    assert actual >= floor, f"the page claims {floor}+ tests, the suite collects {actual}"
    assert actual - floor < 100, (
        f"the page claims {floor}+ but the suite has {actual}: raise the number"
    )
