"""Unit: the things that broke this release and had no check.

Docs prose, a version string and a lockfile are not behavior, so none of them arrives
through a failing test the way a feature does. That does not make them uncheckable,
and each rule here corresponds to something that actually went wrong: a stale lock
failed CI four times in a row, and a docs page that nothing links to is a page nobody
reads.
"""

import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
INDEX = DOCS / "index.md"
LINK = re.compile(r"\[[^\]]+\]\((?P<target>[^)#]+)(?:#[^)]*)?\)")


def _version() -> str:
    # read, not parsed: tomllib is 3.11+ and this package supports 3.10
    project = (ROOT / "pyproject.toml").read_text()
    match = re.search(r'^version = "(?P<v>[^"]+)"', project, re.MULTILINE)
    assert match, "pyproject has no version"
    return match.group("v")


def test_every_page_is_linked_from_the_index():
    """A page nothing links to is a page nobody reads, and the docs are part of what
    1.0 promises. Specs are design records, not pages, so they are not listed."""
    pages = {p.name for p in DOCS.glob("*.md")} - {"index.md"}
    linked = set(LINK.findall(INDEX.read_text()))
    assert pages - linked == set(), f"not linked from the index: {sorted(pages - linked)}"


@pytest.mark.parametrize("page", sorted(DOCS.rglob("*.md")), ids=lambda p: p.name)
def test_every_relative_link_resolves(page: pathlib.Path):
    """A link to a page that was renamed is worse than no link: it reads as an
    answer that exists somewhere."""
    broken = [
        target
        for target in LINK.findall(page.read_text())
        if not target.startswith(("http://", "https://", "mailto:"))
        and not (page.parent / target).resolve().exists()
    ]
    assert broken == [], f"{page.name} links to nothing: {broken}"


def test_the_current_version_has_an_upgrading_entry():
    """Every release says what it breaks, including the ones that break nothing:
    "nothing breaks" is the sentence a reader is looking for.

    Matched as a whole line: `## 1.0.0` is a substring of `## 1.0.0-rc`, and a check
    that passes on the wrong heading is a check that passes on anything.
    """
    headings = re.findall(r"^## (.+)$", (DOCS / "upgrading.md").read_text(), re.MULTILINE)
    assert _version() in headings, f"no entry for {_version()} among {headings[:3]}"


def test_the_version_is_written_down_once():
    """The manifest is the one copy. `uv version --bump patch` edits it and the
    lockfile in a single command, which only works on a static version, and the
    module asks the installed metadata rather than repeating the number: a second
    copy cannot drift when there is no second copy. textual and litestar do this.
    """
    module = (ROOT / "toro" / "__init__.py").read_text()
    assert re.search(r'^__version__ = "', module, re.MULTILINE) is None, (
        "toro/__init__.py carries a second copy of the version"
    )
    assert 'version("toro-queue")' in module, "the module should derive it, not omit it"


def test_the_module_reports_the_version_the_manifest_declares():
    """The derivation is only as good as what it reads: an install that went stale
    reports the old number from a manifest that says otherwise."""
    import toro

    assert toro.__version__ == _version()


def test_the_readme_and_the_docs_agree_on_what_this_is():
    """The README is the first page anyone reads and the only one that ships to PyPI:
    it points at the docs, and the docs point back."""
    readme = (ROOT / "README.md").read_text()
    assert "docs" in readme
    assert INDEX.read_text().count("README") >= 1


def test_a_1_0_release_does_not_call_itself_alpha():
    """From 1.0 the API is a promise, and PyPI shows the development-status
    classifier beside the version: 1.0.0 went out still marked Alpha."""
    assert int(_version().split(".")[0]) >= 1
    status = re.findall(r'"Development Status :: ([^"]+)"', (ROOT / "pyproject.toml").read_text())
    assert status == ["5 - Production/Stable"], status


def _architecture() -> str:
    return (DOCS / "architecture.md").read_text()


def test_every_lua_script_has_a_row_in_the_architecture_doc():
    """The script table in architecture.md is the only map of what runs inside Redis,
    and it had fallen three scripts behind before this check existed."""
    from toro import scripts

    table = _architecture().split("And the scripts themselves:", 1)[1]
    lua = [
        name
        for name, value in vars(scripts).items()
        if not name.startswith("_") and isinstance(value, str) and "redis.call" in value
    ]
    assert lua, "found no scripts: the check would pass on anything"
    missing = [name for name in lua if f"`{name}`" not in table]
    assert not missing, f"scripts with no row in docs/architecture.md: {missing}"


def test_every_routine_the_architecture_doc_names_exists():
    """A renamed Lua routine left its old name in the routine table."""
    from toro import scripts

    doc = _architecture()
    table = doc.split("The scripts share a small library of routines:", 1)[1].split(
        "And the scripts", 1
    )[0]
    named = [
        name
        for row in re.findall(r"^\| ([^|]+) \|", table, re.MULTILINE)
        for name in re.findall(r"`(\w+)`", row)
    ]
    assert named, "found no routines in the table: the check would pass on anything"
    defined = set(re.findall(r"local function (\w+)", scripts._LIB))
    stale = [name for name in named if name not in defined]
    assert not stale, f"routines named in docs/architecture.md that scripts.py lacks: {stale}"


def test_every_worker_event_is_in_the_lifecycle_table():
    """`worker.on()` accepts any name, so an event missing from the table is one a
    reader cannot know to subscribe to. The table had lost two."""
    source = (ROOT / "toro" / "worker.py").read_text()
    # the event argument of every _emit call, including `"a" if cond else "b"`
    first_args = re.findall(r"_emit\(([^,]+),", source)
    emitted = {name for arg in first_args for name in re.findall(r'"([a-z-]+)"', arg)}
    assert emitted, "found no emitted events: the check would pass on anything"
    section = (
        (DOCS / "processing.md").read_text().split("## Lifecycle events", 1)[1].split("\n## ", 1)[0]
    )
    rows = set(re.findall(r"^\| `([a-z-]+)` \|", section, re.MULTILINE))
    assert emitted <= rows, f"events with no row in processing.md: {sorted(emitted - rows)}"


def test_every_redis_key_is_in_the_data_model_doc():
    """keys.py is where every key name is computed; data-model.md is where a reader
    looks one up. Two keys had reached the first without the second."""
    source = (ROOT / "toro" / "keys.py").read_text()
    suffixes = set(re.findall(r'f"\{self\.base\}([a-z][a-z-]*)', source))
    suffixes |= {f"<jobId>:{s}" for s in re.findall(r'\{job_id\}:([a-z-]+)"', source)}
    assert len(suffixes) > 10, f"found only {sorted(suffixes)}: the check would pass on anything"
    doc = (DOCS / "data-model.md").read_text()
    missing = sorted(s for s in suffixes if f"`{s}" not in doc)
    assert not missing, f"keys with no entry in data-model.md: {missing}"


def test_ci_runs_the_suite_on_the_redis_version_the_docs_name_as_the_floor():
    """The floor is a support claim, and a claim no job runs is a guess: every CI cell
    ran Redis 7 while the docs and the site promised 6.2."""
    floor = re.search(
        r"\*\*Redis (?P<v>[\d.]+) and later\.\*\*", (DOCS / "versioning.md").read_text()
    )
    assert floor, "docs/versioning.md states no Redis floor"
    site = (ROOT / "site" / "web" / "templates" / "index.html").read_text()
    assert f"('Redis', '{floor['v']} and later')" in site
    workflow = (ROOT / ".github" / "workflows" / "pr-check.yaml").read_text()
    assert f"image: redis:{floor['v']}-alpine" in workflow


def test_ci_runs_the_suite_on_the_redis_py_floor_pyproject_declares():
    """The lock pins one redis-py and every matrix cell installs that one, so the
    floor in pyproject.toml was a claim no job checked."""
    floor = re.search(r'"redis>=(?P<v>[\d.]+)"', (ROOT / "pyproject.toml").read_text())
    assert floor, "pyproject.toml declares no redis-py floor"
    workflow = (ROOT / ".github" / "workflows" / "pr-check.yaml").read_text()
    assert f'redis_py: "{floor["v"]}"' in workflow
