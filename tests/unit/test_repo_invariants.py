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
