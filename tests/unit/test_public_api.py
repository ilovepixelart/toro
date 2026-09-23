"""Unit: the public API is a contract (docs/specs/one-point-oh.md).

What `import toro` exports is what 1.0 promises; everything else is internal and may
change in a patch release. Both halves of that need a check: a name that appears
without anyone deciding to promise it, and a name that disappears from under someone
who relied on it.
"""

import importlib
import inspect
import pathlib

import pytest

import toro

# The frozen list. Adding to it is a minor release, removing from it a major one, and
# either way this file is the diff that says so.
PUBLIC = {
    "Backoff",
    "BackoffOpts",
    "Deduplication",
    "FlowChild",
    "FlowMetricsPoint",
    "FlowView",
    "Job",
    "JobCancelledError",
    "JobFailedError",
    "JobOptions",
    "JobState",
    "MetricsPoint",
    "NameMetrics",
    "OnFail",
    "PartialFlushError",
    "PendingJobs",
    "Queue",
    "RateLimit",
    "RemoveOption",
    "ToroError",
    "Worker",
    "render_all",
}

DOCS = pathlib.Path(__file__).resolve().parents[2] / "docs"
README = DOCS.parent / "README.md"


def _documentation() -> str:
    """The pages a user reads. Not `docs/specs/`: a spec is a design record, and one
    that names the thing it is about would otherwise satisfy this check on its own.
    """
    pages = [page.read_text() for page in DOCS.glob("*.md")]
    return "\n".join([*pages, README.read_text()])


def test_the_public_api_is_the_one_that_was_frozen():
    """ON-001. A name added to `__all__` without being added here is a promise nobody
    made; a name removed from `__all__` is one somebody broke."""
    assert set(toro.__all__) == PUBLIC


def test_everything_promised_is_importable():
    for name in PUBLIC:
        assert getattr(toro, name, None) is not None, f"{name} is exported but absent"


def test_nothing_public_leaks_a_private_name():
    """A public name whose own members are underscored is fine; a public name that IS
    underscored means the export list and the intent disagree."""
    assert not [name for name in toro.__all__ if name.startswith("_")]


@pytest.mark.parametrize("name", sorted(PUBLIC))
def test_every_public_name_is_documented(name):
    """ON-002. A promise nobody wrote down is a promise nobody can keep."""
    assert name in _documentation(), f"{name} is public and appears in no page"


@pytest.mark.parametrize("cls", [toro.Queue, toro.Worker])
def test_every_constructor_option_is_documented(cls):
    """ON-002. The options are the surface people actually configure, and one of them
    was missing from every page for eleven releases."""
    docs = _documentation()
    options = list(inspect.signature(cls.__init__).parameters)[1:]
    assert [option for option in options if option not in docs] == []


@pytest.mark.parametrize("cls", [toro.Queue, toro.Worker, toro.Job])
def test_every_public_method_is_documented(cls):
    """ON-002. A method with no underscore is callable by anyone reading the object,
    so it is either documented or it should not be public."""
    docs = _documentation()
    methods = [
        name for name, _ in inspect.getmembers(cls, inspect.isfunction) if not name.startswith("_")
    ]
    assert [method for method in methods if method not in docs] == []


def test_the_internals_are_not_advertised():
    """The module paths are internal, whatever they look like: nothing in `__all__`
    is a module, so `toro.scripts` and friends stay changeable."""
    for name in toro.__all__:
        assert not inspect.ismodule(getattr(toro, name))


def test_the_version_is_where_packaging_expects_it():
    assert importlib.metadata.version("toro-queue") == toro.__version__
