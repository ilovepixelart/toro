"""Unit: the release workflow's publish step, checked rather than discovered.

This pin broke the 1.0.0 release twice: first as a commit SHA (the action ships as a
Docker image and the runner pulls `ghcr.io/pypa/gh-action-pypi-publish:<ref>`, which
has no manifest for a bare commit), then as a release tag a year old (its bundled
twine rejects the metadata version `uv build` writes). Both failed in CI after a
green build, which is the most expensive place to find out.
"""

import pathlib
import re

import pytest

WORKFLOWS = sorted(
    (pathlib.Path(__file__).resolve().parents[2] / ".github/workflows").glob("*.y*ml")
)
WORKFLOW = next(w for w in WORKFLOWS if w.name == "release.yml")
# The one action that cannot be pinned to a commit. It is a composite action that
# generates a Docker action at run time, and `create-docker-action.py` builds the
# image reference as `ghcr.io/<repo>:<the ref you called it with>`. No image is
# published under a bare commit, so a hash pin asks for a tag that never existed
# and the runner reports "manifest unknown". Verified the hard way, twice.
DOCKER_ACTION = "pypa/gh-action-pypi-publish"
PUBLISH = re.compile(r"uses:\s*pypa/gh-action-pypi-publish@(?P<ref>\S+)")


def _pin() -> str:
    match = PUBLISH.search(WORKFLOW.read_text())
    assert match, "the workflow no longer uses the publishing action"
    return match.group("ref")


def test_the_publish_action_is_pinned_to_a_release():
    """Not a branch: the job holds the OIDC token that uploads to PyPI, and anyone
    who can move `release/v1` can run code with it. Not a commit either: a Docker
    action has no image under a bare SHA."""
    assert re.fullmatch(r"v\d+\.\d+\.\d+", _pin()), (
        f"pin the publishing action to a release tag, not {_pin()!r}"
    )


def _jobs(workflow: pathlib.Path) -> dict[str, str]:
    """Split a workflow into its jobs. Regex rather than a YAML parser because that
    is one more dependency than this check is worth, and the indentation here is not
    in question."""
    text = workflow.read_text()
    body = text[text.index("\njobs:") :]
    chunks = re.split(r"\n  (?=[\w-]+:\n)", body)
    return {chunk.split(":", 1)[0].strip(): chunk for chunk in chunks[1:]}


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda w: w.name)
def test_every_job_gives_up_eventually(workflow: pathlib.Path):
    """A hosted job runs for six hours before GitHub stops it, and these finish in
    about three minutes. A wedged service container or a deadlocked test would
    otherwise hold a runner all afternoon, once per matrix cell."""
    jobs = _jobs(workflow)
    assert jobs, f"found no jobs in {workflow.name}"
    forever = [name for name, body in jobs.items() if "timeout-minutes:" not in body]
    assert forever == [], f"these run until GitHub stops them: {forever}"


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda w: w.name)
def test_no_checkout_leaves_its_credentials_in_the_workspace(workflow: pathlib.Path):
    """actions/checkout writes a token into .git/config and leaves it there for the
    rest of the job, where anything that packages the directory can carry it out."""
    text = workflow.read_text()
    checkouts = text.count("uses: actions/checkout@")
    assert checkouts == text.count("persist-credentials: false"), (
        f"{workflow.name}: a checkout keeps its credentials"
    )


def test_something_keeps_the_pinned_commits_current():
    """A commit pin freezes the action at that commit, including its unfixed bugs and
    its unfixed vulnerabilities. Pinning without anything to raise the pins trades a
    moving reference for a stale one, so the two belong together: Dependabot rewrites
    the hash and the version comment beside it.
    """
    config = WORKFLOWS[0].parent.parent / "dependabot.yml"
    assert config.exists(), "nothing raises the commit pins this repo now requires"
    text = config.read_text()
    assert "github-actions" in text
    assert "interval" in text


def test_the_publish_job_asks_for_the_token_it_needs_and_no_more():
    """The workflow's default permissions are the repository's, which is more than a
    release needs; the publish job asks for id-token itself."""
    text = WORKFLOW.read_text()
    assert "permissions:\n  contents: read" in text
    assert "id-token: write" in text


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda w: w.name)
def test_every_action_is_pinned_to_a_commit(workflow: pathlib.Path):
    """A tag is not an immutable reference: whoever owns the action can move it, and
    GitHub's own guidance is that a full commit is the only way to pin one. Our
    release job hands an OIDC token that can publish to PyPI to an action we do not
    own, so this is the job where a moved tag would cost the most.

    `DOCKER_ACTION` is the documented exception and is checked separately.
    """
    unpinned = [
        f"{action}@{ref}"
        for action, ref in re.findall(r"uses:\s*(\S+)@(\S+)", workflow.read_text())
        if action != DOCKER_ACTION and not re.fullmatch(r"[0-9a-f]{40}", ref)
    ]
    assert unpinned == [], f"pin these to a commit: {unpinned}"


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda w: w.name)
def test_a_pinned_commit_still_says_which_release_it_is(workflow: pathlib.Path):
    """A bare hash tells a reader nothing and tells Dependabot nothing either: it
    matches the trailing comment when it raises the pin, so the comment is part of
    the mechanism rather than decoration."""
    bare = [
        line.strip()
        for line in workflow.read_text().splitlines()
        if re.search(r"uses:\s*\S+@[0-9a-f]{40}\s*$", line)
    ]
    assert bare == [], f"say which release these are, as `# vX.Y.Z`: {bare}"
