"""Unit: the release workflow's publish step, checked rather than discovered.

This pin broke the 1.0.0 release twice: first as a commit SHA (the action ships as a
Docker image and the runner pulls `ghcr.io/pypa/gh-action-pypi-publish:<ref>`, which
has no manifest for a bare commit), then as a release tag a year old (its bundled
twine rejects the metadata version `uv build` writes). Both failed in CI after a
green build, which is the most expensive place to find out.
"""

import os
import pathlib
import re
import shutil
import subprocess

import pytest

WORKFLOWS = sorted(
    (pathlib.Path(__file__).resolve().parents[2] / ".github/workflows").glob("*.y*ml")
)
WORKFLOW = next(w for w in WORKFLOWS if w.name == "release.yml")
TAG_CHECK = WORKFLOW.parent.parent / "scripts" / "check-release-tag.sh"
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


def _check_tag(tmp_path: pathlib.Path, ref: str, wheel: str | None) -> subprocess.CompletedProcess:
    """Run the build job's tag check against a dist/ holding `wheel`."""
    dist = tmp_path / "dist"
    dist.mkdir()
    if wheel:
        (dist / wheel).write_bytes(b"")
    sh = shutil.which("sh")
    assert sh, "no sh on PATH"
    # the command is a resolved executable and the repo's own script
    return subprocess.run(  # noqa: S603
        [sh, str(TAG_CHECK)],
        cwd=tmp_path,
        env=os.environ | {"REF": ref},
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_build_refuses_a_tag_that_does_not_match_what_it_built(tmp_path):
    """The tag starts the release and the manifest supplies the version, and nothing
    made them agree. Tagging `v1.0.3` while the manifest still says 1.0.2 publishes
    1.0.2 under a 1.0.3 tag, and a PyPI upload cannot be taken back: the wrong file
    is the release from then on.
    """
    result = _check_tag(tmp_path, "refs/tags/v1.0.3", "toro_queue-1.0.2-py3-none-any.whl")
    assert result.returncode == 1
    assert "tag v1.0.3 does not match the built version 1.0.2" in result.stderr


def test_the_build_accepts_the_tag_of_the_version_it_built(tmp_path):
    result = _check_tag(tmp_path, "refs/tags/v1.0.3", "toro_queue-1.0.3-py3-none-any.whl")
    assert result.returncode == 0, result.stderr


def test_the_build_refuses_a_run_from_a_branch(tmp_path):
    """A manual run from a branch has no tag to compare with; the check used to be
    skipped for it while the publish job still ran."""
    result = _check_tag(tmp_path, "refs/heads/main", "toro_queue-1.0.3-py3-none-any.whl")
    assert result.returncode == 1
    assert "a release runs from a v* tag, not refs/heads/main" in result.stderr


def test_the_build_refuses_when_nothing_was_built(tmp_path):
    result = _check_tag(tmp_path, "refs/tags/v1.0.3", None)
    assert result.returncode == 1
    assert "no wheel in dist/" in result.stderr


def test_the_tag_check_runs_on_every_release_before_publishing():
    """The check is in the build job, which the publish job needs, and nothing in
    the build job is conditional: a skipped check publishes whatever was built."""
    build = _jobs(WORKFLOW)["build"]
    assert TAG_CHECK.name in build, "the build job no longer checks the tag"
    assert "if:" not in build, "a step of the build job can be skipped"


def test_a_release_in_progress_is_never_cancelled():
    text = WORKFLOW.read_text()
    assert "concurrency:" in text
    assert "cancel-in-progress: false" in text


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda w: w.name)
def test_a_job_holding_an_oidc_token_restores_no_cache(workflow: pathlib.Path):
    """A cache is writable by any workflow run on the repository, so whatever a job
    holding id-token restores from one runs with that token."""
    for name, body in _jobs(workflow).items():
        if "id-token: write" not in body:
            continue
        assert "actions/cache" not in body, f"{workflow.name}: {name} restores a cache"
        if "astral-sh/setup-uv@" in body:
            assert "enable-cache: false" in body, f"{workflow.name}: {name} caches uv"


def test_the_github_release_carries_the_files_and_their_provenance():
    """The wheel and sdist go on the GitHub release with the build provenance that
    actions/attest signed for them, as a Sigstore bundle and as in-toto JSON Lines."""
    jobs = _jobs(WORKFLOW)
    assert "actions/attest@" in jobs["build"]
    assert ".sigstore.json" in jobs["build"]
    assert ".intoto.jsonl" in jobs["build"]
    release = jobs["github-release"]
    assert "gh release create" in release
    for files in ("dist/*.whl", "dist/*.tar.gz", "provenance/*"):
        assert files in release


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda w: w.name)
def test_only_the_release_job_can_write_to_the_repository(workflow: pathlib.Path):
    writers = [name for name, body in _jobs(workflow).items() if "contents: write" in body]
    assert writers == (["github-release"] if workflow == WORKFLOW else [])


def test_the_workflows_are_linted_by_something():
    """Everything above is a rule about these files that only this suite enforces,
    and this suite only knows the rules it was told. zizmor knows the ones nobody
    here has thought of yet, so it runs where the other checks run."""
    audited = [w.name for w in WORKFLOWS if "zizmor" in w.read_text()]
    assert audited, "nothing audits the workflows themselves"


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


CI = next(w for w in WORKFLOWS if w.name == "pr-check.yaml")
PYPROJECT = WORKFLOWS[0].parents[2] / "pyproject.toml"


def test_the_ci_job_fails_unless_every_other_job_succeeded():
    """The branch rules require only `CI`, so a job left out of its `needs` can fail
    without blocking a merge."""
    jobs = _jobs(CI)
    needs = re.search(r"needs: \[(.*)\]", jobs["ci"])
    assert needs, "the CI job needs nothing"
    assert sorted(n.strip() for n in needs.group(1).split(",")) == sorted(set(jobs) - {"ci"})
    assert "if: always()" in jobs["ci"], "CI is skipped, not failed, when a job fails"


def test_ci_runs_every_python_the_package_claims():
    """From the `requires-python` floor to the newest classifier, each one tested."""
    text = PYPROJECT.read_text()
    floor = re.search(r'requires-python = ">=(3\.\d+)"', text)
    claimed = re.findall(r'"Programming Language :: Python :: (3\.\d+)"', text)
    matrix = re.search(r"python-version: \[(.*)\]", CI.read_text())
    assert floor, "requires-python has no floor"
    assert matrix, "the CI matrix lists no Python versions"
    tested = [v.strip().strip('"') for v in matrix.group(1).split(",")]
    assert tested == claimed
    assert tested[0] == floor.group(1)
