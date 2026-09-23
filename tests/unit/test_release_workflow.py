"""Unit: the release workflow's publish step, checked rather than discovered.

This pin broke the 1.0.0 release twice: first as a commit SHA (the action ships as a
Docker image and the runner pulls `ghcr.io/pypa/gh-action-pypi-publish:<ref>`, which
has no manifest for a bare commit), then as a release tag a year old (its bundled
twine rejects the metadata version `uv build` writes). Both failed in CI after a
green build, which is the most expensive place to find out.
"""

import pathlib
import re

WORKFLOW = pathlib.Path(__file__).resolve().parents[2] / ".github/workflows/release.yml"
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


def test_the_build_refuses_a_tag_that_does_not_match_what_it_built():
    """The tag triggers the release and the module supplies the version, and nothing
    made them agree. Tagging `v1.0.3` while the module still says 1.0.2 publishes
    1.0.2 under a 1.0.3 tag, and a PyPI upload cannot be taken back: the wrong file
    is the release from then on. The build job compares the two before anything is
    uploaded, so a mismatch fails while it is still free to fail.
    """
    text = WORKFLOW.read_text()
    assert "github.ref_name" in text, "nothing in the release reads the tag it was given"
    build = text.split("publish:")[0]
    assert "github.ref_name" in build, "the tag is checked after the build, not before"


def test_the_publish_job_asks_for_the_token_it_needs_and_no_more():
    """The workflow's default permissions are the repository's, which is more than a
    release needs; the publish job asks for id-token itself."""
    text = WORKFLOW.read_text()
    assert "permissions:\n  contents: read" in text
    assert "id-token: write" in text


def test_nothing_else_in_the_release_rides_a_branch():
    """Every action in the release path is pinned to something that does not move."""
    floating = [
        ref
        for ref in re.findall(r"uses:\s*\S+@(\S+)", WORKFLOW.read_text())
        if not re.fullmatch(r"v\d+(\.\d+)*", ref) and not re.fullmatch(r"[0-9a-f]{40}", ref)
    ]
    assert floating == [], f"these move under the release: {floating}"
