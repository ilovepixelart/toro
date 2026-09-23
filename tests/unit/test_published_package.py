"""Unit: what a release actually ships.

The wheel is the package; the sdist is what a distro packager, an auditor or anyone
building from source gets. Both are published to PyPI forever, so what goes in them
is a decision rather than whatever happened to be in the working tree: editor
settings and CI wiring are not part of the software.
"""

import pathlib
import shutil
import subprocess
import tarfile
import tempfile

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
# Not shipped: they describe how this repository is worked on, not how the package is
# built, installed or verified. (`.gitignore` is not on this list: hatchling ships it
# on purpose, because it is how the sdist reproduces its own file selection.)
NOT_SHIPPED = (".github", ".vscode", ".pre-commit-config.yaml")
# Shipped: the package, and enough to build it and check it for yourself.
SHIPPED = ("toro", "tests", "pyproject.toml", "README.md", "LICENSE")


@pytest.fixture(scope="module")
def sdist_entries() -> set[str]:
    """Build the real sdist and read what is in it. Nothing here comes from the working
    tree: the question is what the build backend put in the archive."""
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv builds the sdist")
    with tempfile.TemporaryDirectory() as out:
        # the command is a fixed list of literals and a resolved executable path
        subprocess.run(  # noqa: S603
            [uv, "build", "--sdist", "--out-dir", out], cwd=ROOT, check=True, capture_output=True
        )
        archive = next(pathlib.Path(out).glob("*.tar.gz"))
        with tarfile.open(archive) as tar:
            # names are "<name>-<version>/<path>"; the first segment is the root
            return {name.split("/")[1] for name in tar.getnames() if "/" in name}


@pytest.mark.parametrize("entry", NOT_SHIPPED)
def test_the_sdist_leaves_the_workshop_behind(entry: str, sdist_entries: set[str]):
    assert entry not in sdist_entries


@pytest.mark.parametrize("entry", SHIPPED)
def test_the_sdist_carries_what_it_should(entry: str, sdist_entries: set[str]):
    assert entry in sdist_entries
