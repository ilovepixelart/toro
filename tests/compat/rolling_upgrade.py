"""Two toro versions, one Redis, one queue (docs/specs/one-point-oh.md, ON-010).

A rolling upgrade is the normal way a queue meets two versions of its own library, so
it is the case that has to be proved rather than assumed. This runs the previous
minor from PyPI and the working tree side by side, in both orders, and checks that
every job is processed exactly once and that neither side refuses the other.

    uv run python tests/compat/rolling_upgrade.py             # against the default
    uv run python tests/compat/rolling_upgrade.py 0.11.0      # against a given version

It builds its own throwaway venv for the published version, so it needs network the
first time and nothing afterwards.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import tempfile

PREVIOUS = sys.argv[1] if len(sys.argv) > 1 else "0.11.0"
URL = "redis://localhost:6379"
PREFIX = "rolling"
QUEUE = "upgrade"

# Each side is a whole process: two libraries cannot share one interpreter, which is
# the same reason this cannot be an ordinary test.
SIDE = """
import asyncio, json, sys
from toro import Queue, Worker

URL, PREFIX, QUEUE = sys.argv[1], sys.argv[2], sys.argv[3]
TAG, COUNT = sys.argv[4], int(sys.argv[5])


async def main() -> None:
    import toro

    q = Queue(QUEUE, url=URL, prefix=PREFIX)
    done: list[str] = []

    async def proc(job):
        done.append(job.name)
        return {"by": TAG}

    worker = Worker(QUEUE, proc, url=URL, prefix=PREFIX, stalled_interval=0)
    running = asyncio.create_task(worker.run())
    for i in range(COUNT):
        await q.add(f"{TAG}-{i}", {"i": i})
    for _ in range(600):
        if len(done) >= COUNT:  # its own jobs, or the other side's
            break
        await asyncio.sleep(0.05)
    await worker.stop(grace_period=2)
    running.cancel()
    marker = await q.redis.hget(q.keys.meta, "model") if hasattr(q.keys, "meta") else None
    print(json.dumps({
        "tag": TAG,
        "version": toro.__version__,
        "code": toro.__file__,
        "done": done,
        "model": marker,
    }))
    await q.close()


asyncio.run(main())
"""


def _venv(version: str) -> pathlib.Path:
    """A throwaway environment holding the published version."""
    home = pathlib.Path(tempfile.gettempdir()) / f"toro-compat-{version}"
    if not (home / "bin" / "python").exists():
        subprocess.run(["uv", "venv", str(home), "--quiet"], check=True)
        subprocess.run(
            [
                "uv",
                "pip",
                "install",
                "--quiet",
                "--python",
                str(home / "bin" / "python"),
                f"toro-queue=={version}",
            ],
            check=True,
        )
    return home / "bin" / "python"


def _run(python: str | pathlib.Path, tag: str, count: int) -> subprocess.Popen:
    # Not in the repo: `python -c` puts the working directory first on sys.path, so a
    # child started here would import the working tree whatever venv it was given,
    # and both sides of the comparison would be the same code.
    return subprocess.Popen(
        [str(python), "-c", SIDE, URL, PREFIX, QUEUE, tag, str(count)],
        stdout=subprocess.PIPE,
        text=True,
        cwd=tempfile.gettempdir(),
    )


def _clear() -> None:
    subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import redis,sys; r=redis.from_url(sys.argv[1]); "
                "keys=r.keys(sys.argv[2] + ':*'); r.delete(*keys) if keys else None"
            ),
            URL,
            PREFIX,
        ],
        check=True,
    )


def _round(first: tuple[str, str], second: tuple[str, str], count: int) -> dict:
    """One direction: `first` starts, `second` joins while it is still running."""
    _clear()
    a = _run(first[0], first[1], count)
    b = _run(second[0], second[1], count)
    out = [json.loads(p.communicate()[0].strip().splitlines()[-1]) for p in (a, b)]
    processed = sorted(name for side in out for name in side["done"])
    expected = sorted(
        [f"{first[1]}-{i}" for i in range(count)] + [f"{second[1]}-{i}" for i in range(count)]
    )
    return {
        "versions": [side["version"] for side in out],
        "code": [side["code"] for side in out],
        "processed_once": processed == expected,
        "missing": sorted(set(expected) - set(processed)),
        "twice": sorted({name for name in processed if processed.count(name) > 1}),
        "model": [side["model"] for side in out],
    }


def main() -> int:
    published, working = _venv(PREVIOUS), sys.executable
    count = 25

    print(f"old first ({PREVIOUS} stamps nothing, then the working tree joins):")
    old_first = _round((published, "old"), (working, "new"), count)
    print(" ", old_first)

    print("new first (the working tree stamps the model, then the old one joins):")
    new_first = _round((working, "new"), (published, "old"), count)
    print(" ", new_first)

    ok = old_first["processed_once"] and new_first["processed_once"]
    print("rolling upgrade holds" if ok else "FAILED: see above")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
