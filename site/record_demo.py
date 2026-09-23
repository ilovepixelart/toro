"""Record a real run of the demo queue so the static page can replay it.

The static build has no Redis, so its panel cannot move. Rather than simulate a
queue in JavaScript, this drives the same Demo the live site uses, samples the
real counters twice a second, kills a worker partway through and keeps sampling
while toro's stalled sweep recovers its jobs. The result is a timeline of
numbers that actually happened, which the page replays and labels as a
recording.

    uv run python record_demo.py        # writes web/static/replay.json
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import time
from typing import Any

from web.app import Demo

OUT = pathlib.Path(__file__).parent / "web" / "static" / "replay.json"
SAMPLE_SECONDS = 0.5
WARMUP_SECONDS = 12.0
AFTER_KILL_SECONDS = 18.0
AFTER_REVIVE_SECONDS = 8.0


async def main() -> None:
    demo = Demo()
    await demo.start()
    frames: list[dict[str, Any]] = []
    started = time.monotonic()

    async def sample(note: str | None = None) -> None:
        state = await demo.snapshot()
        frames.append(
            {
                "t": round(time.monotonic() - started, 2),
                "counts": {
                    k: state["counts"].get(k, 0) for k in ("wait", "active", "completed", "failed")
                },
                "workers": state["workers"],
                "totals": {k: state["total"].get(k, 0) for k in ("completed", "failed")},
                "note": note,
            }
        )

    async def run_for(seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            await sample()
            await asyncio.sleep(SAMPLE_SECONDS)

    try:
        await run_for(WARMUP_SECONDS)
        held = await demo.crash("worker-1")
        await sample(f"worker-1 died holding {held} job{'s' if held != 1 else ''}")
        await run_for(AFTER_KILL_SECONDS)
        await demo.revive("worker-1")
        await sample("worker-1 restarted")
        await run_for(AFTER_REVIVE_SECONDS)
    finally:
        await demo.stop()

    OUT.write_text(json.dumps({"sample_seconds": SAMPLE_SECONDS, "frames": frames}))
    peak = max(f["counts"]["active"] for f in frames)
    print(
        f"recorded {len(frames)} frames over {frames[-1]['t']:.0f}s "
        f"({frames[-1]['totals']['completed']} jobs completed, peak {peak} active) -> {OUT.name}"
    )


asyncio.run(main())
