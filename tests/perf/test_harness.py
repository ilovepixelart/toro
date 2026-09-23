"""The perf matrix's own guard (docs/specs/sync-and-scale.md).

The matrix is a measuring tool, so it rots the way tools do: an API it calls moves,
nobody runs it for a month, and the baselines are then measurements of nothing. This
runs the smallest possible cell and asserts it produced numbers. It asserts nothing
about the numbers themselves: a test that fails because a laptop was busy teaches
everyone to ignore it.
"""

import asyncio

from .harness import run_cell


async def test_a_cell_measures_something(q):
    """`q` is here for its cleanup, not its queue: the harness owns its own."""
    cell = await asyncio.to_thread(
        run_cell, loop_factory=asyncio.new_event_loop, eager=False, pipelined=True
    )

    assert cell["enqueue_per_s"] > 0
    assert cell["process_per_s"] > 0
