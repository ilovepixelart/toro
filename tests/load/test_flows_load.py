"""@load flows under volume - many fan-out/fan-in flows and one deep chain,
end to end through real workers.

We assert behaviour, not a vanity number:
  * every flow completes (no parent parked forever, no lost children),
  * every parent aggregates exactly its own children's results (no cross-flow bleed),
  * a deep chain releases level by level all the way to the root.
"""

from toro import FlowChild as c  # noqa: N813 - `c("part", ...)` keeps trees readable


async def _count(q, state):
    return (await q.counts())[state]


def _count_is(q, state, n):
    """A run_until predicate: true once the state's count reaches exactly n."""

    async def check():
        return (await q.counts())[state] == n

    return check


async def test_fanout_fanin_flows_at_volume(q, run_worker, run_until, load_scale):
    flows = int(20 * load_scale)
    width = 8  # children per flow

    async def proc(job):
        if job.name == "part":
            return job.data["flow"] * 1000 + job.data["i"]
        results = await job.children_results()
        return sorted(results.values())

    async with run_worker(q, proc, concurrency=32):
        parents = [
            await q.add_flow(
                "sum", {"flow": f}, children=[c("part", {"flow": f, "i": i}) for i in range(width)]
            )
            for f in range(flows)
        ]
        total = flows * (width + 1)
        assert await run_until(_count_is(q, "completed", total), timeout=60.0), (
            "flows did not drain - a parent is parked or a child was lost"
        )

    # No cross-flow bleed: each parent saw exactly its own children's values.
    for f, parent in enumerate(parents):
        done = await q.get_job(parent.id)
        assert done.returnvalue == [f * 1000 + i for i in range(width)]
    assert await _count(q, "failed") == 0
    assert await _count(q, "waiting-children") == 0


async def test_deep_chain_releases_to_the_root(q, run_worker, run_until, load_scale):
    depth = int(30 * load_scale)

    tree = c("step", {"lvl": 0})
    for lvl in range(1, depth):
        tree = c("step", {"lvl": lvl}, children=[tree])

    async def proc(job):
        results = await job.children_results()
        below = next(iter(results.values()), 0)
        return below + 1

    async with run_worker(q, proc, concurrency=4):
        root = await q.add_flow("chain-root", {}, children=[tree])
        assert await run_until(_count_is(q, "completed", depth + 1), timeout=60.0), (
            "the chain stalled before reaching the root"
        )
        # depth+1 jobs each add 1 (the leaf returns 1): a full count proves
        # every level released in order, all the way to the root
        assert await root.result(timeout=10) == depth + 1
