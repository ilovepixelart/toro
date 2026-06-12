"""FlowChild: the declarative node of a flow tree.

A flow is a parent job enqueued atomically with its children; children run
first and the parent is parked in `waiting-children` until they settle. The
full design (and the landscape research behind it) is docs/flows-design.md.
"""

from __future__ import annotations

import json
from typing import Any, Literal

from . import scripts
from .job import JobOptions

OnFail = Literal["fail_parent", "continue"]

# Whole-tree cap: ADD_FLOW inserts the tree in one atomic script, so its size
# bounds how long that script can hold Redis (same idea as PROMOTE_BATCH).
MAX_FLOW_NODES = 1000


class FlowChild:
    """One node of a flow tree.

    Takes a job name + data, the same options as ``Queue.add()``, optional
    nested children, and `on_fail` - what this child's terminal failure does
    to its parent:

    * ``"fail_parent"`` (default): the parent fails immediately, recursively up
      ancestors that also default. No flow is ever parked forever on a failure.
    * ``"continue"``: the failure is recorded; the parent still runs once every
      child has settled and can inspect ``job.failed_children()``.
    """

    __slots__ = ("children", "data", "name", "on_fail", "opts")

    def __init__(
        self,
        name: str,
        data: Any = None,
        *,
        children: list[FlowChild] | None = None,
        on_fail: OnFail = "fail_parent",
        **opts: Any,
    ) -> None:
        if not name or not isinstance(name, str):
            raise ValueError("a flow node needs a non-empty job name")
        if on_fail not in ("fail_parent", "continue"):
            raise ValueError("on_fail must be 'fail_parent' or 'continue'")
        if "job_id" in opts:
            raise ValueError("flow nodes use server-generated ids; custom job_id is not supported")
        if "deduplication" in opts:
            raise ValueError("deduplication is not supported on flow nodes")
        self.children = list(children or [])
        try:
            # fail typos at construction, where the node is in hand (queue
            # defaults merge later - node_options re-validates after the merge)
            local_options = JobOptions(**opts)
        except TypeError as exc:
            raise ValueError(f"flow node {name!r}: {exc}") from None
        if self.children and local_options.delay > 0:
            raise ValueError(
                "a node with children can't be delayed - it runs when its children settle"
            )
        self.name = name
        self.data = data
        self.on_fail = on_fail
        self.opts = dict(opts)


def count_nodes(node: FlowChild) -> int:
    """Size of the whole tree (the MAX_FLOW_NODES guard)."""
    return 1 + sum(count_nodes(child) for child in node.children)


def clamp_priority(p: int) -> int:
    """Clamp a priority into the packable range (shared with Queue.add)."""
    return max(0, min(int(p), scripts.PRIORITY_OFFSET))


def node_options(node: FlowChild, defaults: dict[str, Any]) -> JobOptions:
    """Merge queue defaults under a node's opts into validated JobOptions.

    Errors name the node - in a tree of up to MAX_FLOW_NODES, "which node?"
    is the question. The delay invariant re-checks here because a delay can
    arrive via `defaults`, which FlowChild's constructor never sees.
    """
    try:
        options = JobOptions(**{**defaults, **node.opts})
    except TypeError as exc:
        raise ValueError(f"flow node {node.name!r}: {exc}") from None
    options.priority = clamp_priority(options.priority)
    if node.children and options.delay > 0:
        raise ValueError(
            f"flow node {node.name!r}: a node with children can't be delayed "
            "- it runs when its children settle (check default_job_options)"
        )
    return options


def to_tree(node: FlowChild, defaults: dict[str, Any]) -> dict[str, Any]:
    """Serialize a node (recursively) for the ADD_FLOW script. `data` and
    `opts` are pre-encoded as JSON strings so the script stores them verbatim -
    a cjson round trip could mangle edge cases like `[]` vs `{}`.
    """
    options = node_options(node, defaults)
    payload: dict[str, Any] = {
        "name": node.name,
        "data": json.dumps(node.data),
        "opts": json.dumps(options.to_dict()),
        "delay": options.delay,
        "priority": options.priority,
        "onFail": node.on_fail,
    }
    if node.children:
        payload["children"] = [to_tree(child, defaults) for child in node.children]
    return payload
