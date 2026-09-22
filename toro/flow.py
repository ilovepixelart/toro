"""FlowChild: the declarative node of a flow tree.

A flow is a parent job enqueued atomically with its children; children run
first and the parent is parked in `waiting-children` until they settle. The
full design (and the landscape research behind it) is docs/flows-design.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal

from . import scripts
from .job import FINISHED_STATES, JobOptions

OnFail = Literal["fail_parent", "continue"]

# A flow node is terminal once it has settled; anything else is still moving.
_TERMINAL = FINISHED_STATES

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
        "concurrencyKey": options.concurrency_key or "",
    }
    if node.children:
        payload["children"] = [to_tree(child, defaults) for child in node.children]
    return payload


@dataclass(frozen=True, slots=True)
class FlowView:
    """A whole flow projected for a dashboard in one read.

    `tree` is the `{job, children}` shape `get_flow` returns; `results` and
    `failures` and `cancellations` are the parent's collected child return values and
    its tolerated (`on_fail="continue"`) failures and cancellations, kept apart
    because a job stopped on purpose did not fail. The counts are derived over the root's
    direct children - completions only, so a failed flow never reads as done -
    and `live` is true while ANY node in the subtree is still non-terminal.
    Retention can take a finished child's hash, and with it the child's node in
    `tree`, while the flow is still running; its outcome was copied into the parent
    when it settled, so the counts read both and never go backwards.
    Built by `Queue.flow_view()`, which reads it all in O(depth) round trips.
    """

    tree: dict[str, Any]
    results: dict[str, Any]
    failures: dict[str, str]
    cancellations: dict[str, str] = field(default_factory=dict)

    @property
    def total(self) -> int:
        # the root's declared child list, robust to a child hash vanishing mid-walk
        return len(self.tree["job"].children_ids or [])

    @property
    def done(self) -> int:
        return len(self._children_in("completed") | self.results.keys())

    @property
    def failed(self) -> int:
        return len(self._children_in("failed") | self.failures.keys())

    @property
    def cancelled(self) -> int:
        return len(self._children_in("cancelled") | self.cancellations.keys())

    def _children_in(self, state: str) -> set[str]:
        return {n["job"].id for n in self.tree["children"] if n["job"].state == state}

    @property
    def live(self) -> bool:
        def moving(node: dict[str, Any]) -> bool:
            return node["job"].state not in _TERMINAL or any(
                moving(child) for child in node["children"]
            )

        return moving(self.tree)
