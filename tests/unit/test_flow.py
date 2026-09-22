"""Unit: FlowChild - the declarative flow-tree node - and FlowView's counts (no I/O)."""

import pytest

from toro import FlowChild, Job
from toro.flow import MAX_FLOW_NODES, FlowView, count_nodes


def test_defaults():
    node = FlowChild("fetch", {"part": 1})
    assert node.name == "fetch"
    assert node.data == {"part": 1}
    assert node.children == []
    assert node.on_fail == "fail_parent"
    assert node.opts == {}


def test_options_pass_through_like_add():
    node = FlowChild("fetch", {}, priority=5, attempts=3, backoff=100)
    assert node.opts == {"priority": 5, "attempts": 3, "backoff": 100}


def test_rejects_unknown_on_fail():
    with pytest.raises(ValueError):
        FlowChild("fetch", {}, on_fail="explode")


def test_rejects_empty_name():
    with pytest.raises(ValueError):
        FlowChild("", {})


def test_rejects_flow_incompatible_options():
    with pytest.raises(ValueError):
        FlowChild("fetch", {}, job_id="custom")  # server-side ids only
    with pytest.raises(ValueError):
        FlowChild("fetch", {}, deduplication={"id": "x", "ttl": 100})
    with pytest.raises(ValueError):  # interior nodes can't be delayed
        FlowChild("mid", {}, delay=100, children=[FlowChild("leaf", {})])


def test_leaf_delay_is_allowed():
    assert FlowChild("fetch", {}, delay=100).opts["delay"] == 100


def test_unknown_option_fails_at_construction_naming_the_node():
    with pytest.raises(ValueError, match="resize"):  # a typo'd option, caught early
        FlowChild("resize", {}, attemps=3)


def test_count_nodes_counts_the_whole_tree():
    tree = FlowChild("mid", {}, children=[FlowChild("a", {}), FlowChild("b", {})])
    assert count_nodes(tree) == 3
    assert MAX_FLOW_NODES >= 1000


def _view(children: dict[str, str], results: dict, failures: dict) -> FlowView:
    root = Job("root", "report", {}, state="waiting-children", children_ids=["a", "b", "c", "d"])
    nodes = [{"job": Job(i, "part", {}, state=st), "children": []} for i, st in children.items()]
    return FlowView({"job": root, "children": nodes}, results, failures)


def test_flow_view_counts_read_the_tree_and_the_parents_copies():
    """The tree and the parent's copies are separate reads, and retention takes nodes
    out of the tree: either can hold an outcome the other lacks, and one that both
    hold is counted once."""
    view = _view(
        {"a": "completed", "b": "failed", "c": "active"},  # d's hash was trimmed
        results={"d": 1},  # a finished after this was read
        failures={"b": "boom"},  # tolerated, so it is in both
    )
    assert (view.total, view.done, view.failed) == (4, 2, 1)


def test_flow_view_counts_a_fail_parent_failure_from_the_tree_alone():
    # only tolerated failures are copied into the parent
    view = _view({"a": "failed"}, results={}, failures={})
    assert (view.done, view.failed) == (0, 1)
