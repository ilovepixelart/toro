"""Unit: Keys - the one place that knows the Redis key layout."""

import pytest

from toro.keys import Keys


def test_collection_keys_are_namespaced():
    k = Keys("emails", "toro")
    assert k.base == "toro:emails:"
    assert k.id == "toro:emails:id"
    assert k.prioritized == "toro:emails:prioritized"
    assert k.marker == "toro:emails:marker"
    assert k.delayed == "toro:emails:delayed"
    assert k.limiter == "toro:emails:limiter"


def test_per_job_keys():
    k = Keys("emails", "toro")
    assert k.job(5) == "toro:emails:5"
    assert k.lock(5) == "toro:emails:5:lock"
    assert k.logs(5) == "toro:emails:5:logs"
    # custom (string) ids slot in cleanly too
    assert k.job("order-7") == "toro:emails:order-7"


def test_flow_keys():
    k = Keys("emails", "toro")
    assert k.waiting_children == "toro:emails:waiting-children"
    assert k.deps(5) == "toro:emails:5:deps"
    assert k.results(5) == "toro:emails:5:results"
    assert k.cfail(5) == "toro:emails:5:cfail"


def test_roots_index_keys():
    # the children index + its diff scratch, behind the root-first listing
    k = Keys("emails", "toro")
    assert k.children == "toro:emails:children"
    assert k.roots_scratch == "toro:emails:roots-scratch"


def test_scheduler_key():
    assert Keys("emails", "toro").scheduler("nightly") == "toro:emails:repeat:nightly"


def test_prefix_is_configurable():
    assert Keys("q", "myapp").base == "myapp:q:"


@pytest.mark.parametrize("name", ["orders:eu", "", "x" * 300, "a\nb", "a b" * 100])
def test_a_queue_name_that_could_collide_is_refused(name):
    """`base = f"{prefix}:{name}:"`, so a colon in the NAME makes two different
    (prefix, name) pairs the same namespace: queue `orders`' job `5:x` and queue
    `orders:5`' job `x` shared a lock key, and one queue's worker could expire the
    other's lock. With no colon in a name the decomposition is unique.
    """
    with pytest.raises(ValueError, match="queue name"):
        Keys(name, "toro")


def test_a_prefix_may_still_namespace_with_colons():
    """A prefix is the operator's own namespace and often contains one: `app:toro`.
    It cannot collide with anything once names have none."""
    assert Keys("orders", "app:toro").base == "app:toro:orders:"


@pytest.mark.parametrize("prefix", ["", "a\nb"])
def test_a_prefix_still_has_to_be_a_key_segment(prefix):
    with pytest.raises(ValueError, match="prefix"):
        Keys("orders", prefix)
