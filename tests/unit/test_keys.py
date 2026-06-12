"""Unit: Keys - the one place that knows the Redis key layout."""

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


def test_scheduler_key():
    assert Keys("emails", "toro").scheduler("nightly") == "toro:emails:repeat:nightly"


def test_prefix_is_configurable():
    assert Keys("q", "myapp").base == "myapp:q:"
