from runtime_cache import BoundedTTLCache, BoundedTTLSet


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_cache_active_hit_expiry_and_stale_pruning():
    clock = Clock()
    cache = BoundedTTLCache(ttl_seconds=10, max_entries=4, clock=clock)
    cache["place"] = "weather"
    assert cache.get("place") == "weather"
    clock.now = 9.9
    assert cache.get("place") == "weather"
    clock.now = 10
    assert cache.get("place") is None
    assert len(cache) == 0


def test_cache_maximum_evicts_oldest_deterministically():
    clock = Clock()
    cache = BoundedTTLCache(ttl_seconds=100, max_entries=2, clock=clock)
    cache["oldest"] = 1
    clock.now += 1
    cache["middle"] = 2
    clock.now += 1
    cache["newest"] = 3
    assert list(cache) == ["middle", "newest"]
    assert cache.get("oldest") is None


def test_cache_update_refreshes_age_and_pop_release_works():
    clock = Clock()
    cache = BoundedTTLCache(ttl_seconds=10, max_entries=2, clock=clock)
    cache["user"] = "first"
    clock.now = 8
    cache["user"] = "refreshed"
    clock.now = 12
    assert cache["user"] == "refreshed"
    assert cache.pop("user") == "refreshed"
    assert "user" not in cache


def test_bounded_ttl_set_preserves_recent_ids_only():
    clock = Clock()
    seen = BoundedTTLSet(ttl_seconds=5, max_entries=2, clock=clock)
    seen.add(1)
    clock.now += 1
    seen.add(2)
    clock.now += 1
    seen.add(3)
    assert 1 not in seen
    assert 2 in seen and 3 in seen
    clock.now = 7
    assert len(seen) == 0


def test_production_cache_policies_are_explicit_and_bounded():
    policies = {
        "tedtalk": (2 * 3600, 128),
        "weather": (3600, 256),
        "voice": (30 * 86400, 2048),
        "presence": (6 * 3600, 4096),
        "hostage": (24 * 3600, 512),
        "processed": (3600, 500),
    }
    for ttl, maximum in policies.values():
        cache = BoundedTTLCache(ttl_seconds=ttl, max_entries=maximum)
        assert cache.ttl_seconds == ttl
        assert cache.max_entries == maximum
