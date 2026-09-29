"""Dependency-free bounded TTL collections for genuinely ephemeral state."""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import MutableMapping, MutableSet
import time


class BoundedTTLCache(MutableMapping):
    def __init__(self, *, ttl_seconds: float, max_entries: int, clock=time.monotonic):
        self.ttl_seconds = max(1.0, float(ttl_seconds))
        self.max_entries = max(1, int(max_entries))
        self.clock = clock
        self._items = OrderedDict()

    def prune(self, *, now=None) -> int:
        now = self.clock() if now is None else now
        removed = 0
        for key, (created, _) in list(self._items.items()):
            if now - created >= self.ttl_seconds:
                self._items.pop(key, None)
                removed += 1
        while len(self._items) > self.max_entries:
            self._items.popitem(last=False)
            removed += 1
        return removed

    def __getitem__(self, key):
        self.prune()
        created, value = self._items[key]
        return value

    def __setitem__(self, key, value):
        self.prune()
        self._items.pop(key, None)
        self._items[key] = (self.clock(), value)
        self.prune()

    def __delitem__(self, key):
        del self._items[key]

    def __iter__(self):
        self.prune()
        return iter(tuple(self._items))

    def __len__(self):
        self.prune()
        return len(self._items)

    def clear(self):
        self._items.clear()

    def pop(self, key, default=None):
        self.prune()
        item = self._items.pop(key, None)
        return default if item is None else item[1]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


class BoundedTTLSet(MutableSet):
    def __init__(self, *, ttl_seconds: float, max_entries: int, clock=time.monotonic):
        self._cache = BoundedTTLCache(
            ttl_seconds=ttl_seconds, max_entries=max_entries, clock=clock,
        )

    def __contains__(self, value):
        return self._cache.get(value, False) is True

    def __iter__(self):
        return iter(self._cache)

    def __len__(self):
        return len(self._cache)

    def add(self, value):
        self._cache[value] = True

    def discard(self, value):
        self._cache.pop(value, None)

    def clear(self):
        self._cache.clear()

    def prune(self):
        return self._cache.prune()
