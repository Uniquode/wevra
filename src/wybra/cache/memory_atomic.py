from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from wybra.cache.feature_models import (
    AtomicCacheValue,
    CacheConflictError,
    CacheFeatureError,
    CacheRevision,
    CounterCacheValue,
    validate_payload,
    validate_positive_finite,
    validate_positive_integer,
    validate_resource,
)

type Clock = Callable[[], float]
type StorageKey = tuple[str, str]


@dataclass(slots=True)
class _AtomicEntry:
    value: bytes | int
    revision: CacheRevision
    expires_at: float


@dataclass(slots=True)
class InMemoryAtomicCache:
    clock: Clock = field(default=time.monotonic, repr=False)
    max_entries: int = 10_000
    _entries: dict[StorageKey, _AtomicEntry] = field(default_factory=dict, init=False)
    _next_revision: int = field(default=0, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        validate_positive_integer(self.max_entries, label="maximum atomic entries")

    async def get(self, owner: str, key: str) -> AtomicCacheValue | None:
        storage_key = _storage_key(owner, key)
        async with self._lock:
            entry = self._live_entry(storage_key)
            if entry is None:
                return None
            if not isinstance(entry.value, bytes):
                raise CacheConflictError("The requested atomic key contains a counter.")
            return AtomicCacheValue(entry.value, entry.revision)

    async def create(
        self,
        owner: str,
        key: str,
        value: bytes,
        *,
        ttl: float,
    ) -> AtomicCacheValue | None:
        storage_key = _storage_key(owner, key)
        value = validate_payload(value)
        ttl = validate_positive_finite(ttl, label="atomic value TTL")
        async with self._lock:
            if self._live_entry(storage_key) is not None:
                return None
            self._ensure_capacity(storage_key)
            revision = self._revision()
            self._entries[storage_key] = _AtomicEntry(
                value,
                revision,
                self.clock() + ttl,
            )
            return AtomicCacheValue(value, revision)

    async def compare_and_swap(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
        value: bytes,
        *,
        ttl: float,
    ) -> AtomicCacheValue | None:
        storage_key = _storage_key(owner, key)
        value = validate_payload(value)
        ttl = validate_positive_finite(ttl, label="atomic value TTL")
        async with self._lock:
            entry = self._live_entry(storage_key)
            if entry is None or entry.revision != expected:
                return None
            if not isinstance(entry.value, bytes):
                raise CacheConflictError("The requested atomic key contains a counter.")
            revision = self._revision()
            self._entries[storage_key] = _AtomicEntry(
                value,
                revision,
                self.clock() + ttl,
            )
            return AtomicCacheValue(value, revision)

    async def compare_and_delete(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
    ) -> bool:
        storage_key = _storage_key(owner, key)
        async with self._lock:
            entry = self._live_entry(storage_key)
            if entry is None or entry.revision != expected:
                return False
            if not isinstance(entry.value, bytes):
                raise CacheConflictError("The requested atomic key contains a counter.")
            self._entries.pop(storage_key, None)
            return True

    async def increment(
        self,
        owner: str,
        key: str,
        *,
        amount: int = 1,
        ttl: float,
    ) -> CounterCacheValue:
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise TypeError("Counter amount must be an integer.")
        storage_key = _storage_key(owner, key)
        ttl = validate_positive_finite(ttl, label="counter TTL")
        async with self._lock:
            entry = self._live_entry(storage_key)
            if entry is None:
                self._ensure_capacity(storage_key)
                value = amount
            elif isinstance(entry.value, int):
                value = entry.value + amount
            else:
                raise CacheConflictError(
                    "The requested counter key contains an atomic byte value."
                )
            revision = self._revision()
            self._entries[storage_key] = _AtomicEntry(
                value,
                revision,
                self.clock() + ttl,
            )
            return CounterCacheValue(value, revision)

    def _live_entry(self, storage_key: StorageKey) -> _AtomicEntry | None:
        entry = self._entries.get(storage_key)
        if entry is not None and entry.expires_at <= self.clock():
            self._entries.pop(storage_key, None)
            return None
        return entry

    def _ensure_capacity(self, storage_key: StorageKey) -> None:
        if storage_key in self._entries:
            return
        now = self.clock()
        expired = [
            key for key, entry in self._entries.items() if entry.expires_at <= now
        ]
        for key in expired:
            self._entries.pop(key, None)
        if len(self._entries) >= self.max_entries:
            raise CacheFeatureError(
                "The in-memory atomic cache has reached its configured entry capacity."
            )

    def _revision(self) -> CacheRevision:
        self._next_revision += 1
        return CacheRevision(self._next_revision)


def _storage_key(owner: str, key: str) -> StorageKey:
    return (
        validate_resource(owner, label="cache owner"),
        validate_resource(key, label="cache key"),
    )


__all__ = ("InMemoryAtomicCache",)
