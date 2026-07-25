from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from uuid import uuid4

from wybra.cache.feature_models import (
    CacheConflictError,
    CacheFeatureError,
    FencingToken,
    LeaseToken,
    validate_positive_finite,
    validate_positive_integer,
    validate_resource,
)

type Clock = Callable[[], float]
type LeaseKey = tuple[str, str]


@dataclass(slots=True)
class _LeaseState:
    holder: str
    fencing_token: FencingToken
    expires_at: float
    token: str

    def value(self, owner: str, resource: str) -> LeaseToken:
        return LeaseToken(
            owner=owner,
            resource=resource,
            holder=self.holder,
            fencing_token=self.fencing_token,
            expires_at=self.expires_at,
            token=self.token,
        )


@dataclass(slots=True)
class InMemoryLeaseCache:
    clock: Clock = field(default=time.monotonic, repr=False)
    max_leases: int = 10_000
    _leases: dict[LeaseKey, _LeaseState] = field(default_factory=dict, init=False)
    _next_fencing_token: int = field(default=0, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        validate_positive_integer(self.max_leases, label="maximum leases")

    async def acquire(
        self,
        owner: str,
        resource: str,
        holder: str,
        *,
        ttl: float,
    ) -> LeaseToken | None:
        lease_key = _lease_key(owner, resource)
        holder = validate_resource(holder, label="lease holder")
        ttl = validate_positive_finite(ttl, label="lease TTL")
        async with self._lock:
            current = self._live_lease(lease_key)
            if current is not None:
                return None
            self._ensure_capacity()
            self._next_fencing_token += 1
            state = _LeaseState(
                holder=holder,
                fencing_token=FencingToken(self._next_fencing_token),
                expires_at=self.clock() + ttl,
                token=uuid4().hex,
            )
            self._leases[lease_key] = state
            return state.value(owner, resource)

    async def renew(self, lease: LeaseToken, *, ttl: float) -> LeaseToken:
        ttl = validate_positive_finite(ttl, label="lease TTL")
        async with self._lock:
            lease_key, current = self._required_lease(lease)
            current.expires_at = self.clock() + ttl
            self._leases[lease_key] = current
            return current.value(lease.owner, lease.resource)

    async def release(self, lease: LeaseToken) -> None:
        async with self._lock:
            lease_key, _current = self._required_lease(lease)
            self._leases.pop(lease_key, None)

    def _required_lease(self, lease: LeaseToken) -> tuple[LeaseKey, _LeaseState]:
        lease_key = _lease_key_from_token(lease)
        current = self._live_lease(lease_key)
        if (
            current is None
            or current.token != lease.token
            or current.fencing_token != lease.fencing_token
            or current.holder != lease.holder
        ):
            raise CacheConflictError(
                f"Lease for resource {lease.resource!r} is stale or no longer held."
            )
        return lease_key, current

    def _live_lease(self, lease_key: LeaseKey) -> _LeaseState | None:
        current = self._leases.get(lease_key)
        if current is not None and current.expires_at <= self.clock():
            self._leases.pop(lease_key, None)
            return None
        return current

    def _ensure_capacity(self) -> None:
        now = self.clock()
        expired = [
            lease_key
            for lease_key, state in self._leases.items()
            if state.expires_at <= now
        ]
        for lease_key in expired:
            self._leases.pop(lease_key, None)
        if len(self._leases) >= self.max_leases:
            raise CacheFeatureError(
                "The in-memory lease cache has reached its configured lease capacity."
            )


def _lease_key(owner: str, resource: str) -> LeaseKey:
    return (
        validate_resource(owner, label="cache owner"),
        validate_resource(resource, label="lease resource"),
    )


def _lease_key_from_token(lease: LeaseToken) -> LeaseKey:
    return _lease_key(lease.owner, lease.resource)


__all__ = ("InMemoryLeaseCache",)
