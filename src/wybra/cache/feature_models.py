from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

MAX_CACHE_FEATURE_PAYLOAD_BYTES = 1_048_576
MAX_CACHE_FEATURE_LIMIT = 10_000
_FEATURE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9-]*$")


class CacheFeatureError(RuntimeError):
    """Base error for optional cache feature operations."""


class CacheFeatureUnavailableError(CacheFeatureError, LookupError):
    """Raised when a cache does not advertise a required feature."""


class CacheConflictError(CacheFeatureError):
    """Raised when a revision, delivery, lease, or claim is stale."""


class CachePositionExpiredError(CacheFeatureError):
    """Raised when a requested stream position is no longer retained."""


@dataclass(frozen=True, slots=True)
class CacheFeatureGuarantees:
    scope: str
    durable: bool
    restart_recovery: bool
    horizontal_consumers: bool
    ordering_scope: str | None = None
    replay: bool = False
    retention: bool = False
    acknowledgement: bool = False
    redelivery: bool = False
    scheduling: bool = False

    def __post_init__(self) -> None:
        if self.scope not in {"process", "shared"}:
            raise ValueError("Cache feature scope must be 'process' or 'shared'.")
        if self.ordering_scope is not None:
            validate_feature_name(self.ordering_scope, label="ordering scope")


@dataclass(frozen=True, slots=True)
class CacheFeatureMetadata:
    name: str
    guarantees: CacheFeatureGuarantees

    def __post_init__(self) -> None:
        validate_feature_name(self.name, label="feature name")


@dataclass(frozen=True, slots=True)
class CacheFeatureRegistration:
    protocol: type[object] = field(repr=False)
    implementation: object = field(repr=False)
    metadata: CacheFeatureMetadata

    def __post_init__(self) -> None:
        if not isinstance(self.protocol, type):
            raise TypeError("Cache feature protocol must be a type.")


@dataclass(frozen=True, slots=True, order=True)
class CacheRevision:
    value: int

    def __post_init__(self) -> None:
        validate_positive_integer(self.value, label="cache revision")


@dataclass(frozen=True, slots=True, order=True)
class FencingToken:
    value: int

    def __post_init__(self) -> None:
        validate_positive_integer(self.value, label="fencing token")


@dataclass(frozen=True, slots=True)
class AtomicCacheValue:
    value: bytes = field(repr=False)
    revision: CacheRevision

    def __post_init__(self) -> None:
        validate_payload(self.value)


@dataclass(frozen=True, slots=True)
class CounterCacheValue:
    value: int
    revision: CacheRevision


@dataclass(frozen=True, slots=True)
class LeaseToken:
    owner: str
    resource: str
    holder: str
    fencing_token: FencingToken
    expires_at: float
    token: str = field(repr=False)

    def __post_init__(self) -> None:
        validate_resource(self.owner, label="cache owner")
        validate_resource(self.resource)
        validate_resource(self.holder, label="lease holder")
        validate_finite(self.expires_at, label="lease expiry")
        validate_resource(self.token, label="lease token")


@dataclass(frozen=True, slots=True)
class WorkIdentity:
    value: str

    def __post_init__(self) -> None:
        validate_resource(self.value, label="work identity")


@dataclass(frozen=True, slots=True)
class WorkDelivery:
    queue: str
    identity: WorkIdentity
    payload: bytes = field(repr=False)
    attempt: int
    visible_until: float
    receipt: str = field(repr=False)

    def __post_init__(self) -> None:
        validate_resource(self.queue, label="queue")
        validate_payload(self.payload)
        validate_positive_integer(self.attempt, label="delivery attempt")
        validate_finite(self.visible_until, label="delivery visibility deadline")
        validate_resource(self.receipt, label="delivery receipt")


@dataclass(frozen=True, slots=True, order=True)
class StreamPosition:
    value: int

    def __post_init__(self) -> None:
        validate_positive_integer(self.value, label="stream position")


@dataclass(frozen=True, slots=True)
class StreamRecord:
    stream: str
    position: StreamPosition
    payload: bytes = field(repr=False)

    def __post_init__(self) -> None:
        validate_resource(self.stream, label="stream")
        validate_payload(self.payload)


@dataclass(frozen=True, slots=True)
class ScheduleRecord:
    identity: str
    revision: CacheRevision
    payload: bytes = field(repr=False)
    next_due_at: float
    interval_seconds: float | None = None

    def __post_init__(self) -> None:
        validate_resource(self.identity, label="schedule identity")
        validate_payload(self.payload)
        validate_finite(self.next_due_at, label="schedule due time")
        if self.interval_seconds is not None:
            validate_positive_finite(
                self.interval_seconds,
                label="schedule interval",
            )


@dataclass(frozen=True, slots=True)
class ScheduleClaim:
    owner: str
    record: ScheduleRecord
    claimant: str
    fencing_token: FencingToken
    expires_at: float
    token: str = field(repr=False)

    def __post_init__(self) -> None:
        validate_resource(self.owner, label="cache owner")
        validate_resource(self.claimant, label="schedule claimant")
        validate_finite(self.expires_at, label="schedule claim expiry")
        validate_resource(self.token, label="schedule claim token")


def validate_feature_name(value: str, *, label: str = "feature name") -> str:
    if not isinstance(value, str) or _FEATURE_NAME_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f"{label.capitalize()} must start with a lowercase letter and contain "
            "only lowercase letters, digits, and hyphens."
        )
    return value


def validate_resource(value: str, *, label: str = "resource") -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label.capitalize()} must be a non-blank string.")
    return value


def validate_payload(value: bytes) -> bytes:
    if not isinstance(value, bytes):
        raise TypeError("Cache feature payloads must be bytes.")
    if len(value) > MAX_CACHE_FEATURE_PAYLOAD_BYTES:
        raise ValueError(
            f"Cache feature payloads cannot exceed "
            f"{MAX_CACHE_FEATURE_PAYLOAD_BYTES} bytes."
        )
    return value


def validate_finite(value: float, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{label.capitalize()} must be a finite number.")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label.capitalize()} must be finite.")
    return result


def validate_positive_finite(value: float, *, label: str) -> float:
    result = validate_finite(value, label=label)
    if result <= 0:
        raise ValueError(f"{label.capitalize()} must be greater than zero.")
    return result


def validate_non_negative_finite(value: float, *, label: str) -> float:
    result = validate_finite(value, label=label)
    if result < 0:
        raise ValueError(f"{label.capitalize()} cannot be negative.")
    return result


def validate_positive_integer(value: int, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label.capitalize()} must be a positive integer.")
    return value


def validate_limit(value: int) -> int:
    validate_positive_integer(value, label="limit")
    if value > MAX_CACHE_FEATURE_LIMIT:
        raise ValueError(f"Limit cannot exceed {MAX_CACHE_FEATURE_LIMIT}.")
    return value


__all__ = (
    "AtomicCacheValue",
    "CacheConflictError",
    "CacheFeatureError",
    "CacheFeatureGuarantees",
    "CacheFeatureMetadata",
    "CacheFeatureRegistration",
    "CacheFeatureUnavailableError",
    "CachePositionExpiredError",
    "CacheRevision",
    "CounterCacheValue",
    "FencingToken",
    "LeaseToken",
    "MAX_CACHE_FEATURE_LIMIT",
    "MAX_CACHE_FEATURE_PAYLOAD_BYTES",
    "ScheduleClaim",
    "ScheduleRecord",
    "StreamPosition",
    "StreamRecord",
    "WorkDelivery",
    "WorkIdentity",
    "validate_feature_name",
    "validate_finite",
    "validate_limit",
    "validate_non_negative_finite",
    "validate_payload",
    "validate_positive_finite",
    "validate_positive_integer",
    "validate_resource",
)
