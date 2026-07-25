from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final
from uuid import UUID

TASK_NAME_PATTERN: Final = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)


class TaskDeclarationError(ValueError):
    """Raised when a task declaration is invalid."""


class TaskPayloadError(ValueError):
    """Raised when task arguments cannot form a safe payload."""


class TaskRegistrationError(ValueError):
    """Raised when a task identity cannot be registered."""


@dataclass(frozen=True, slots=True, order=True)
class TaskIdentity:
    name: str
    version: int = 1

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not TASK_NAME_PATTERN.fullmatch(name):
            raise TaskDeclarationError("Task name must be a dotted Python identifier.")
        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise TaskDeclarationError(
                "Task schema version must be a positive integer."
            )
        if self.version < 1:
            raise TaskDeclarationError(
                "Task schema version must be a positive integer."
            )
        object.__setattr__(self, "name", name)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 1
    initial_delay_seconds: float = 0.0
    backoff_multiplier: float = 2.0
    maximum_delay_seconds: float | None = None
    jitter_seconds: float = 0.0

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
        ):
            raise TaskDeclarationError(
                "Task maximum attempts must be a positive integer."
            )
        initial_delay = _finite_number(
            self.initial_delay_seconds,
            "Task initial retry delay",
        )
        backoff_multiplier = _finite_number(
            self.backoff_multiplier,
            "Task retry backoff multiplier",
        )
        maximum_delay = (
            None
            if self.maximum_delay_seconds is None
            else _finite_number(
                self.maximum_delay_seconds,
                "Task maximum retry delay",
            )
        )
        jitter = _finite_number(self.jitter_seconds, "Task retry jitter")
        if initial_delay < 0:
            raise TaskDeclarationError("Task initial retry delay cannot be negative.")
        if backoff_multiplier < 1:
            raise TaskDeclarationError(
                "Task retry backoff multiplier cannot be less than one."
            )
        if maximum_delay is not None and maximum_delay < initial_delay:
            raise TaskDeclarationError(
                "Task maximum retry delay cannot be less than the initial delay."
            )
        if jitter < 0:
            raise TaskDeclarationError("Task retry jitter cannot be negative.")
        object.__setattr__(self, "initial_delay_seconds", initial_delay)
        object.__setattr__(self, "backoff_multiplier", backoff_multiplier)
        object.__setattr__(self, "maximum_delay_seconds", maximum_delay)
        object.__setattr__(self, "jitter_seconds", jitter)


@dataclass(frozen=True, slots=True, init=False)
class TaskPayload:
    _encoded: bytes = field(repr=False)

    def __init__(self, arguments: Mapping[str, object]) -> None:
        if not isinstance(arguments, Mapping) or any(
            not isinstance(name, str) for name in arguments
        ):
            raise TaskPayloadError("Task arguments must be a mapping with string keys.")
        object.__setattr__(self, "_encoded", _encode_arguments(dict(arguments)))

    @property
    def arguments(self) -> dict[str, object]:
        decoded = json.loads(self._encoded)
        if not isinstance(
            decoded, dict
        ):  # pragma: no cover - constructor guarantees it
            raise TaskPayloadError("Task arguments must decode to a mapping.")
        return decoded

    def to_json(self) -> bytes:
        return self._encoded


@dataclass(frozen=True, slots=True)
class TaskExecutionContext:
    task_id: UUID
    task_name: str
    schema_version: int
    attempt: int
    correlation_id: UUID
    causation_id: UUID | None = None
    idempotency_key: str | None = None
    queue: str | None = None
    worker_id: str | None = None


def _finite_number(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise TaskDeclarationError(f"{label} must be a finite number.")
    return float(value)


def _encode_arguments(arguments: dict[str, object]) -> bytes:
    try:
        encoded = json.dumps(
            arguments,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise TaskPayloadError("Task arguments must be JSON-compatible.") from exc
    return encoded.encode()


__all__ = (
    "RetryPolicy",
    "TaskDeclarationError",
    "TaskExecutionContext",
    "TaskIdentity",
    "TaskPayload",
    "TaskPayloadError",
    "TaskRegistrationError",
)
