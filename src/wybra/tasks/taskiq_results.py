from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from math import isfinite
from typing import Any

from taskiq import AsyncResultBackend, TaskiqResult

from wybra.cache import CacheCapability

_TASKIQ_RESULT_OWNER = "taskiq-result"


def _encode_default_result(result: TaskiqResult[Any]) -> bytes:
    return result.model_dump_json().encode()


def _decode_default_result(payload: bytes) -> TaskiqResult[Any]:
    return TaskiqResult.model_validate_json(payload)


@dataclass(frozen=True, slots=True)
class TaskiqResultPolicy:
    """Caller-owned retention and safe serialisation policy for Taskiq results."""

    retention_seconds: float
    maximum_serialised_bytes: int
    encode: Callable[[TaskiqResult[Any]], bytes] | None = None
    decode: Callable[[bytes], TaskiqResult[Any]] | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.retention_seconds, bool)
            or not isinstance(self.retention_seconds, int | float)
            or not isfinite(self.retention_seconds)
            or self.retention_seconds <= 0
        ):
            raise ValueError("Result retention must be a positive finite number.")
        if (
            isinstance(self.maximum_serialised_bytes, bool)
            or not isinstance(self.maximum_serialised_bytes, int)
            or self.maximum_serialised_bytes < 1
        ):
            raise ValueError("Result byte limit must be a positive integer.")
        if self.encode is not None and not callable(self.encode):
            raise ValueError("Result encoder must be callable.")
        if self.decode is not None and not callable(self.decode):
            raise ValueError("Result decoder must be callable.")

    def serialise(self, result: TaskiqResult[Any]) -> bytes:
        if self.encode is None:
            return _encode_default_result(result)
        return self.encode(result)

    def deserialise(self, payload: bytes) -> TaskiqResult[Any]:
        if self.decode is None:
            return _decode_default_result(payload)
        return self.decode(payload)


class CacheTaskiqResultBackend(AsyncResultBackend[Any]):
    """Store Taskiq result envelopes through Wybra's baseline cache capability."""

    def __init__(self, cache: CacheCapability, policy: TaskiqResultPolicy) -> None:
        self._cache = cache
        self._policy = policy

    async def set_result(self, task_id: str, result: TaskiqResult[Any]) -> None:
        payload = self._policy.serialise(result)
        if not isinstance(payload, bytes):
            raise TypeError("Taskiq result encoder must return bytes.")
        if len(payload) > self._policy.maximum_serialised_bytes:
            raise ValueError("Taskiq result exceeds the configured byte limit.")
        await self._cache.set(
            _TASKIQ_RESULT_OWNER,
            task_id,
            payload,
            ttl=float(self._policy.retention_seconds),
        )

    async def is_result_ready(self, task_id: str) -> bool:
        return await self._cache.get(_TASKIQ_RESULT_OWNER, task_id) is not None

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[Any]:
        del with_logs
        payload = await self._cache.get(_TASKIQ_RESULT_OWNER, task_id)
        if payload is None:
            raise KeyError("Task result is unavailable.")
        return self._policy.deserialise(payload)


__all__ = ("CacheTaskiqResultBackend", "TaskiqResultPolicy")
