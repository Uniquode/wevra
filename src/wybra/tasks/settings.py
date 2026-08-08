from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import ClassVar

from wybra.cache import to_cache_name
from wybra.config import BaseSettings, ConfigDef, to_bool
from wybra.core.exceptions import ConfigurationError
from wybra.tasks.config import (
    DEFAULT_TASK_ACTIVE_STATUS_TIMEOUT_SECONDS,
    DEFAULT_TASK_BACKEND,
    DEFAULT_TASK_CACHE_NAME,
    DEFAULT_TASK_DELIVERY_ATTEMPTS,
    DEFAULT_TASK_QUEUE,
    DEFAULT_TASK_RESULT_BYTES,
    DEFAULT_TASK_RESULT_RETENTION_SECONDS,
    DEFAULT_TASK_STATUS_RETENTION_SECONDS,
    DEFAULT_TASK_VISIBILITY_TIMEOUT_SECONDS,
    DEFAULT_TASK_WAIT_TIMEOUT_SECONDS,
    DEFAULT_TASK_WORKER_CONCURRENCY,
    DEFAULT_TASK_WORKER_SHUTDOWN_GRACE_SECONDS,
    MAX_TASK_DELIVERY_ATTEMPTS,
    MAX_TASK_RESULT_BYTES,
    MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS,
    TASKS_CONFIG_SECTION,
    module_config,
    to_task_backend,
)
from wybra.tasks.models import RetryPolicy


@dataclass(frozen=True, slots=True)
class TasksSettings(BaseSettings):
    module_config: ClassVar[ConfigDef] = module_config
    config_section: ClassVar[str | None] = TASKS_CONFIG_SECTION

    enabled: bool = True
    backend: str = DEFAULT_TASK_BACKEND
    cache_name: str = DEFAULT_TASK_CACHE_NAME
    default_queue: str = DEFAULT_TASK_QUEUE
    max_attempts: int = 1
    initial_delay_seconds: float = 0.0
    backoff_multiplier: float = 2.0
    maximum_delay_seconds: float | None = None
    jitter_seconds: float = 0.0
    status_retention_seconds: float = DEFAULT_TASK_STATUS_RETENTION_SECONDS
    active_status_timeout_seconds: float = DEFAULT_TASK_ACTIVE_STATUS_TIMEOUT_SECONDS
    worker_id: str | None = None
    worker_concurrency: int = DEFAULT_TASK_WORKER_CONCURRENCY
    worker_shutdown_grace_seconds: float = DEFAULT_TASK_WORKER_SHUTDOWN_GRACE_SECONDS
    visibility_timeout_seconds: float = DEFAULT_TASK_VISIBILITY_TIMEOUT_SECONDS
    wait_timeout_seconds: float = DEFAULT_TASK_WAIT_TIMEOUT_SECONDS
    max_delivery_attempts: int = DEFAULT_TASK_DELIVERY_ATTEMPTS
    result_retention_seconds: float = DEFAULT_TASK_RESULT_RETENTION_SECONDS
    max_result_bytes: int = DEFAULT_TASK_RESULT_BYTES

    def __post_init__(self) -> None:
        try:
            enabled = to_bool(self.enabled)
            backend = to_task_backend(self.backend)
            if backend == "taskiq":
                cache_name = _validated_taskiq_cache_name(self.cache_name)
            else:
                if not isinstance(self.cache_name, str):
                    raise ValueError("cache_name must be a string.")
                cache_name = self.cache_name
            if (
                not isinstance(self.default_queue, str)
                or not self.default_queue.strip()
            ):
                raise ValueError("default_queue must not be blank.")
            default_queue = self.default_queue.strip()
            retry = RetryPolicy(
                max_attempts=self.max_attempts,
                initial_delay_seconds=self.initial_delay_seconds,
                backoff_multiplier=self.backoff_multiplier,
                maximum_delay_seconds=self.maximum_delay_seconds,
                jitter_seconds=self.jitter_seconds,
            )
            if isinstance(self.status_retention_seconds, bool):
                raise ValueError(
                    "status_retention_seconds must be a positive finite number."
                )
            retention = float(self.status_retention_seconds)
            if retention <= 0 or not isfinite(retention):
                raise ValueError(
                    "status_retention_seconds must be a positive finite number."
                )
            if (
                isinstance(self.worker_concurrency, bool)
                or not isinstance(self.worker_concurrency, int)
                or self.worker_concurrency < 1
            ):
                raise ValueError("worker_concurrency must be a positive integer.")
            for field_name, value in (
                ("visibility_timeout_seconds", self.visibility_timeout_seconds),
                ("wait_timeout_seconds", self.wait_timeout_seconds),
                ("worker_shutdown_grace_seconds", self.worker_shutdown_grace_seconds),
                ("result_retention_seconds", self.result_retention_seconds),
                ("active_status_timeout_seconds", self.active_status_timeout_seconds),
            ):
                if isinstance(value, bool) or not isinstance(value, int | float):
                    raise ValueError(f"{field_name} must be a positive finite number.")
                if value <= 0 or not isfinite(value):
                    raise ValueError(f"{field_name} must be a positive finite number.")
            if (
                self.visibility_timeout_seconds
                < MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS
            ):
                raise ValueError(
                    "visibility_timeout_seconds must be at least "
                    f"{MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS} seconds."
                )
            for field_name, value in (
                ("max_delivery_attempts", self.max_delivery_attempts),
                ("max_result_bytes", self.max_result_bytes),
            ):
                if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                    raise ValueError(f"{field_name} must be a positive integer.")
            if self.max_delivery_attempts > MAX_TASK_DELIVERY_ATTEMPTS:
                raise ValueError(
                    "max_delivery_attempts must not exceed "
                    f"{MAX_TASK_DELIVERY_ATTEMPTS}."
                )
            if self.max_result_bytes > MAX_TASK_RESULT_BYTES:
                raise ValueError(
                    f"max_result_bytes must not exceed {MAX_TASK_RESULT_BYTES}."
                )
            worker_id = _optional_string(self.worker_id, "worker_id")
        except ConfigurationError:
            raise
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"tasks: {exc}") from exc
        object.__setattr__(self, "enabled", enabled)
        object.__setattr__(self, "backend", backend)
        object.__setattr__(self, "cache_name", cache_name)
        object.__setattr__(self, "default_queue", default_queue)
        object.__setattr__(self, "max_attempts", retry.max_attempts)
        object.__setattr__(
            self,
            "initial_delay_seconds",
            retry.initial_delay_seconds,
        )
        object.__setattr__(
            self,
            "backoff_multiplier",
            retry.backoff_multiplier,
        )
        object.__setattr__(
            self,
            "maximum_delay_seconds",
            retry.maximum_delay_seconds,
        )
        object.__setattr__(self, "jitter_seconds", retry.jitter_seconds)
        object.__setattr__(self, "status_retention_seconds", retention)
        object.__setattr__(
            self,
            "active_status_timeout_seconds",
            float(self.active_status_timeout_seconds),
        )
        object.__setattr__(
            self,
            "visibility_timeout_seconds",
            float(self.visibility_timeout_seconds),
        )
        object.__setattr__(
            self,
            "wait_timeout_seconds",
            float(self.wait_timeout_seconds),
        )
        object.__setattr__(
            self,
            "worker_shutdown_grace_seconds",
            float(self.worker_shutdown_grace_seconds),
        )
        object.__setattr__(
            self,
            "max_delivery_attempts",
            self.max_delivery_attempts,
        )
        object.__setattr__(
            self,
            "result_retention_seconds",
            float(self.result_retention_seconds),
        )
        object.__setattr__(self, "max_result_bytes", self.max_result_bytes)
        object.__setattr__(
            self,
            "worker_id",
            worker_id,
        )

    @property
    def retry_policy(self) -> RetryPolicy:
        return RetryPolicy(
            max_attempts=self.max_attempts,
            initial_delay_seconds=self.initial_delay_seconds,
            backoff_multiplier=self.backoff_multiplier,
            maximum_delay_seconds=self.maximum_delay_seconds,
            jitter_seconds=self.jitter_seconds,
        )


def _optional_string(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string when configured.")
    return value.strip()


def _validated_taskiq_cache_name(value: object) -> str:
    try:
        cache_name = to_cache_name(value)
    except TypeError, ValueError:
        pass
    else:
        return cache_name
    raise ConfigurationError("tasks.cache_name must be a valid cache name.")


__all__ = ("TasksSettings",)
