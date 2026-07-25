from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from typing import ClassVar

from wybra.config import BaseSettings, ConfigDef
from wybra.core.exceptions import ConfigurationError
from wybra.tasks.config import (
    DEFAULT_TASK_BACKEND,
    DEFAULT_TASK_QUEUE,
    DEFAULT_TASK_STATUS_RETENTION_SECONDS,
    DEFAULT_TASK_WORKER_CONCURRENCY,
    TASKS_CONFIG_SECTION,
    module_config,
    to_task_backend,
)
from wybra.tasks.models import RetryPolicy


@dataclass(frozen=True, slots=True)
class TasksSettings(BaseSettings):
    module_config: ClassVar[ConfigDef] = module_config
    config_section: ClassVar[str | None] = TASKS_CONFIG_SECTION

    backend: str = DEFAULT_TASK_BACKEND
    default_queue: str = DEFAULT_TASK_QUEUE
    max_attempts: int = 1
    initial_delay_seconds: float = 0.0
    backoff_multiplier: float = 2.0
    maximum_delay_seconds: float | None = None
    jitter_seconds: float = 0.0
    status_retention_seconds: float = DEFAULT_TASK_STATUS_RETENTION_SECONDS
    worker_id: str | None = None
    worker_concurrency: int = DEFAULT_TASK_WORKER_CONCURRENCY
    scheduler_owner: str | None = None
    broker_url: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        try:
            backend = to_task_backend(self.backend)
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
            worker_id = _optional_string(self.worker_id, "worker_id")
            scheduler_owner = _optional_string(
                self.scheduler_owner,
                "scheduler_owner",
            )
            broker_url = _optional_string(self.broker_url, "broker_url")
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"tasks: {exc}") from exc
        object.__setattr__(self, "backend", backend)
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
            "worker_id",
            worker_id,
        )
        object.__setattr__(
            self,
            "scheduler_owner",
            scheduler_owner,
        )
        object.__setattr__(
            self,
            "broker_url",
            broker_url,
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


__all__ = ("TasksSettings",)
