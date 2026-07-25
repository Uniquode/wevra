from __future__ import annotations

import math
from typing import Final

from wybra.config import ConfigDef, ConfigField, ConfigGroup, to_bool
from wybra.config.transforms import (
    to_non_blank_string,
    to_optional_non_blank_string,
    to_optional_positive_float,
    to_positive_float,
    to_positive_int,
)

TASKS_CONFIG_SECTION: Final = "tasks"
DEFAULT_TASK_BACKEND: Final = "immediate"
DEFAULT_TASK_QUEUE: Final = "default"
DEFAULT_TASK_STATUS_RETENTION_SECONDS: Final = 3600.0
DEFAULT_TASK_WORKER_CONCURRENCY: Final = 1


def to_task_backend(value: object) -> str:
    backend = to_non_blank_string(value).lower()
    if backend != "immediate":
        raise ValueError("tasks backend must be 'immediate'.")
    return backend


def to_non_negative_float(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("must be a non-negative number.")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("must be a non-negative number.") from exc
    if parsed < 0 or not math.isfinite(parsed):
        raise ValueError("must be a non-negative number.")
    return parsed


module_config: Final = ConfigDef(
    {
        TASKS_CONFIG_SECTION: ConfigGroup(
            fields=(
                ConfigField(
                    name="enabled",
                    default=True,
                    transform=to_bool,
                ),
                ConfigField(
                    name="backend",
                    default=DEFAULT_TASK_BACKEND,
                    transform=to_task_backend,
                ),
                ConfigField(
                    name="default_queue",
                    default=DEFAULT_TASK_QUEUE,
                    transform=to_non_blank_string,
                ),
                ConfigField(
                    name="max_attempts",
                    default=1,
                    transform=to_positive_int,
                ),
                ConfigField(
                    name="initial_delay_seconds",
                    default=0.0,
                    transform=to_non_negative_float,
                ),
                ConfigField(
                    name="backoff_multiplier",
                    default=2.0,
                    transform=to_positive_float,
                ),
                ConfigField(
                    name="maximum_delay_seconds",
                    default=None,
                    transform=to_optional_positive_float,
                ),
                ConfigField(
                    name="jitter_seconds",
                    default=0.0,
                    transform=to_non_negative_float,
                ),
                ConfigField(
                    name="status_retention_seconds",
                    default=DEFAULT_TASK_STATUS_RETENTION_SECONDS,
                    transform=to_positive_float,
                ),
                ConfigField(
                    name="worker_id",
                    default=None,
                    transform=to_optional_non_blank_string,
                ),
                ConfigField(
                    name="worker_concurrency",
                    default=DEFAULT_TASK_WORKER_CONCURRENCY,
                    transform=to_positive_int,
                ),
                ConfigField(
                    name="scheduler_owner",
                    default=None,
                    transform=to_optional_non_blank_string,
                ),
                ConfigField(
                    name="broker_url",
                    default=None,
                    transform=to_optional_non_blank_string,
                ),
            )
        )
    }
)


__all__ = (
    "DEFAULT_TASK_BACKEND",
    "DEFAULT_TASK_QUEUE",
    "DEFAULT_TASK_STATUS_RETENTION_SECONDS",
    "DEFAULT_TASK_WORKER_CONCURRENCY",
    "TASKS_CONFIG_SECTION",
    "module_config",
    "to_non_negative_float",
    "to_task_backend",
)
