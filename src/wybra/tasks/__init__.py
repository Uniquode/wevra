"""Declarative task definitions and provider-neutral execution contracts."""

from wybra.tasks.capabilities import (
    TaskDispatchPolicy,
    TaskHandle,
    TasksCapability,
    TaskSubmissionOptions,
    dispatch,
)
from wybra.tasks.config import module_config
from wybra.tasks.context import current_task_context
from wybra.tasks.declarations import TaskDefinition, task
from wybra.tasks.lifecycle import (
    TaskLifecycleError,
    TaskLifecycleEvent,
    TaskLifecycleKind,
    TaskState,
    TaskStatus,
)
from wybra.tasks.models import (
    RetryPolicy,
    TaskDeclarationError,
    TaskExecutionContext,
    TaskIdentity,
    TaskPayload,
    TaskPayloadError,
    TaskRegistrationError,
)
from wybra.tasks.registry import TaskRegistry
from wybra.tasks.settings import TasksSettings
from wybra.tasks.setup import setup_site

__all__ = (
    "RetryPolicy",
    "TaskDispatchPolicy",
    "TaskDeclarationError",
    "TaskDefinition",
    "TaskExecutionContext",
    "TaskHandle",
    "TaskIdentity",
    "TaskLifecycleError",
    "TaskLifecycleEvent",
    "TaskLifecycleKind",
    "TaskPayload",
    "TaskPayloadError",
    "TaskRegistrationError",
    "TaskRegistry",
    "TaskState",
    "TaskStatus",
    "TaskSubmissionOptions",
    "TasksCapability",
    "TasksSettings",
    "current_task_context",
    "dispatch",
    "module_config",
    "setup_site",
    "task",
)
