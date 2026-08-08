"""Declarative task definitions and provider-neutral execution contracts."""

from wybra.tasks.capabilities import (
    TaskDispatchPolicy,
    TaskFeature,
    TaskFeatures,
    TaskFeatureUnavailableError,
    TaskHandle,
    TasksCapability,
    TaskSubmissionOptions,
    dispatch,
)
from wybra.tasks.config import module_config
from wybra.tasks.context import current_task_context
from wybra.tasks.declarations import TaskDefinition, task
from wybra.tasks.events import TASK_EVENT_SCOPE, TaskLifecycleObservationEvent
from wybra.tasks.lifecycle import (
    TaskLifecycleError,
    TaskLifecycleEvent,
    TaskLifecycleKind,
    TaskProgressError,
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
    TaskSubmissionError,
)
from wybra.tasks.settings import TasksSettings
from wybra.tasks.setup import post_setup_site, setup_site

__all__ = (
    "RetryPolicy",
    "TASK_EVENT_SCOPE",
    "TaskDispatchPolicy",
    "TaskDeclarationError",
    "TaskDefinition",
    "TaskExecutionContext",
    "TaskFeature",
    "TaskFeatures",
    "TaskFeatureUnavailableError",
    "TaskHandle",
    "TaskIdentity",
    "TaskLifecycleError",
    "TaskLifecycleEvent",
    "TaskLifecycleKind",
    "TaskLifecycleObservationEvent",
    "TaskPayload",
    "TaskPayloadError",
    "TaskProgressError",
    "TaskRegistrationError",
    "TaskSubmissionError",
    "TaskState",
    "TaskStatus",
    "TaskSubmissionOptions",
    "TasksCapability",
    "TasksSettings",
    "current_task_context",
    "dispatch",
    "module_config",
    "post_setup_site",
    "setup_site",
    "task",
)
