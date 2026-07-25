from __future__ import annotations

from contextvars import ContextVar, Token
from uuid import uuid7

from wybra.tasks.models import TaskExecutionContext, TaskIdentity

_CURRENT_TASK_CONTEXT: ContextVar[TaskExecutionContext | None] = ContextVar(
    "wybra_current_task_context",
    default=None,
)


def current_task_context() -> TaskExecutionContext | None:
    return _CURRENT_TASK_CONTEXT.get()


def direct_task_context(identity: TaskIdentity) -> TaskExecutionContext:
    parent = current_task_context()
    task_id = uuid7()
    return TaskExecutionContext(
        task_id=task_id,
        task_name=identity.name,
        schema_version=identity.version,
        attempt=1,
        correlation_id=parent.correlation_id if parent is not None else task_id,
        causation_id=parent.task_id if parent is not None else None,
    )


def set_task_context(
    context: TaskExecutionContext,
) -> Token[TaskExecutionContext | None]:
    return _CURRENT_TASK_CONTEXT.set(context)


def reset_task_context(token: Token[TaskExecutionContext | None]) -> None:
    _CURRENT_TASK_CONTEXT.reset(token)


__all__ = (
    "current_task_context",
    "direct_task_context",
    "reset_task_context",
    "set_task_context",
)
