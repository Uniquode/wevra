from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from wybra.tasks.models import TaskIdentity, TaskRegistrationError

if TYPE_CHECKING:
    from wybra.tasks.declarations import TaskDefinition


@dataclass(slots=True)
class TaskRegistry:
    _definitions: dict[TaskIdentity, object] = field(default_factory=dict)

    def register[DefinitionT: TaskDefinition](
        self,
        definition: DefinitionT,
    ) -> DefinitionT:
        identity = definition.identity
        if identity in self._definitions:
            raise TaskRegistrationError(
                f"Task {identity.name!r} version {identity.version} is already "
                "registered."
            )
        self._definitions[identity] = definition
        return definition

    def require(self, identity: TaskIdentity) -> TaskDefinition:
        try:
            definition = self._definitions[identity]
        except KeyError as exc:
            raise TaskRegistrationError(
                f"Task {identity.name!r} version {identity.version} is not registered."
            ) from exc
        return cast("TaskDefinition", definition)

    def definitions(self) -> tuple[TaskDefinition, ...]:
        return tuple(
            cast("TaskDefinition", item) for item in self._definitions.values()
        )


default_task_registry = TaskRegistry()


__all__ = ("TaskRegistry", "default_task_registry")
