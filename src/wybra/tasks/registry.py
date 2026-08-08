"""Internal site-scoped registry for durable task declarations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from weakref import ReferenceType, ref

from wybra.tasks.models import TaskIdentity, TaskRegistrationError

if TYPE_CHECKING:
    from wybra.tasks.declarations import TaskDefinition


@dataclass(slots=True)
class TaskRegistry:
    """Site-local durable task definitions discovered from configured modules."""

    _definitions: dict[TaskIdentity, TaskDefinition] = field(default_factory=dict)

    def register(self, definition: TaskDefinition) -> None:
        identity = definition.identity
        existing = self._definitions.get(identity)
        if existing is not None and existing is not definition:
            raise TaskRegistrationError(
                f"Task {identity.name!r} version {identity.version} is declared more "
                "than once by configured modules."
            )
        self._definitions[identity] = definition

    def definitions(self) -> tuple[TaskDefinition, ...]:
        return tuple(self._definitions.values())


_declarations: dict[str, list[ReferenceType[TaskDefinition]]] = {}


def register_task_declaration(module_name: str, definition: TaskDefinition) -> None:
    """Record a declaration until a composed site selects its owning module."""
    _declarations.setdefault(module_name, []).append(ref(definition))


def discover_task_registry(module_roots: tuple[str, ...]) -> TaskRegistry | None:
    """Build a site-local registry from declarations owned by configured modules."""
    registry = TaskRegistry()
    for module_name, references in tuple(_declarations.items()):
        live_references: list[ReferenceType[TaskDefinition]] = []
        definitions: list[TaskDefinition] = []
        for reference in references:
            definition = reference()
            if definition is not None:
                live_references.append(reference)
                definitions.append(definition)
        if not definitions:
            _declarations.pop(module_name)
            continue
        _declarations[module_name] = live_references
        if not any(_module_is_owned(module_name, root) for root in module_roots):
            continue
        for definition in definitions:
            registry.register(definition)
    return registry if registry.definitions() else None


def _module_is_owned(module_name: str, module_root: str) -> bool:
    return module_name == module_root or module_name.startswith(f"{module_root}.")


__all__ = ("TaskRegistry", "discover_task_registry", "register_task_declaration")
