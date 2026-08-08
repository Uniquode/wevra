from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from inspect import BoundArguments, Parameter, Signature, iscoroutinefunction, signature
from math import isfinite
from typing import Any, get_type_hints, overload

from pydantic import TypeAdapter, ValidationError

from wybra.tasks.config import MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS
from wybra.tasks.context import (
    direct_task_context,
    reset_task_context,
    set_task_context,
)
from wybra.tasks.models import (
    RetryPolicy,
    TaskDeclarationError,
    TaskExecutionContext,
    TaskIdentity,
    TaskPayload,
    TaskPayloadError,
)
from wybra.tasks.registry import register_task_declaration


@dataclass(frozen=True, slots=True, weakref_slot=True)
class TaskDefinition[**P, R]:
    identity: TaskIdentity
    retry: RetryPolicy | None
    _function: Callable[P, Awaitable[R]] = field(repr=False)
    _signature: Signature = field(repr=False)
    _adapters: Mapping[str, TypeAdapter[Any]] = field(repr=False)
    visibility_timeout_seconds: float | None = None

    def payload(self, *args: P.args, **kwargs: P.kwargs) -> TaskPayload:
        bound = self._validated_bound_arguments(*args, **kwargs)
        return TaskPayload(arguments=dict(bound.arguments))

    def validate_payload(self, payload: TaskPayload) -> TaskPayload:
        try:
            bound = self._signature.bind(**payload.arguments)
        except TypeError as exc:
            raise TaskPayloadError(str(exc)) from exc
        bound = self._validate_bound_arguments(bound)
        return TaskPayload(arguments=dict(bound.arguments))

    async def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return await self.execute(
            self.payload(*args, **kwargs),
            direct_task_context(self.identity),
        )

    async def execute(
        self,
        payload: TaskPayload,
        context: TaskExecutionContext,
    ) -> R:
        validated_payload = self.validate_payload(payload)
        bound = self._signature.bind(**validated_payload.arguments)
        token = set_task_context(context)
        try:
            return await self._function(*bound.args, **bound.kwargs)
        finally:
            reset_task_context(token)

    def _validated_bound_arguments(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> BoundArguments:
        try:
            bound = self._signature.bind(*args, **kwargs)
        except TypeError as exc:
            raise TaskPayloadError(str(exc)) from exc
        return self._validate_bound_arguments(bound)

    def _validate_bound_arguments(
        self,
        bound: BoundArguments,
    ) -> BoundArguments:
        bound.apply_defaults()
        try:
            for name, value in tuple(bound.arguments.items()):
                parameter = self._signature.parameters[name]
                bound.arguments[name] = _validated_argument(
                    parameter,
                    value,
                    self._adapters[name],
                )
        except (TypeError, ValueError, ValidationError) as exc:
            raise TaskPayloadError(
                f"Task {self.identity.name!r} arguments are invalid."
            ) from exc
        return bound


@overload
def task[**P, R](
    function: Callable[P, Awaitable[R]],
    /,
    *,
    name: str | None = None,
    version: int = 1,
    retry: RetryPolicy | None = None,
    visibility_timeout_seconds: float | None = None,
) -> TaskDefinition[P, R]: ...


@overload
def task[**P, R](
    function: None = None,
    /,
    *,
    name: str | None = None,
    version: int = 1,
    retry: RetryPolicy | None = None,
    visibility_timeout_seconds: float | None = None,
) -> Callable[[Callable[P, Awaitable[R]]], TaskDefinition[P, R]]: ...


def task[**P, R](
    function: Callable[P, Awaitable[R]] | None = None,
    /,
    *,
    name: str | None = None,
    version: int = 1,
    retry: RetryPolicy | None = None,
    visibility_timeout_seconds: float | None = None,
) -> TaskDefinition[P, R] | Callable[[Callable[P, Awaitable[R]]], TaskDefinition[P, R]]:
    def declare(
        task_function: Callable[P, Awaitable[R]],
    ) -> TaskDefinition[P, R]:
        if not iscoroutinefunction(task_function):
            raise TaskDeclarationError("A task must wrap an async function.")
        task_signature = signature(task_function)
        _validate_signature(task_signature)
        identity = TaskIdentity(
            name=name or _derived_task_name(task_function),
            version=version,
        )
        definition = TaskDefinition(
            identity=identity,
            retry=retry,
            visibility_timeout_seconds=_visibility_timeout(
                visibility_timeout_seconds,
            ),
            _function=task_function,
            _signature=task_signature,
            _adapters=_argument_adapters(task_function, task_signature),
        )
        register_task_declaration(_declaring_module(task_function), definition)
        return definition

    if function is None:
        return declare
    return declare(function)


def _derived_task_name(function: Callable[..., object]) -> str:
    qualified_name_value = getattr(function, "__qualname__", None)
    if not isinstance(qualified_name_value, str):
        raise TaskDeclarationError("Task callables must have a qualified name.")
    qualified_name = qualified_name_value.replace(".<locals>", "")
    return f"{function.__module__}.{qualified_name}"


def _declaring_module(function: Callable[..., object]) -> str:
    module_name = getattr(function, "__module__", None)
    if not isinstance(module_name, str) or not module_name:
        raise TaskDeclarationError("Task callables must belong to a Python module.")
    return module_name


def _validate_signature(task_signature: Signature) -> None:
    for parameter in task_signature.parameters.values():
        if parameter.kind is Parameter.POSITIONAL_ONLY:
            raise TaskDeclarationError(
                "Task functions cannot declare positional-only parameters."
            )
        if parameter.kind in {
            Parameter.VAR_POSITIONAL,
            Parameter.VAR_KEYWORD,
        }:
            raise TaskDeclarationError(
                "Task functions cannot declare variadic parameters."
            )


def _argument_adapters(
    function: Callable[..., object],
    task_signature: Signature,
) -> dict[str, TypeAdapter[Any]]:
    try:
        annotations = get_type_hints(function)
    except (NameError, TypeError) as exc:
        raise TaskDeclarationError("Task annotations could not be resolved.") from exc
    return {
        name: TypeAdapter(annotations.get(name, Any))
        for name in task_signature.parameters
    }


def _validated_argument(
    parameter: Parameter,
    value: object,
    adapter: TypeAdapter[Any],
) -> object:
    del parameter
    return adapter.validate_python(value)


def _visibility_timeout(value: float | None) -> float | None:
    if value is None:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not isfinite(value)
        or value <= 0
    ):
        raise TaskDeclarationError(
            "Task visibility timeout must be a positive finite number."
        )
    if value < MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS:
        raise TaskDeclarationError(
            "Task visibility timeout must be at least "
            f"{MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS} seconds."
        )
    return float(value)


__all__ = ("TaskDefinition", "task")
