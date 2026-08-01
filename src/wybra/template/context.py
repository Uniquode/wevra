"""Template context provider composition support."""

from __future__ import annotations

import inspect
from collections import ChainMap
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any, cast

from wybra.core.composition import CompositionError
from wybra.core.diagnostics import diagnostic_message


class TemplateContext(ChainMap[str, Any]):
    def __init__(self, *maps: Mapping[str, Any]) -> None:
        layers = maps or ({},)
        super().__init__(*(_readonly_layer(mapping) for mapping in layers))

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> TemplateContext:
        return cls.from_layers(values)

    @classmethod
    def from_layers(cls, *layers: Mapping[str, Any]) -> TemplateContext:
        return cls(*layers)

    def with_values(self, **kwargs: Any) -> TemplateContext:
        return self.with_layer(kwargs)

    def with_layer(self, values: Mapping[str, Any]) -> TemplateContext:
        return TemplateContext(values, *self.maps)

    def as_dict(self) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for mapping in reversed(self.maps):
            values.update(mapping)
        return values

    def __getitem__(self, key: str) -> Any:
        return super().__getitem__(key)

    def __iter__(self) -> Iterator[str]:
        seen: set[str] = set()
        for mapping in self.maps:
            for key in mapping:
                if key not in seen:
                    seen.add(key)
                    yield key

    def __len__(self) -> int:
        return len(set().union(*self.maps))


type ContextResult = TemplateContext | Awaitable[TemplateContext]
type ContextProvider = Callable[[Any, TemplateContext], ContextResult]
type ContextProviderRegistration = ContextProvider | Mapping[str, Any]

_CONTEXT_PROVIDERS: dict[str, list[ContextProvider]] = {}
REQUEST_CONTEXT_STATE_ATTRIBUTE = "wybra_template_context"


class ContextProviderError(CompositionError):
    """Raised when a context provider cannot be registered."""


def add_to_context(
    provider: ContextProviderRegistration,
    *,
    module_name: str | None = None,
) -> ContextProvider:
    """Register a provider for the context module that calls this function."""
    registered_provider = _normalise_provider(provider)
    provider_module_name = (
        module_name
        if module_name is not None
        else _provider_module_name(provider, registered_provider)
    )
    _CONTEXT_PROVIDERS.setdefault(provider_module_name, []).append(registered_provider)
    return registered_provider


def _normalise_provider(provider: ContextProviderRegistration) -> ContextProvider:
    if isinstance(provider, Mapping):
        context_values = dict(provider)

        def static_context(_request: Any, context: TemplateContext) -> TemplateContext:
            return context.with_layer(context_values)

        return static_context

    if not callable(provider):
        raise ContextProviderError("Context provider must be callable.")

    return provider


def get_context_providers(module_name: str) -> tuple[ContextProvider, ...]:
    return tuple(_CONTEXT_PROVIDERS.get(module_name, ()))


def validate_context_providers(
    providers: Iterable[ContextProvider],
) -> tuple[ContextProvider, ...]:
    validated_providers = tuple(providers)
    for provider in validated_providers:
        if not callable(provider):
            raise ContextProviderError("Context provider must be callable.")

    return validated_providers


async def resolve_context_providers(
    providers: Iterable[ContextProvider],
    request: Any,
    *,
    initial_context: TemplateContext | None = None,
) -> TemplateContext:
    context = initial_context if initial_context is not None else TemplateContext()
    for provider in providers:
        context = await _call_provider(provider, request, context)

    return context


def set_request_context(request: Any, context: TemplateContext) -> None:
    setattr(request.state, REQUEST_CONTEXT_STATE_ATTRIBUTE, context)


def get_request_context(request: Any) -> dict[str, Any]:
    context = getattr(request.state, REQUEST_CONTEXT_STATE_ATTRIBUTE, None)
    if context is None:
        return {}
    if isinstance(context, TemplateContext):
        return context.as_dict()

    raise ContextProviderError(
        "Stored request template context must be a TemplateContext; wrap raw "
        "mapping values with TemplateContext.from_mapping(...)."
    )


def clear_context_providers(module_name: str | None = None) -> None:
    if module_name is None:
        _CONTEXT_PROVIDERS.clear()
        return

    _CONTEXT_PROVIDERS.pop(module_name, None)


def _calling_module_name(frame_depth: int = 2) -> str:
    frame = inspect.currentframe()
    provider_frame = frame
    for _ in range(frame_depth):
        provider_frame = provider_frame.f_back if provider_frame is not None else None
    module_name = (
        provider_frame.f_globals.get("__name__") if provider_frame is not None else None
    )
    if not isinstance(module_name, str) or not module_name:
        raise ContextProviderError("Context provider module could not be resolved.")

    return module_name


def _provider_module_name(
    provider: ContextProviderRegistration,
    registered_provider: ContextProvider,
) -> str:
    if isinstance(provider, Mapping):
        return _calling_module_name(frame_depth=3)

    if not isinstance(provider, Mapping):
        module_name = getattr(provider, "__module__", None)
        if isinstance(module_name, str) and module_name:
            return module_name

    module_name = getattr(registered_provider, "__module__", None)
    if isinstance(module_name, str) and module_name != __name__:
        return module_name

    return _calling_module_name(frame_depth=3)


async def _call_provider(
    provider: ContextProvider,
    request: Any,
    context: TemplateContext,
) -> TemplateContext:
    provider_context = provider(request, context)
    if inspect.isawaitable(provider_context):
        provider_context = await provider_context
    if not isinstance(provider_context, TemplateContext):
        raise ContextProviderError(
            diagnostic_message(
                f"Context provider {_provider_name(provider)}",
                "must return a TemplateContext.",
            )
        )

    for key, _value in provider_context.items():
        if not isinstance(key, str):
            raise ContextProviderError(
                diagnostic_message(
                    f"Context provider {_provider_name(provider)}",
                    "returned a non-string template context key.",
                )
            )

    return provider_context


def _provider_name(provider: ContextProvider) -> str:
    provider_module = getattr(provider, "__module__", "")
    provider_name = getattr(provider, "__qualname__", repr(provider))
    if provider_module:
        return f"{provider_module}.{provider_name}"

    return str(provider_name)


def _readonly_layer(values: Mapping[str, Any]) -> dict[str, Any]:
    return cast(dict[str, Any], MappingProxyType(dict(values)))


__all__ = [
    "ContextProvider",
    "ContextProviderError",
    "ContextProviderRegistration",
    "ContextResult",
    "REQUEST_CONTEXT_STATE_ATTRIBUTE",
    "TemplateContext",
    "add_to_context",
    "clear_context_providers",
    "get_context_providers",
    "get_request_context",
    "resolve_context_providers",
    "set_request_context",
    "validate_context_providers",
]
