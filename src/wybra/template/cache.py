"""Jinja support for cacheable rendered fragments."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast
from weakref import WeakKeyDictionary

from jinja2 import Environment, nodes
from jinja2.environment import Template
from jinja2.exceptions import TemplateNotFound, TemplateRuntimeError
from jinja2.ext import Extension
from jinja2.runtime import Context

from wybra.cache import (
    MAX_CACHE_VALUE_BYTES,
    CacheCapability,
    UncachedCacheValue,
    validate_cache_ttl,
)

FRAGMENT_CACHE_OWNER = "template.fragment"
type CacheProvider = Callable[[], Awaitable[CacheCapability | None]]
type FragmentCaller = Callable[[], Awaitable[str]]
type CacheKeyNormaliser = Callable[[object], object]
type CacheKeyNormalisers = Mapping[type[object], CacheKeyNormaliser]


class CacheExtension(Extension):
    """Render a block through the optional Wybra cache capability."""

    tags = {"cache"}

    def __init__(self, environment: Environment) -> None:
        super().__init__(environment)
        self.cache_provider: CacheProvider | None = None
        self.cache_key_normalisers: dict[type[object], CacheKeyNormaliser] = {}
        self._template_fingerprints: WeakKeyDictionary[Template, str] = (
            WeakKeyDictionary()
        )
        cast(dict[str, Any], environment.globals)["cache_key"] = self.cache_key

    def cache_key(
        self,
        *values: object,
        **conditions: object,
    ) -> object:
        """Return a canonical variation value for use with ``vary_by``."""
        return _canonical_value(
            {"values": values, "conditions": conditions},
            self.cache_key_normalisers,
        )

    def parse(self, parser: Any) -> nodes.Node:
        lineno = next(parser.stream).lineno
        fragment_name = parser.parse_expression()
        parser.stream.expect("name:ttl")
        parser.stream.expect("assign")
        ttl = parser.parse_expression()
        vary_by: nodes.Expr = nodes.Tuple([], "load")
        if parser.stream.skip_if("name:vary_by"):
            parser.stream.expect("assign")
            vary_by = parser.parse_expression()
        body = parser.parse_statements(("name:endcache",), drop_needle=True)
        return nodes.CallBlock(
            self.call_method(
                "_render_fragment",
                [nodes.ContextReference(), fragment_name, ttl, vary_by],
            ),
            [],
            [],
            body,
        ).set_lineno(lineno)

    async def _render_fragment(
        self,
        context: Context,
        fragment_name: object,
        ttl: object,
        vary_by: object,
        caller: FragmentCaller,
    ) -> str:
        if not isinstance(fragment_name, str) or not fragment_name:
            raise TemplateRuntimeError(
                "Cache fragment names must be non-empty strings."
            )
        try:
            cache_ttl = validate_cache_ttl(ttl)
        except ValueError as exc:
            raise TemplateRuntimeError(str(exc)) from None

        provider = self.cache_provider
        cache = await provider() if provider is not None else None
        if cache is None:
            return await caller()

        key = self._fragment_key(context, fragment_name, vary_by)

        async def render_fragment() -> bytes | UncachedCacheValue:
            rendered = await caller()
            value = rendered.encode("utf-8")
            if len(value) > MAX_CACHE_VALUE_BYTES:
                return UncachedCacheValue(value)
            return value

        cached_fragment = await cache.get_or_set(
            FRAGMENT_CACHE_OWNER,
            key,
            ttl=cache_ttl,
            factory=render_fragment,
        )
        return cached_fragment.decode("utf-8")

    def _fragment_key(
        self,
        context: Context,
        fragment_name: str,
        vary_by: object,
    ) -> str:
        try:
            variation = json.dumps(
                _canonical_value(vary_by, self.cache_key_normalisers),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            )
        except TypeError as exc:
            raise TemplateRuntimeError(
                "Cache fragment vary_by values must be JSON-compatible or use "
                "cache_key() with a registered cache-key normaliser."
            ) from exc
        variation_fingerprint = hashlib.sha256(variation.encode("utf-8")).hexdigest()
        return ":".join(
            (
                self._template_fingerprint(context.environment, context.name),
                fragment_name,
                variation_fingerprint,
            )
        )

    def _template_fingerprint(
        self,
        environment: Environment,
        template_name: str | None,
    ) -> str:
        if template_name is None:
            return _hash_template_source("<inline>", "<inline>")

        template = environment.get_template(template_name)
        fingerprint = self._template_fingerprints.get(template)
        if fingerprint is not None:
            return fingerprint

        source = template_name
        if environment.loader is not None:
            try:
                source, _, _ = environment.loader.get_source(environment, template_name)
            except TemplateNotFound:
                pass
        fingerprint = _hash_template_source(template_name, source)
        self._template_fingerprints[template] = fingerprint
        return fingerprint


def configure_cache_extension(
    environment: Environment,
    cache_provider: CacheProvider | None,
    *,
    cache_key_normalisers: CacheKeyNormalisers | None = None,
) -> None:
    extension = environment.extensions.get(CacheExtension.identifier)
    if not isinstance(extension, CacheExtension):
        raise RuntimeError("Jinja cache extension is not registered.")
    extension.cache_provider = cache_provider
    extension.cache_key_normalisers = _validate_normalisers(cache_key_normalisers)


def _hash_template_source(identity: str, source: str) -> str:
    return hashlib.sha256(f"{identity}\0{source}".encode()).hexdigest()


def _canonical_value(
    value: object,
    normalisers: CacheKeyNormalisers,
) -> object:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise TypeError("Cache key floats must be finite.")
        return value
    if isinstance(value, Mapping):
        canonical_mapping: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("Cache key mapping keys must be strings.")
            canonical_mapping[key] = _canonical_value(item, normalisers)
        return canonical_mapping
    if isinstance(value, list | tuple):
        return [_canonical_value(item, normalisers) for item in value]
    if isinstance(value, set | frozenset):
        canonical_values = [_canonical_value(item, normalisers) for item in value]
        return sorted(
            canonical_values,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ),
        )

    normaliser = _normaliser_for(value, normalisers)
    if normaliser is None:
        raise TypeError(f"Unsupported cache-key value type: {type(value).__name__}.")
    normalised = normaliser(value)
    if normalised is value:
        raise TypeError("Cache-key normalisers must return a different value.")
    return _canonical_value(normalised, normalisers)


def _normaliser_for(
    value: object,
    normalisers: CacheKeyNormalisers,
) -> CacheKeyNormaliser | None:
    for value_type, normaliser in normalisers.items():
        if isinstance(value, value_type):
            return normaliser
    return None


def _validate_normalisers(
    normalisers: CacheKeyNormalisers | None,
) -> dict[type[object], CacheKeyNormaliser]:
    if normalisers is None:
        return {}
    validated: dict[type[object], CacheKeyNormaliser] = {}
    for value_type, normaliser in normalisers.items():
        if not isinstance(value_type, type) or not callable(normaliser):
            raise TypeError(
                "Cache-key normalisers must map types to callable normalisers."
            )
        validated[value_type] = normaliser
    return validated


__all__ = (
    "CacheExtension",
    "CacheKeyNormaliser",
    "CacheKeyNormalisers",
    "CacheProvider",
    "FRAGMENT_CACHE_OWNER",
    "configure_cache_extension",
)
