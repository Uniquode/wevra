"""Reusable secret-safe JSON metadata canonicalisation."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Final, cast

SENSITIVE_ATTRIBUTE_PARTS: Final = (
    "authorisation",
    "authorization",
    "cookie",
    "credential",
    "csrf",
    "password",
    "secret",
    "session",
    "token",
)
SENSITIVE_KEY_PARTS: Final = frozenset(
    {
        ("access", "key"),
        ("api", "key"),
        ("private", "key"),
        ("secret", "key"),
        ("signing", "key"),
    }
)
SENSITIVE_COMPACT_NAMES: Final = frozenset(
    compact_name
    for parts in SENSITIVE_KEY_PARTS
    for compact_name in (
        "".join(parts),
        "".join((*parts[:-1], f"{parts[-1]}s")),
    )
)
SENSITIVE_PLURAL_ATTRIBUTE_PARTS: Final = frozenset(
    f"{part}s" for part in SENSITIVE_ATTRIBUTE_PARTS if part != "csrf"
)
NON_SENSITIVE_METRIC_SUFFIXES: Final = frozenset(
    {
        "changed",
        "checked",
        "count",
        "created",
        "deleted",
        "failed",
        "migrated",
        "processed",
        "remaining",
        "succeeded",
        "total",
    }
)
MAX_SAFE_STRING_LENGTH: Final = 500
MAX_SAFE_METADATA_DEPTH: Final = 8
MAX_SAFE_METADATA_ITEMS: Final = 128
MAX_SAFE_METADATA_BYTES: Final = 16_384
_CAMEL_CASE_BOUNDARY: Final = re.compile(
    r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])"
)
_ATTRIBUTE_PART_SEPARATOR: Final = re.compile(r"[^A-Za-z0-9]+")


@dataclass(frozen=True, slots=True, init=False, eq=False)
class SafeJsonMetadata(Mapping[str, object]):
    """Immutable canonical secret-safe JSON metadata."""

    _encoded: bytes = field(repr=False)

    def __init__(self, values: Mapping[str, object]) -> None:
        if not isinstance(values, Mapping) or any(
            not isinstance(name, str) for name in values
        ):
            raise ValueError("metadata must be a mapping with string keys.")
        item_count = [0]
        safe = _safe_mapping(values, depth=0, item_count=item_count)
        encoded = json.dumps(
            safe,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(encoded) > MAX_SAFE_METADATA_BYTES:
            raise ValueError("metadata exceeds the safe encoded size.")
        object.__setattr__(self, "_encoded", encoded)

    def __getitem__(self, key: str) -> object:
        return self._decoded()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._decoded())

    def __len__(self) -> int:
        return len(self._decoded())

    def __hash__(self) -> int:
        return hash(self._encoded)

    def __repr__(self) -> str:
        return "<safe JSON metadata>"

    def to_json(self) -> bytes:
        return self._encoded

    def _decoded(self) -> dict[str, object]:
        decoded = json.loads(self._encoded)
        if not isinstance(decoded, dict):  # pragma: no cover
            raise ValueError("metadata must decode to a mapping.")
        return decoded


def safe_json_metadata(values: Mapping[str, object]) -> SafeJsonMetadata:
    """Return bounded immutable JSON metadata with sensitive values redacted."""

    if isinstance(values, SafeJsonMetadata):
        return values
    return SafeJsonMetadata(values)


def is_sensitive_attribute_name(key: str) -> bool:
    """Return whether an attribute name indicates secret-bearing content."""

    parts = _attribute_name_parts(key)
    for index, part in enumerate(parts):
        without_numeric_suffix = part.rstrip("0123456789")
        if without_numeric_suffix in SENSITIVE_ATTRIBUTE_PARTS:
            return True
        if part in SENSITIVE_PLURAL_ATTRIBUTE_PARTS and not _is_plural_metric(
            parts,
            index,
        ):
            return True
    if any(
        _matches_sensitive_key_parts(
            parts[index : index + len(sensitive_parts)],
            sensitive_parts,
        )
        for sensitive_parts in SENSITIVE_KEY_PARTS
        for index in range(len(parts) - len(sensitive_parts) + 1)
    ):
        return True
    return "".join(parts) in SENSITIVE_COMPACT_NAMES


def truncate_safe_string(
    value: str,
    *,
    maximum_length: int = MAX_SAFE_STRING_LENGTH,
) -> str:
    """Return a string truncated within the requested maximum length."""

    if len(value) <= maximum_length:
        return value
    if maximum_length <= 3:
        return "." * maximum_length
    return f"{value[: maximum_length - 3]}..."


def _safe_mapping(
    values: Mapping[str, object],
    *,
    depth: int,
    item_count: list[int],
) -> dict[str, object]:
    _check_depth(depth)
    safe: dict[str, object] = {}
    for key, value in values.items():
        _count_item(item_count)
        safe[key] = (
            "[redacted]"
            if is_sensitive_attribute_name(key)
            else _safe_value(value, depth=depth + 1, item_count=item_count)
        )
    return safe


def _safe_value(
    value: object,
    *,
    depth: int,
    item_count: list[int],
) -> object:
    _check_depth(depth)
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("metadata numbers must be finite.")
        return value
    if isinstance(value, str):
        if len(value) > MAX_SAFE_STRING_LENGTH:
            raise ValueError("metadata strings exceed the safe length.")
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(name, str) for name in value):
            raise ValueError("nested metadata mappings require string keys.")
        return _safe_mapping(
            cast("Mapping[str, object]", value),
            depth=depth,
            item_count=item_count,
        )
    if isinstance(value, (list, tuple)):
        safe_items: list[object] = []
        for item in value:
            _count_item(item_count)
            safe_items.append(_safe_value(item, depth=depth + 1, item_count=item_count))
        return safe_items
    raise ValueError(f"metadata values cannot include {type(value).__name__}.")


def _check_depth(depth: int) -> None:
    if depth > MAX_SAFE_METADATA_DEPTH:
        raise ValueError("metadata exceeds the safe nesting depth.")


def _count_item(item_count: list[int]) -> None:
    item_count[0] += 1
    if item_count[0] > MAX_SAFE_METADATA_ITEMS:
        raise ValueError("metadata exceeds the safe item count.")


def _attribute_name_parts(key: str) -> tuple[str, ...]:
    separated = _CAMEL_CASE_BOUNDARY.sub("_", key)
    return tuple(
        part.lower() for part in _ATTRIBUTE_PART_SEPARATOR.split(separated) if part
    )


def _is_plural_metric(parts: tuple[str, ...], index: int) -> bool:
    suffix = parts[index + 1 :]
    return bool(suffix) and all(
        part in NON_SENSITIVE_METRIC_SUFFIXES for part in suffix
    )


def _matches_sensitive_key_parts(
    candidate: tuple[str, ...],
    sensitive_parts: tuple[str, ...],
) -> bool:
    return candidate[:-1] == sensitive_parts[:-1] and candidate[-1] in {
        sensitive_parts[-1],
        f"{sensitive_parts[-1]}s",
    }


__all__ = (
    "MAX_SAFE_METADATA_BYTES",
    "MAX_SAFE_METADATA_DEPTH",
    "MAX_SAFE_METADATA_ITEMS",
    "MAX_SAFE_STRING_LENGTH",
    "SENSITIVE_ATTRIBUTE_PARTS",
    "SENSITIVE_COMPACT_NAMES",
    "SENSITIVE_KEY_PARTS",
    "SafeJsonMetadata",
    "is_sensitive_attribute_name",
    "safe_json_metadata",
    "truncate_safe_string",
)
