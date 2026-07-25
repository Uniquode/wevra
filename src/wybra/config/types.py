from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Protocol

ConfigDiagnosticSeverity = Literal["error", "warning", "info"]
type ConfigTransform = Callable[[Any], Any]
type ConfigNameValidator = Callable[[str], None]
type EnvOverride = str | tuple[str, ...]


class _NoDefault:
    pass


NO_DEFAULT = _NoDefault()


class ConfigError(RuntimeError):
    """Raised when configuration service operations fail."""


class ConfigSourceError(ConfigError):
    """Raised when a required configuration source cannot load."""


class ConfigDefinitionError(ConfigError):
    """Raised when a module config definition is invalid."""


@dataclass(frozen=True, slots=True)
class ConfigSourceMetadata:
    source: str
    required: bool = True


@dataclass(frozen=True, slots=True)
class ConfigSourceLocation:
    file: Path | None = None
    line: int | None = None
    column: int | None = None


@dataclass(frozen=True, slots=True)
class ConfigDiagnostic:
    source: ConfigSourceMetadata
    message: str
    severity: ConfigDiagnosticSeverity = "error"
    code: str | None = None
    location: ConfigSourceLocation | None = None


@dataclass(frozen=True, slots=True)
class ConfigField:
    name: str
    default: Any = NO_DEFAULT
    env: tuple[str, ...] = ()
    transform: ConfigTransform | None = None

    def __init__(
        self,
        *,
        name: str,
        default: Any = NO_DEFAULT,
        env: EnvOverride = (),
        transform: ConfigTransform | None = None,
    ) -> None:
        field_name = name.strip()
        if not field_name:
            raise ConfigDefinitionError("Config field name must not be blank.")
        object.__setattr__(self, "name", field_name)
        object.__setattr__(self, "default", default)
        object.__setattr__(self, "env", _normalise_env_override(env))
        object.__setattr__(self, "transform", transform)

    @property
    def has_default(self) -> bool:
        return self.default is not NO_DEFAULT


@dataclass(frozen=True, slots=True)
class ConfigGroup:
    fields: tuple[ConfigField, ...] = ()
    field_map: Mapping[str, ConfigField] = field(default_factory=dict)

    def __init__(
        self,
        *,
        fields: tuple[ConfigField, ...] = (),
    ) -> None:
        field_map = _field_map(fields)
        object.__setattr__(self, "fields", tuple(fields))
        object.__setattr__(self, "field_map", MappingProxyType(field_map))

    @property
    def field_names(self) -> frozenset[str]:
        return frozenset(self.field_map)

    @property
    def defaults(self) -> Mapping[str, Any]:
        return MappingProxyType(
            {field.name: field.default for field in self.fields if field.has_default}
        )

    @property
    def env(self) -> Mapping[str, tuple[str, ...]]:
        return MappingProxyType(
            {field.name: field.env for field in self.fields if field.env}
        )


def _field_map(fields: tuple[ConfigField, ...]) -> dict[str, ConfigField]:
    values: dict[str, ConfigField] = {}
    duplicates: set[str] = set()
    for field_def in fields:
        if not isinstance(field_def, ConfigField):
            raise ConfigDefinitionError(
                "ConfigGroup fields must be ConfigField instances."
            )
        if field_def.name in values:
            duplicates.add(field_def.name)
        values[field_def.name] = field_def
    if duplicates:
        raise ConfigDefinitionError(
            "Config fields contain duplicate names: " + ", ".join(sorted(duplicates))
        )
    return values


@dataclass(frozen=True, slots=True)
class RepeatedConfigSection:
    """Apply one field schema to configured ``prefix.<name>`` sections."""

    group: ConfigGroup
    environment_prefix: str | None = None
    environment_fields: frozenset[str] = frozenset()
    name_validator: ConfigNameValidator | None = None

    def __init__(
        self,
        *,
        group: ConfigGroup,
        environment_prefix: str | None = None,
        environment_fields: tuple[str, ...] = (),
        name_validator: ConfigNameValidator | None = None,
    ) -> None:
        if not isinstance(group, ConfigGroup):
            raise ConfigDefinitionError(
                "RepeatedConfigSection group must be a ConfigGroup."
            )
        prefix = environment_prefix.strip() if environment_prefix is not None else None
        if environment_prefix is not None:
            if not prefix:
                raise ConfigDefinitionError(
                    "RepeatedConfigSection environment prefix must not be blank."
                )
            if environment_prefix != prefix:
                raise ConfigDefinitionError(
                    "RepeatedConfigSection environment prefix must not contain "
                    "surrounding whitespace."
                )
        fields = frozenset(environment_fields)
        unknown_fields = fields - group.field_names
        if unknown_fields:
            raise ConfigDefinitionError(
                "RepeatedConfigSection environment fields are not declared: "
                + ", ".join(sorted(unknown_fields))
            )
        if name_validator is not None and not callable(name_validator):
            raise ConfigDefinitionError(
                "RepeatedConfigSection name validator must be callable."
            )
        object.__setattr__(self, "group", group)
        object.__setattr__(self, "environment_prefix", prefix)
        object.__setattr__(self, "environment_fields", fields)
        object.__setattr__(self, "name_validator", name_validator)


@dataclass(frozen=True, slots=True)
class ConfigDef:
    sections: Mapping[str, ConfigGroup]
    repeated_sections: Mapping[str, RepeatedConfigSection]

    def __init__(
        self,
        sections: Mapping[str, ConfigGroup],
        *,
        repeated_sections: Mapping[str, RepeatedConfigSection] | None = None,
    ) -> None:
        object.__setattr__(self, "sections", MappingProxyType(dict(sections)))
        object.__setattr__(
            self,
            "repeated_sections",
            MappingProxyType(dict(repeated_sections or {})),
        )


@dataclass(frozen=True, slots=True)
class LoadedConfig:
    """Loaded configuration with immutable mapping containers.

    Config values are intentionally preserved as raw source values. Nested
    mutable values are not coerced into different container types by this layer.
    """

    values: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    sources: Mapping[str, str] = field(default_factory=dict)
    diagnostics: tuple[ConfigDiagnostic, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "values", _freeze_nested_mapping(self.values))
        object.__setattr__(self, "sources", MappingProxyType(dict(self.sources)))

    def get_config(self, section: str) -> Mapping[str, Any] | None:
        return self.values.get(section)


@dataclass(frozen=True, slots=True)
class ConfigSourceResult:
    values: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    diagnostics: tuple[ConfigDiagnostic, ...] = ()


class ConfigSource(Protocol):
    @property
    def metadata(self) -> ConfigSourceMetadata: ...

    def load(self) -> ConfigSourceResult: ...


def merge_config_defs(definitions: tuple[ConfigDef, ...]) -> ConfigDef:
    sections: dict[str, ConfigGroup] = {}
    repeated_sections: dict[str, RepeatedConfigSection] = {}
    for definition in definitions:
        for section_name, section in definition.sections.items():
            existing = sections.get(section_name)
            if existing is None:
                sections[section_name] = section
                continue

            merged_fields = _merge_fields(section_name, existing, section)
            sections[section_name] = ConfigGroup(fields=merged_fields)
        for section_name, repeated in definition.repeated_sections.items():
            existing_repeated = repeated_sections.get(section_name)
            if existing_repeated is None:
                repeated_sections[section_name] = repeated
                continue
            if existing_repeated != repeated:
                raise ConfigDefinitionError(
                    f"Conflicting repeated section definition for {section_name}."
                )
    return ConfigDef(sections, repeated_sections=repeated_sections)


def _merge_fields(
    section_name: str,
    first: ConfigGroup,
    second: ConfigGroup,
) -> tuple[ConfigField, ...]:
    merged = dict(first.field_map)
    for field_name, field_def in second.field_map.items():
        existing = merged.get(field_name)
        if existing is None:
            merged[field_name] = field_def
            continue
        if existing != field_def:
            raise ConfigDefinitionError(
                f"Conflicting definition for field {section_name}.{field_name}."
            )
    return tuple(merged.values())


def _normalise_env_override(value: EnvOverride) -> tuple[str, ...]:
    if isinstance(value, tuple):
        return value
    return (value,)


def _freeze_nested_mapping(
    values: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Mapping[str, Any]]:
    """Freeze section mappings while preserving raw configured values.

    Only the top two mapping levels are frozen. Nested mutable values, such as
    lists or dicts stored inside sections, are not copied or frozen by this raw
    configuration layer and therefore remain shared and mutable.
    """

    return MappingProxyType(
        {
            section: MappingProxyType(dict(section_values))
            for section, section_values in values.items()
        }
    )
