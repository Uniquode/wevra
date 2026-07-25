from __future__ import annotations

from collections.abc import Iterable, Mapping
from importlib import import_module
from typing import Any, ClassVar

from wybra.config.types import (
    ConfigDef,
    ConfigDefinitionError,
    ConfigDiagnostic,
    ConfigSource,
    ConfigSourceError,
    ConfigSourceMetadata,
    ConfigSourceResult,
    LoadedConfig,
    RepeatedConfigSection,
    merge_config_defs,
)
from wybra.core.environment import (
    environment_get,
    environment_is_set,
    runtime_environment,
)

APP_SECTION = "app"
APP_MODULES_KEY = "modules"
MODULE_CONFIG_ATTRIBUTE = "module_config"
type _ResolvedRepeatedSection = tuple[str, RepeatedConfigSection]


class ConfigService:
    _environ: ClassVar[object | None] = None

    def __init__(
        self,
        sources: Iterable[ConfigSource] = (),
        *,
        config_defs: Iterable[ConfigDef] = (),
        discover_module_config: bool = True,
    ) -> None:
        self._sources = tuple(sources)
        self._config_defs = tuple(config_defs)
        self._discover_module_config = discover_module_config
        self._config = self._load_sources()

    @classmethod
    def set_runtime_environment(cls, environ: object) -> None:
        cls._environ = environ

    @classmethod
    def runtime_environment(cls) -> object:
        if cls._environ is None:
            cls._environ = runtime_environment()
        return cls._environ

    @property
    def config(self) -> LoadedConfig:
        return self._config

    @property
    def diagnostics(self) -> tuple[ConfigDiagnostic, ...]:
        return self._config.diagnostics

    @property
    def environ(self) -> object:
        return self.__class__.runtime_environment()

    def get_config(self, section: str) -> Mapping[str, Any] | None:
        return self._config.get_config(section)

    def _load_sources(self) -> LoadedConfig:
        source_values: dict[str, dict[str, Any]] = {}
        value_sources: dict[str, str] = {}
        diagnostics: list[ConfigDiagnostic] = []

        for source in self._sources:
            try:
                result = source.load()
            except ConfigSourceError as exc:
                message = _source_error_message(source.metadata, str(exc))
                if source.metadata.required:
                    raise ConfigSourceError(message) from exc
                diagnostics.append(
                    ConfigDiagnostic(
                        source=source.metadata,
                        message=message,
                        code="source_load_error",
                    )
                )
                continue

            diagnostics.extend(result.diagnostics)
            if _has_error_diagnostic(result):
                message = _first_error_message(result) or "Configuration source failed."
                if source.metadata.required:
                    raise ConfigSourceError(
                        _source_error_message(source.metadata, message)
                    )
                continue

            _merge_values(source_values, value_sources, result.values, source.metadata)

        config_defs = self._resolved_config_defs(source_values)
        values, sources = _apply_config_defs(
            config_defs,
            source_values,
            value_sources,
            self.environ,
        )
        return LoadedConfig(
            values=values,
            sources=sources,
            diagnostics=tuple(diagnostics),
        )

    def _resolved_config_defs(
        self,
        source_values: Mapping[str, Mapping[str, Any]],
    ) -> tuple[ConfigDef, ...]:
        if not self._discover_module_config:
            return self._config_defs
        return (
            *self._config_defs,
            *discover_module_config_defs(_bootstrap_modules(source_values)),
        )


def discover_module_config_defs(module_names: Iterable[str]) -> tuple[ConfigDef, ...]:
    definitions: list[ConfigDef] = []
    for module_name in module_names:
        try:
            module = import_module(module_name)
        except ImportError as exc:
            raise ConfigDefinitionError(
                f"Failed to import module {module_name!r} while discovering "
                f"{MODULE_CONFIG_ATTRIBUTE}."
            ) from exc
        module_config = getattr(module, MODULE_CONFIG_ATTRIBUTE, None)
        if module_config is None:
            continue
        if not isinstance(module_config, ConfigDef):
            raise ConfigDefinitionError(
                f"{module_name}.{MODULE_CONFIG_ATTRIBUTE} must be a ConfigDef."
            )
        definitions.append(module_config)
    return tuple(definitions)


def _bootstrap_modules(
    values: Mapping[str, Mapping[str, Any]],
) -> tuple[str, ...]:
    app_values = values.get(APP_SECTION, {})
    modules = app_values.get(APP_MODULES_KEY, ())
    if modules is None:
        return ()
    if isinstance(modules, str):
        raise ConfigDefinitionError(
            "[app].modules must be a list or tuple of module names."
        )
    if isinstance(modules, (tuple, list)) and all(
        isinstance(module, str) for module in modules
    ):
        return tuple(modules)
    raise ConfigDefinitionError(
        "[app].modules must be a list or tuple of module names."
    )


def _apply_config_defs(
    definitions: tuple[ConfigDef, ...],
    source_values: Mapping[str, Mapping[str, Any]],
    source_index: Mapping[str, str],
    environ: object | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    merged_def = merge_config_defs(definitions)
    repeated_sections = _resolved_repeated_sections(merged_def, source_values)
    values: dict[str, dict[str, Any]] = {
        section: dict(section_values)
        for section, section_values in _default_values(
            merged_def,
            repeated_sections,
        ).items()
    }
    sources: dict[str, str] = {
        f"{section}.{key}": "default"
        for section, section_values in values.items()
        for key in section_values
    }

    _merge_indexed_values(values, sources, source_values, source_index)
    if environ is not None:
        _merge_values(
            values,
            sources,
            _env_values(merged_def, repeated_sections, environ),
            ConfigSourceMetadata(source="environment"),
        )
    _transform_values(merged_def, repeated_sections, values, sources)
    return values, sources


def _default_values(
    definition: ConfigDef,
    repeated_sections: Mapping[str, _ResolvedRepeatedSection],
) -> dict[str, dict[str, Any]]:
    values = {
        section_name: dict(section.defaults)
        for section_name, section in definition.sections.items()
        if section.defaults
    }
    values.update(
        {
            section_name: dict(repeated.group.defaults)
            for section_name, (_, repeated) in repeated_sections.items()
            if repeated.group.defaults
        }
    )
    return values


def _env_values(
    definition: ConfigDef,
    repeated_sections: Mapping[str, _ResolvedRepeatedSection],
    environ: object,
) -> dict[str, dict[str, str]]:
    values: dict[str, dict[str, str]] = {}
    for section_name, section in definition.sections.items():
        for field_name, env_names in section.env.items():
            env_name = next(
                (name for name in env_names if environment_is_set(environ, name)),
                None,
            )
            if env_name is not None:
                env_value = environment_get(environ, env_name)
                if env_value is not None:
                    values.setdefault(section_name, {})[field_name] = env_value
    for section_name, (section_prefix, repeated) in repeated_sections.items():
        prefix = repeated.environment_prefix
        if prefix is None:
            continue
        instance_name = section_name.removeprefix(f"{section_prefix}.")
        for field_name in repeated.environment_fields:
            env_name = f"{prefix}__{instance_name.upper()}__{field_name.upper()}"
            if not environment_is_set(environ, env_name):
                continue
            env_value = environment_get(environ, env_name)
            if env_value is not None:
                values.setdefault(section_name, {})[field_name] = env_value
    return values


def _transform_values(
    definition: ConfigDef,
    repeated_sections: Mapping[str, _ResolvedRepeatedSection],
    values: dict[str, dict[str, Any]],
    sources: Mapping[str, str],
) -> None:
    sections = {
        **definition.sections,
        **{
            section_name: repeated.group
            for section_name, (_, repeated) in repeated_sections.items()
        },
    }
    for section_name, section in sections.items():
        section_values = values.get(section_name)
        if section_values is None:
            continue

        for field_name, field_def in section.field_map.items():
            if field_def.transform is None or field_name not in section_values:
                continue
            try:
                section_values[field_name] = field_def.transform(
                    section_values[field_name]
                )
            except Exception as exc:
                source_name = sources.get(f"{section_name}.{field_name}")
                source_description = (
                    f" (source: {source_name})" if source_name is not None else ""
                )
                raise ConfigSourceError(
                    f"Config value {section_name}.{field_name} is invalid: "
                    f"{exc}{source_description}"
                ) from exc


def _resolved_repeated_sections(
    definition: ConfigDef,
    source_values: Mapping[str, Mapping[str, Any]],
) -> dict[str, _ResolvedRepeatedSection]:
    resolved: dict[str, _ResolvedRepeatedSection] = {}
    repeated_definitions = sorted(
        definition.repeated_sections.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    )
    for section_name in source_values:
        if section_name in definition.sections:
            continue
        for prefix, repeated in repeated_definitions:
            marker = f"{prefix}."
            if not section_name.startswith(marker):
                continue
            instance_name = section_name.removeprefix(marker)
            validator = repeated.name_validator
            if validator is not None:
                try:
                    validator(instance_name)
                except (TypeError, ValueError) as exc:
                    raise ConfigSourceError(
                        f"Config section {section_name!r} is invalid: {exc}"
                    ) from exc
            resolved[section_name] = (prefix, repeated)
            break
    return resolved


def _merge_values(
    target: dict[str, dict[str, Any]],
    source_index: dict[str, str],
    values: Mapping[str, Mapping[str, Any]],
    metadata: ConfigSourceMetadata,
) -> None:
    for section, section_values in values.items():
        target_section = target.setdefault(section, {})
        target_section.update(section_values)
        for key in section_values:
            source_index[f"{section}.{key}"] = metadata.source


def _merge_indexed_values(
    target: dict[str, dict[str, Any]],
    target_index: dict[str, str],
    values: Mapping[str, Mapping[str, Any]],
    source_index: Mapping[str, str],
    default_source: str = "source",
) -> None:
    for section, section_values in values.items():
        target_section = target.setdefault(section, {})
        target_section.update(section_values)
        for key in section_values:
            index_key = f"{section}.{key}"
            target_index[index_key] = source_index.get(index_key, default_source)


def _has_error_diagnostic(result: ConfigSourceResult) -> bool:
    return any(diagnostic.severity == "error" for diagnostic in result.diagnostics)


def _first_error_message(result: ConfigSourceResult) -> str | None:
    return next(
        (
            diagnostic.message
            for diagnostic in result.diagnostics
            if diagnostic.severity == "error"
        ),
        None,
    )


def _source_error_message(metadata: ConfigSourceMetadata, message: str) -> str:
    return f"{metadata.source}: {message}" if message else metadata.source
