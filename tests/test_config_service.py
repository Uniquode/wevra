from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

from wybra.config import (
    AppConfigSource,
    BaseSettings,
    ConfigDef,
    ConfigDefinitionError,
    ConfigDiagnostic,
    ConfigField,
    ConfigGroup,
    ConfigService,
    ConfigSourceError,
    ConfigSourceMetadata,
    ConfigSourceResult,
    CredentialReference,
    EnvironmentConfigSource,
    FileConfigSource,
    MappingConfigSource,
    RepeatedConfigSection,
    to_bool,
    to_path,
)
from wybra.core.composition import (
    AppConfig,
    AssetExportMode,
    AssetOptions,
    RouteOptions,
    TemplateOptions,
    load_app_config,
    raw_config_sections,
)
from wybra.core.settings import EnvironmentSetting, EnvironmentValueType
from wybra.security import CorsPolicy, CorsPolicySet


class FailingSource:
    def __init__(self, *, source: str = "failing", required: bool = True) -> None:
        self._metadata = ConfigSourceMetadata(source=source, required=required)

    @property
    def metadata(self) -> ConfigSourceMetadata:
        return self._metadata

    def load(self) -> ConfigSourceResult:
        return ConfigSourceResult(
            diagnostics=(
                ConfigDiagnostic(
                    source=self.metadata,
                    message="source failed",
                    code="source_failed",
                ),
            )
        )


class RaisingSource:
    def __init__(self, *, source: str = "raising", required: bool = False) -> None:
        self._metadata = ConfigSourceMetadata(source=source, required=required)

    @property
    def metadata(self) -> ConfigSourceMetadata:
        return self._metadata

    def load(self) -> ConfigSourceResult:
        raise ConfigSourceError("boom")


def test_config_service_loads_required_sources() -> None:
    service = ConfigService(
        [MappingConfigSource({"identity": {"totp_mode": "required"}})]
    )

    config = service.get_config("identity")

    assert config is not None
    assert config["totp_mode"] == "required"


@dataclass(frozen=True, slots=True)
class EmptyCredentialSettings(BaseSettings):
    module_config = ConfigDef({"empty": ConfigGroup()})


def test_base_settings_credential_references_defaults_to_empty_tuple() -> None:
    references = EmptyCredentialSettings().credential_references()

    assert references == ()
    assert isinstance(references, tuple)
    assert all(isinstance(reference, CredentialReference) for reference in references)


def test_raw_config_sections_flattens_runtime_module_subsections() -> None:
    sections = raw_config_sections(
        {
            "app": {
                "modules": ["wybra.secrets", "wybra.auth"],
                "assets": {
                    "url_path": "/static",
                    "cors": {"allow_origins": ["https://example.test"]},
                },
            },
            "auth": {
                "session_cookie_name": "session",
                "providers": {"google": {"secrets": "keychain"}},
            },
            "secrets": {
                "crypto": {"source": "keychain"},
                "keychain": {"appname": "wybra"},
            },
            "cache": {
                "backend": "redis",
                "url": "redis://cache/0",
                "session": {"backend": "memory"},
            },
            "cache.tasks": {"backend": "memory"},
        }
    )

    assert sections["app"] == {"modules": ["wybra.secrets", "wybra.auth"]}
    assert sections["app.assets"] == {
        "url_path": "/static",
        "cors": {"allow_origins": ["https://example.test"]},
    }
    assert sections["app.assets.cors"] == {"allow_origins": ["https://example.test"]}
    assert sections["auth"] == {"session_cookie_name": "session"}
    assert sections["auth.providers"] == {"google": {"secrets": "keychain"}}
    assert sections["secrets.crypto"] == {"source": "keychain"}
    assert sections["secrets.keychain"] == {"appname": "wybra"}
    assert sections["cache"] == {
        "backend": "redis",
        "url": "redis://cache/0",
    }
    assert sections["cache.session"] == {"backend": "memory"}
    assert sections["cache.tasks"] == {"backend": "memory"}


def test_load_app_config_preserves_auth_policy_and_flattens_auth_providers(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "app.toml"
    config_path.write_text(
        """
[app]
modules = ["wybra.auth", "wybra.providers"]

[app.templates]
auto_reload = true
cache_size = 0

[app.assets]
url_path = "/static/"

[auth.password.policy]
minimum_length = 8

[auth.providers.google]
enabled = true
client_id = "client-id"
""".strip(),
        encoding="utf-8",
    )

    app_config = load_app_config(project_root=tmp_path, config_path=config_path)

    assert app_config.auth == {
        "password": {"policy": {"minimum_length": 8}},
    }
    assert app_config.raw_config["auth.providers"] == {
        "google": {"enabled": True, "client_id": "client-id"},
    }


def test_required_source_failure_fails_loading() -> None:
    with pytest.raises(ConfigSourceError, match="source failed"):
        ConfigService([FailingSource()])


def test_optional_source_failure_records_diagnostic() -> None:
    service = ConfigService(
        [
            MappingConfigSource({"app": {"name": "uniquode"}}),
            FailingSource(source="optional", required=False),
        ]
    )

    assert service.get_config("app") == {"name": "uniquode"}
    assert len(service.diagnostics) == 1
    assert service.diagnostics[0].code == "source_failed"


def test_optional_source_load_error_records_diagnostic() -> None:
    service = ConfigService(
        [
            MappingConfigSource({"app": {"name": "uniquode"}}),
            RaisingSource(source="raising", required=False),
        ]
    )

    assert service.get_config("app") == {"name": "uniquode"}
    assert len(service.diagnostics) == 1
    diagnostic = service.diagnostics[0]
    assert diagnostic.code == "source_load_error"
    assert diagnostic.message == "raising: boom"


def test_config_lookup_returns_none_for_missing_section() -> None:
    service = ConfigService([MappingConfigSource({"app": {"name": "uniquode"}})])

    assert service.get_config("missing") is None


def test_loaded_config_is_immutable() -> None:
    service = ConfigService([MappingConfigSource({"app": {"name": "uniquode"}})])
    config = service.get_config("app")

    assert config is not None
    mutable_config = cast(Any, config)
    with pytest.raises(TypeError):
        mutable_config["name"] = "changed"


def test_later_source_overrides_earlier_source_and_tracks_origin() -> None:
    service = ConfigService(
        [
            MappingConfigSource(
                {"app": {"name": "first", "debug": False}},
                source="first",
            ),
            MappingConfigSource({"app": {"name": "second"}}, source="second"),
        ]
    )

    config = service.get_config("app")

    assert config == {"name": "second", "debug": False}
    assert service.config.sources["app.name"] == "second"
    assert service.config.sources["app.debug"] == "first"


def test_environment_source_parses_explicit_environment_values() -> None:
    service = ConfigService(
        [
            EnvironmentConfigSource(
                {"APP_RELOAD": "true", "APP_PORT": "8000"},
                env_settings=(
                    EnvironmentSetting("APP_RELOAD", "reload", "bool"),
                    EnvironmentSetting("APP_PORT", "port", "int"),
                ),
                section="app",
            )
        ]
    )

    assert service.get_config("app") == {"reload": True, "port": 8000}


def test_environment_source_default_behaviour_without_env_settings() -> None:
    service = ConfigService(
        [
            EnvironmentConfigSource(
                {"APP_RELOAD": "true", "APP_PORT": "8000"},
                section="app",
            )
        ]
    )

    assert service.get_config("app") == {
        "APP_RELOAD": "true",
        "APP_PORT": "8000",
    }


@pytest.mark.parametrize(
    ("value_type", "env_name", "field_name", "message"),
    [
        (
            "bool",
            "APP_DEBUG",
            "debug",
            "APP_DEBUG for debug: APP_DEBUG must not be blank.",
        ),
        (
            "int",
            "APP_PORT",
            "port",
            "APP_PORT for port: APP_PORT must not be blank.",
        ),
        (
            "path",
            "APP_DATA_DIR",
            "data_dir",
            "APP_DATA_DIR for data_dir: APP_DATA_DIR must not be blank.",
        ),
    ],
)
@pytest.mark.parametrize("value", ["", "   "])
def test_environment_source_blank_values_record_diagnostic(
    value_type: EnvironmentValueType,
    env_name: str,
    field_name: str,
    message: str,
    value: str,
) -> None:
    service = ConfigService(
        [
            MappingConfigSource({"app": {"name": "uniquode"}}),
            EnvironmentConfigSource(
                {env_name: value},
                env_settings=(EnvironmentSetting(env_name, field_name, value_type),),
                section="app",
                required=False,
            ),
        ]
    )

    assert service.get_config("app") == {"name": "uniquode"}
    assert len(service.diagnostics) == 1
    diagnostic = service.diagnostics[0]
    assert diagnostic.code == "environment_config_error"
    assert diagnostic.message == message


def test_environment_source_invalid_bool_records_diagnostic() -> None:
    service = ConfigService(
        [
            MappingConfigSource({"app": {"name": "uniquode"}}),
            EnvironmentConfigSource(
                {"APP_DEBUG": "maybe"},
                env_settings=(EnvironmentSetting("APP_DEBUG", "debug", "bool"),),
                section="app",
                required=False,
            ),
        ]
    )

    assert service.get_config("app") == {"name": "uniquode"}
    assert len(service.diagnostics) == 1
    diagnostic = service.diagnostics[0]
    assert diagnostic.code == "environment_config_error"
    assert diagnostic.message == (
        "APP_DEBUG for debug: APP_DEBUG must be a boolean value."
    )


def test_required_environment_source_error_fails_loading() -> None:
    with pytest.raises(ConfigSourceError, match="APP_RELOAD must be a boolean value"):
        ConfigService(
            [
                EnvironmentConfigSource(
                    {"APP_RELOAD": "not-bool"},
                    env_settings=(EnvironmentSetting("APP_RELOAD", "reload", "bool"),),
                    section="app",
                )
            ]
        )


def test_file_source_reads_resolved_app_config(tmp_path: Path) -> None:
    config_path = tmp_path / "app.toml"
    config_path.write_text(
        """
[app]
modules = ["wybra"]
database_url = "sqlite:///app.sqlite3"

[app.database]
backend = "sqlite"
database = "structured.sqlite3"

[app.templates]
auto_reload = true
cache_size = 0

[app.assets]
url_path = "/static"
root = "static"

[auth]
account_creation_policy = "closed"

[secrets.crypto]
source = "keychain"
current_key = "secrets/key/current"

[secrets.keychain]
appname = "wybra"
username = "deployment"
""".strip(),
        encoding="utf-8",
    )

    service = ConfigService([FileConfigSource(config_path, project_root=tmp_path)])

    app_section = service.get_config("app")
    assert app_section is not None
    assert app_section["database_url"] == "sqlite:///app.sqlite3"
    assert service.get_config("app.database") == {
        "backend": "sqlite",
        "database": "structured.sqlite3",
    }
    assert service.get_config("app.assets") == {
        "url_path": "/static",
        "root": Path("static"),
        "export_mode": "normal",
        "serve": True,
    }
    assert service.get_config("app.templates") == {
        "auto_reload": True,
        "cache_size": 0,
        "root": None,
    }
    assert service.get_config("auth") == {"account_creation_policy": "closed"}
    assert service.get_config("secrets.crypto") == {
        "source": "keychain",
        "current_key": "secrets/key/current",
    }
    assert service.get_config("secrets.keychain") == {
        "appname": "wybra",
        "username": "deployment",
    }


def test_file_source_reports_parse_diagnostic(tmp_path: Path) -> None:
    config_path = tmp_path / "app.toml"
    config_path.write_text("[app\n", encoding="utf-8")

    with pytest.raises(ConfigSourceError, match="is invalid"):
        ConfigService([FileConfigSource(config_path, project_root=tmp_path)])


def test_optional_file_source_diagnostic_includes_source_location(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "app.toml"
    config_path.write_text("[app\n", encoding="utf-8")

    service = ConfigService(
        [FileConfigSource(config_path, project_root=tmp_path, required=False)]
    )

    assert len(service.diagnostics) == 1
    diagnostic = service.diagnostics[0]
    assert diagnostic.location is not None
    assert diagnostic.location.file == config_path
    assert diagnostic.code == "file_config_error"


def test_config_def_applies_raw_defaults_and_env_overrides() -> None:
    config_def = ConfigDef(
        {
            "app.assets": ConfigGroup(
                fields=(
                    ConfigField(name="url_path", default="/static/"),
                    ConfigField(
                        name="root",
                        default="static",
                        env="APP_STATIC_EXPORT",
                    ),
                ),
            )
        }
    )

    ConfigService.set_runtime_environment({"APP_STATIC_EXPORT": "public-static"})
    service = ConfigService(config_defs=(config_def,))

    assert service.get_config("app.assets") == {
        "url_path": "/static/",
        "root": "public-static",
    }
    assert service.config.sources["app.assets.url_path"] == "default"
    assert service.config.sources["app.assets.root"] == "environment"


def test_repeated_config_section_applies_defaults_transforms_and_environment() -> None:
    group = ConfigGroup(
        fields=(
            ConfigField(name="enabled", default=False, transform=to_bool),
            ConfigField(name="label", default="worker"),
        )
    )
    config_def = ConfigDef(
        {},
        repeated_sections={
            "service": RepeatedConfigSection(
                group=group,
                environment_prefix="APP_SERVICE",
                environment_fields=("enabled",),
            )
        },
    )
    ConfigService.set_runtime_environment({"APP_SERVICE__EMAIL__ENABLED": "yes"})

    service = ConfigService(
        [
            MappingConfigSource(
                {
                    "service.email": {},
                    "service.tasks": {"enabled": "false"},
                }
            )
        ],
        config_defs=(config_def,),
    )

    assert service.get_config("service.email") == {
        "enabled": True,
        "label": "worker",
    }
    assert service.get_config("service.tasks") == {
        "enabled": False,
        "label": "worker",
    }
    assert service.config.sources["service.email.enabled"] == "environment"
    assert service.config.sources["service.email.label"] == "default"
    assert service.config.sources["service.tasks.enabled"] == "mapping"


def test_repeated_config_section_validates_instance_name() -> None:
    def validate_name(name: str) -> None:
        if name != "valid":
            raise ValueError("name must be 'valid'.")

    config_def = ConfigDef(
        {},
        repeated_sections={
            "service": RepeatedConfigSection(
                group=ConfigGroup(),
                name_validator=validate_name,
            )
        },
    )

    with pytest.raises(
        ConfigSourceError,
        match=r"Config section 'service\.invalid' is invalid: name must be 'valid'",
    ):
        ConfigService(
            [MappingConfigSource({"service.invalid": {}})],
            config_defs=(config_def,),
        )


def test_exact_and_more_specific_sections_precede_repeated_prefixes() -> None:
    config_def = ConfigDef(
        {
            "service.special": ConfigGroup(
                fields=(
                    ConfigField(
                        name="value",
                        transform=lambda value: f"exact:{value}",
                    ),
                ),
            )
        },
        repeated_sections={
            "service": RepeatedConfigSection(
                group=ConfigGroup(
                    fields=(
                        ConfigField(
                            name="value",
                            transform=lambda value: f"general:{value}",
                        ),
                    ),
                ),
            ),
            "service.region": RepeatedConfigSection(
                group=ConfigGroup(
                    fields=(
                        ConfigField(
                            name="value",
                            transform=lambda value: f"regional:{value}",
                        ),
                    ),
                ),
            ),
        },
    )

    service = ConfigService(
        [
            MappingConfigSource(
                {
                    "service.special": {"value": "fixed"},
                    "service.region.email": {"value": "dynamic"},
                }
            )
        ],
        config_defs=(config_def,),
    )

    assert service.get_config("service.special") == {"value": "exact:fixed"}
    assert service.get_config("service.region.email") == {"value": "regional:dynamic"}


def test_config_def_field_without_transform_preserves_raw_value() -> None:
    config_def = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(ConfigField(name="enabled", default="true"),),
            )
        }
    )

    service = ConfigService(config_defs=(config_def,))

    assert service.get_config("app") == {"enabled": "true"}


def test_config_def_field_transform_applies_to_resolved_value() -> None:
    config_def = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(
                    ConfigField(
                        name="enabled",
                        default=False,
                        env="APP_ENABLED",
                        transform=to_bool,
                    ),
                ),
            )
        }
    )

    ConfigService.set_runtime_environment({"APP_ENABLED": "yes"})
    service = ConfigService(config_defs=(config_def,))

    assert service.get_config("app") == {"enabled": True}
    assert service.config.sources["app.enabled"] == "environment"


def test_config_def_field_transform_can_normalise_paths() -> None:
    config_def = ConfigDef(
        {
            "app.assets": ConfigGroup(
                fields=(
                    ConfigField(
                        name="root",
                        default="static",
                        transform=to_path,
                    ),
                ),
            )
        }
    )

    service = ConfigService(config_defs=(config_def,))

    config = service.get_config("app.assets")
    assert config is not None
    assert config["root"] == (Path.cwd() / "static").resolve()


def test_config_def_field_transform_failure_fails_loading() -> None:
    config_def = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(
                    ConfigField(
                        name="enabled",
                        default="maybe",
                        transform=to_bool,
                    ),
                ),
            )
        }
    )

    with pytest.raises(
        ConfigSourceError,
        match=(
            r"Config value app\.enabled is invalid: must be a boolean value\. "
            r"\(source: default\)"
        ),
    ):
        ConfigService(config_defs=(config_def,))


def test_config_def_field_transform_failure_names_environment_source() -> None:
    config_def = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(
                    ConfigField(
                        name="enabled",
                        env="APP_ENABLED",
                        transform=to_bool,
                    ),
                ),
            )
        }
    )

    with pytest.raises(
        ConfigSourceError,
        match=(
            r"Config value app\.enabled is invalid: must be a boolean value\. "
            r"\(source: environment\)"
        ),
    ):
        ConfigService.set_runtime_environment({"APP_ENABLED": "maybe"})
        ConfigService(config_defs=(config_def,))


def test_base_settings_infers_single_config_section() -> None:
    @dataclass(frozen=True, slots=True)
    class ExampleSettings(BaseSettings):
        module_config = ConfigDef(
            {
                "example": ConfigGroup(
                    fields=(
                        ConfigField(
                            name="enabled",
                            default="true",
                            transform=to_bool,
                        ),
                        ConfigField(name="ignored", default="not-a-setting"),
                    ),
                )
            }
        )

        enabled: bool = False

    settings = ExampleSettings.load_settings(
        ConfigService(config_defs=(ExampleSettings.module_config,))
    )

    assert settings == ExampleSettings(enabled=True)


def test_base_settings_requires_section_for_multi_section_config_def() -> None:
    @dataclass(frozen=True, slots=True)
    class ExampleSettings(BaseSettings):
        module_config = ConfigDef(
            {
                "first": ConfigGroup(fields=(ConfigField(name="name"),)),
                "second": ConfigGroup(fields=(ConfigField(name="name"),)),
            }
        )

        name: str = "default"

    with pytest.raises(
        ConfigDefinitionError,
        match=(
            "config_section must be set when module_config declares multiple sections"
        ),
    ):
        ExampleSettings.load_settings({"name": "configured"})


def test_base_settings_accepts_explicit_section_for_multi_section_config_def() -> None:
    @dataclass(frozen=True, slots=True)
    class ExampleSettings(BaseSettings):
        module_config = ConfigDef(
            {
                "first": ConfigGroup(fields=(ConfigField(name="name"),)),
                "second": ConfigGroup(fields=(ConfigField(name="name"),)),
            }
        )
        config_section = "second"

        name: str = "default"

    settings = ExampleSettings.load_settings({"name": "configured"})

    assert settings == ExampleSettings(name="configured")


def test_base_settings_reports_missing_module_config() -> None:
    @dataclass(frozen=True, slots=True)
    class BrokenSettings(BaseSettings):
        enabled: bool = False

    with pytest.raises(
        ConfigDefinitionError,
        match="BrokenSettings.module_config must be a ConfigDef",
    ):
        BrokenSettings.load_settings({})


def test_base_settings_reports_invalid_module_config_type() -> None:
    @dataclass(frozen=True, slots=True)
    class BrokenSettings(BaseSettings):
        module_config = object()

        enabled: bool = False

    with pytest.raises(
        ConfigDefinitionError,
        match="BrokenSettings.module_config must be a ConfigDef, not 'object'",
    ):
        BrokenSettings.load_settings({})


def test_base_settings_section_values_does_not_leak_non_mapping_source_value() -> None:
    @dataclass(frozen=True, slots=True)
    class ExampleSettings(BaseSettings):
        module_config = ConfigDef(
            {"example": ConfigGroup(fields=(ConfigField(name="secret"),))}
        )

        secret: str = ""

    secret_value = "super-secret-token"

    with pytest.raises(ConfigSourceError) as exc_info:
        ExampleSettings.section_values(secret_value, "example")  # type: ignore[arg-type]

    message = str(exc_info.value)
    assert "str" in message
    assert secret_value not in message


def test_base_settings_section_values_does_not_leak_non_mapping_section() -> None:
    @dataclass(frozen=True, slots=True)
    class ExampleSettings(BaseSettings):
        module_config = ConfigDef(
            {"example": ConfigGroup(fields=(ConfigField(name="secret"),))}
        )

        secret: str = ""

    secret_value = "super-secret-token"

    with pytest.raises(ConfigSourceError) as exc_info:
        ExampleSettings.section_values({"example": secret_value}, "example")

    message = str(exc_info.value)
    assert "str" in message
    assert secret_value not in message


def test_base_settings_transform_errors_do_not_leak_raw_values() -> None:
    def reject_secret(value: object) -> str:
        raise ValueError(f"cannot use {value!r}")

    @dataclass(frozen=True, slots=True)
    class ExampleSettings(BaseSettings):
        module_config = ConfigDef(
            {
                "example": ConfigGroup(
                    fields=(ConfigField(name="secret", transform=reject_secret),)
                )
            }
        )

        secret: str = ""

    secret_value = "super-secret-token"

    with pytest.raises(ConfigSourceError) as exc_info:
        ExampleSettings.load_settings({"secret": secret_value})

    message = str(exc_info.value)
    assert "example.secret" in message
    assert secret_value not in message


def test_config_section_rejects_duplicate_field_names() -> None:
    with pytest.raises(
        ConfigDefinitionError,
        match="Config fields contain duplicate names: known",
    ):
        ConfigGroup(
            fields=(
                ConfigField(name="known"),
                ConfigField(name="known"),
            )
        )


def test_config_field_rejects_blank_names() -> None:
    with pytest.raises(ConfigDefinitionError, match="must not be blank"):
        ConfigField(name="   ")


def test_config_section_rejects_non_field_values() -> None:
    with pytest.raises(
        ConfigDefinitionError,
        match="ConfigGroup fields must be ConfigField instances.",
    ):
        ConfigGroup(fields=("known",))  # type: ignore[arg-type]


def test_config_def_supports_multiple_sections_and_source_overrides() -> None:
    config_def = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(
                    ConfigField(
                        name="database_url",
                        default="sqlite:///default.db",
                    ),
                ),
            ),
            "auth": ConfigGroup(
                fields=(ConfigField(name="session_cookie_name", default="default"),),
            ),
        }
    )

    service = ConfigService(
        [MappingConfigSource({"app": {"database_url": "sqlite:///configured.db"}})],
        config_defs=(config_def,),
    )

    assert service.get_config("app") == {"database_url": "sqlite:///configured.db"}
    assert service.get_config("auth") == {"session_cookie_name": "default"}


def test_config_def_rejects_conflicting_default_definitions() -> None:
    first = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(
                    ConfigField(
                        name="database_url",
                        default="sqlite:///first.db",
                    ),
                ),
            )
        }
    )
    second = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(
                    ConfigField(
                        name="database_url",
                        default="sqlite:///second.db",
                    ),
                ),
            )
        }
    )

    with pytest.raises(
        ConfigDefinitionError,
        match="Conflicting definition for field app.database_url.",
    ):
        ConfigService(config_defs=(first, second))


def test_config_def_rejects_conflicting_env_definitions() -> None:
    first = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(ConfigField(name="database_url", env="DATABASE_URL"),),
            )
        }
    )
    second = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(ConfigField(name="database_url", env="APP_DATABASE_URL"),),
            )
        }
    )

    with pytest.raises(
        ConfigDefinitionError,
        match="Conflicting definition for field app.database_url.",
    ):
        ConfigService(config_defs=(first, second))


def test_config_def_rejects_conflicting_field_transforms() -> None:
    def first_transform(value: Any) -> Any:
        return value

    def second_transform(value: Any) -> Any:
        return value

    first = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(ConfigField(name="enabled", transform=first_transform),),
            )
        }
    )
    second = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(ConfigField(name="enabled", transform=second_transform),),
            )
        }
    )

    with pytest.raises(
        ConfigDefinitionError,
        match="Conflicting definition for field app.enabled.",
    ):
        ConfigService(config_defs=(first, second))


def test_module_config_def_is_discovered_from_module_root(
    tmp_path: Path, monkeypatch
) -> None:
    module_root = tmp_path / "example_module"
    module_root.mkdir()
    module_root.joinpath("__init__.py").write_text(
        "from wybra.config import ConfigDef, ConfigField, ConfigGroup\n"
        "module_config = ConfigDef({\n"
        "    'example': ConfigGroup(\n"
        "        fields=(ConfigField(name='enabled', default=True),)\n"
        "    )\n"
        "})\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    service = ConfigService(
        [MappingConfigSource({"app": {"modules": ("example_module",)}})]
    )

    assert service.get_config("example") == {"enabled": True}


@pytest.mark.parametrize(
    "modules",
    [
        "example_module",
        ("example_module", 123),
    ],
)
def test_module_config_discovery_rejects_malformed_modules(modules: object) -> None:
    with pytest.raises(
        ConfigDefinitionError,
        match=r"\[app\]\.modules must be a list or tuple of module names.",
    ):
        ConfigService([MappingConfigSource({"app": {"modules": modules}})])


def test_invalid_module_config_def_is_rejected(tmp_path: Path, monkeypatch) -> None:
    module_root = tmp_path / "bad_module"
    module_root.mkdir()
    module_root.joinpath("__init__.py").write_text(
        "module_config = object()\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(ConfigDefinitionError, match="bad_module.module_config"):
        ConfigService([MappingConfigSource({"app": {"modules": ("bad_module",)}})])


def test_config_def_env_override_uses_first_present_environment_name() -> None:
    config_def = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(
                    ConfigField(
                        name="database_url",
                        default="sqlite:///default.db",
                        env=("DATABASE_URL", "APP_DATABASE_URL"),
                    ),
                ),
            )
        }
    )

    ConfigService.set_runtime_environment({"APP_DATABASE_URL": "sqlite:///fallback.db"})
    service = ConfigService(config_defs=(config_def,))

    assert service.get_config("app") == {"database_url": "sqlite:///fallback.db"}


def test_config_def_env_override_prefers_first_environment_name() -> None:
    config_def = ConfigDef(
        {
            "app": ConfigGroup(
                fields=(
                    ConfigField(
                        name="database_url",
                        default="sqlite:///default.db",
                        env=("DATABASE_URL", "APP_DATABASE_URL"),
                    ),
                ),
            )
        }
    )

    ConfigService.set_runtime_environment(
        {
            "DATABASE_URL": "sqlite:///primary.db",
            "APP_DATABASE_URL": "sqlite:///fallback.db",
        }
    )
    service = ConfigService(config_defs=(config_def,))

    assert service.get_config("app") == {"database_url": "sqlite:///primary.db"}


def test_app_config_source_loads_app_config_sections(tmp_path: Path) -> None:
    app_config = AppConfig(
        config_path=tmp_path / "app.toml",
        project_root=tmp_path,
        modules=("app",),
        routes=RouteOptions(prefixes={"app": {"default": ""}}),
        templates=TemplateOptions(auto_reload=True, cache_size=12),
        assets=AssetOptions(
            url_path="/assets/",
            root=Path("static"),
            export_mode=AssetExportMode.NORMAL,
            cors=CorsPolicySet(
                enabled=True,
                allow_origins=("https://example.com",),
                paths={
                    "/assets/private/": CorsPolicy(
                        allow_origins=("https://admin.example.com",),
                    )
                },
            ),
        ),
        database_url="sqlite:///app.sqlite3",
        auth={"account_creation_policy": "closed"},
    )

    service = ConfigService([AppConfigSource(app_config)], discover_module_config=False)

    assert service.get_config("app") == {
        "config_path": tmp_path / "app.toml",
        "project_root": tmp_path,
        "modules": ("app",),
        "database_url": "sqlite:///app.sqlite3",
        "deployment_environment": None,
    }
    assert service.get_config("app.routes") == {"prefixes": {"app": {"default": ""}}}
    assert service.get_config("app.assets") == {
        "url_path": "/assets/",
        "root": Path("static"),
        "export_mode": "normal",
        "serve": True,
    }
    assert service.get_config("app.assets.cors") == {
        "enabled": True,
        "allow_origins": ("https://example.com",),
        "allow_methods": ("GET", "HEAD"),
        "allow_headers": (),
        "expose_headers": (),
        "allow_credentials": False,
        "max_age": 600,
        "paths": {
            "/assets/private/": {
                "allow_origins": ("https://admin.example.com",),
                "allow_methods": ("GET", "HEAD"),
                "allow_headers": (),
                "expose_headers": (),
                "allow_credentials": False,
                "max_age": 600,
            }
        },
    }
    assert service.get_config("app.assets.cors.paths./assets/private/") is None
    assert service.get_config("app.templates") == {
        "auto_reload": True,
        "cache_size": 12,
        "root": None,
    }
    assert service.get_config("auth") == {"account_creation_policy": "closed"}
