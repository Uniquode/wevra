from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Literal, Self, cast

from wybra.cache import DEFAULT_CACHE_NAME
from wybra.config import BaseSettings, ConfigDef, ConfigService
from wybra.core.exceptions import ConfigurationError
from wybra.core.runtime import (
    DEFAULT_DEPLOYMENT_ENVIRONMENT,
    LOCAL_ENVIRONMENT,
    DeploymentEnvironment,
    normalise_deployment_environment,
)
from wybra.sessions.config import (
    DEFAULT_SESSION_FILE_DIRECTORY,
    SESSIONS_CONFIG_SECTION,
    SessionStorageBackend,
    module_config,
    to_cookie_same_site,
    to_non_blank_string,
    to_optional_bool,
    to_optional_cache_name,
    to_optional_non_blank_string,
    to_optional_path,
    to_optional_storage_backend,
    to_positive_float,
    to_positive_int,
)


@dataclass(frozen=True, slots=True)
class SessionsSettings(BaseSettings):
    module_config: ClassVar[ConfigDef] = module_config
    config_section: ClassVar[str | None] = SESSIONS_CONFIG_SECTION

    storage_backend: SessionStorageBackend | str | None = None
    lifetime_seconds: float | str = 14 * 24 * 60 * 60
    cookie_name: str = "wybra_session"
    cookie_domain: str | None = None
    cookie_path: str = "/"
    cookie_secure: bool | str | None = None
    cookie_same_site: str = "lax"
    file_directory: Path | str | None = None
    cache_name: str | None = None
    cache_key_prefix: str = "wybra:sessions:"
    database_connection_name: str = "default"
    payload_max_bytes: int | str = 65_536
    cookie_payload_max_bytes: int | str = 3_800
    project_root: Path = Path.cwd()
    deployment_environment: DeploymentEnvironment | str = DEFAULT_DEPLOYMENT_ENVIRONMENT

    @classmethod
    def load_settings(
        cls,
        config: ConfigService | Mapping[str, Any],
        *,
        deployment_environment: DeploymentEnvironment | str | None = None,
    ) -> Self:
        kwargs = cls.settings_kwargs(config)
        app_values = cls.section_values(config, "app")
        project_root = app_values.get("project_root")
        if isinstance(project_root, Path):
            kwargs["project_root"] = project_root
        elif isinstance(project_root, str) and project_root.strip():
            kwargs["project_root"] = Path(project_root)
        configured_environment = deployment_environment
        if configured_environment is None:
            configured_environment = app_values.get(
                "deployment_environment",
                DEFAULT_DEPLOYMENT_ENVIRONMENT,
            )
        kwargs["deployment_environment"] = configured_environment
        return cls(**kwargs)

    def __post_init__(self) -> None:
        deployment_environment = _configuration_value(
            normalise_deployment_environment,
            self.deployment_environment,
            "deployment_environment",
        )
        storage_backend = _configuration_value(
            to_optional_storage_backend,
            self.storage_backend,
            "storage_backend",
        )
        if storage_backend is None:
            if deployment_environment != LOCAL_ENVIRONMENT:
                raise ConfigurationError(
                    "wybra.sessions.storage_backend must be configured outside "
                    "local deployments."
                )
            storage_backend = SessionStorageBackend.COOKIE

        lifetime_seconds = _configuration_value(
            to_positive_float,
            self.lifetime_seconds,
            "lifetime_seconds",
        )
        cookie_name = _configuration_value(
            to_non_blank_string,
            self.cookie_name,
            "cookie_name",
        )
        cookie_domain = _configuration_value(
            to_optional_non_blank_string,
            self.cookie_domain,
            "cookie_domain",
        )
        cookie_path = _configuration_value(
            to_non_blank_string,
            self.cookie_path,
            "cookie_path",
        )
        cookie_secure = _configuration_value(
            to_optional_bool,
            self.cookie_secure,
            "cookie_secure",
        )
        if cookie_secure is None:
            cookie_secure = deployment_environment != LOCAL_ENVIRONMENT
        cookie_same_site = _configuration_value(
            to_cookie_same_site,
            self.cookie_same_site,
            "cookie_same_site",
        )
        if cookie_same_site == "none" and cookie_secure is not True:
            raise ConfigurationError(
                "wybra.sessions.cookie_secure must be true when "
                "cookie_same_site is 'none'."
            )
        project_root = _project_root(self.project_root)
        file_directory = _configuration_value(
            to_optional_path,
            self.file_directory,
            "file_directory",
        )
        if file_directory is None:
            file_directory = DEFAULT_SESSION_FILE_DIRECTORY
        if not file_directory.is_absolute():
            file_directory = project_root / file_directory
        cache_name = _configuration_value(
            to_optional_cache_name,
            self.cache_name,
            "cache_name",
        )
        cache_key_prefix = _configuration_value(
            to_non_blank_string,
            self.cache_key_prefix,
            "cache_key_prefix",
        )
        database_connection_name = _configuration_value(
            to_non_blank_string,
            self.database_connection_name,
            "database_connection_name",
        )
        payload_max_bytes = _configuration_value(
            to_positive_int,
            self.payload_max_bytes,
            "payload_max_bytes",
        )
        cookie_payload_max_bytes = _configuration_value(
            to_positive_int,
            self.cookie_payload_max_bytes,
            "cookie_payload_max_bytes",
        )

        object.__setattr__(self, "deployment_environment", deployment_environment)
        object.__setattr__(self, "storage_backend", storage_backend)
        object.__setattr__(self, "lifetime_seconds", lifetime_seconds)
        object.__setattr__(self, "cookie_name", cookie_name)
        object.__setattr__(self, "cookie_domain", cookie_domain)
        object.__setattr__(self, "cookie_path", cookie_path)
        object.__setattr__(self, "cookie_secure", cookie_secure)
        object.__setattr__(self, "cookie_same_site", cookie_same_site)
        object.__setattr__(self, "project_root", project_root)
        object.__setattr__(self, "file_directory", file_directory.resolve())
        object.__setattr__(self, "cache_name", cache_name)
        object.__setattr__(self, "cache_key_prefix", cache_key_prefix)
        object.__setattr__(self, "database_connection_name", database_connection_name)
        object.__setattr__(self, "payload_max_bytes", payload_max_bytes)
        object.__setattr__(self, "cookie_payload_max_bytes", cookie_payload_max_bytes)

    @property
    def resolved_storage_backend(self) -> SessionStorageBackend:
        return cast(SessionStorageBackend, self.storage_backend)

    @property
    def resolved_lifetime_seconds(self) -> float:
        return cast(float, self.lifetime_seconds)

    @property
    def resolved_cookie_secure(self) -> bool:
        return cast(bool, self.cookie_secure)

    @property
    def resolved_cookie_same_site(self) -> Literal["lax", "strict", "none"]:
        return cast(Literal["lax", "strict", "none"], self.cookie_same_site)

    @property
    def resolved_file_directory(self) -> Path:
        return cast(Path, self.file_directory)

    @property
    def resolved_cache_name(self) -> str:
        return DEFAULT_CACHE_NAME if self.cache_name is None else self.cache_name

    @property
    def resolved_payload_max_bytes(self) -> int:
        return cast(int, self.payload_max_bytes)

    @property
    def resolved_cookie_payload_max_bytes(self) -> int:
        return cast(int, self.cookie_payload_max_bytes)


def _configuration_value[ValueT](
    normalise: Any,
    value: object,
    setting_name: str,
) -> ValueT:
    try:
        return normalise(value)
    except ValueError as exc:
        raise ConfigurationError(f"wybra.sessions.{setting_name}: {exc}") from exc


def _project_root(value: object) -> Path:
    if isinstance(value, Path):
        return value.resolve()
    if isinstance(value, str) and value.strip():
        return Path(value).resolve()
    raise ConfigurationError("wybra.sessions project_root must be a path.")


__all__ = ("SessionsSettings",)
