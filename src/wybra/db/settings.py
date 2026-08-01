from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Protocol, cast

from wybra.config import ConfigService, CredentialReference
from wybra.core.environment import environment_get
from wybra.core.exceptions import ConfigurationError
from wybra.db.config import (
    AWS_CONFIG_SECTION,
    DATABASE_AWS_CONFIG_SECTION,
    DATABASE_CONFIG_SECTION,
)
from wybra.db.routing import (
    DATABASE_ROTATION_POLICIES,
    DATABASE_ROUTE_ROLES,
    DatabaseRotationPolicy,
    DatabaseRouteInstance,
    DatabaseRouteRegistry,
    DatabaseRouteRole,
)
from wybra.db.urls import (
    DatabaseBackend,
    database_backend_for_scheme,
    database_backend_for_url,
    database_url_support_error,
    resolve_database_url,
    tortoise_database_url,
)
from wybra.services.secrets import (
    ENVIRONMENT_SOURCE,
    SecretsCapability,
    SecretsError,
    SecretSource,
    normalise_secret_source,
)

APP_CONFIG_SECTION = "app"
DATABASE_URL_FIELD = "database_url"
DATABASE_URL_SOURCE_ENVIRONMENT = "environment"
DatabaseConfigSource = Literal["url", "structured"]
CredentialPurpose = Literal["runtime", "service_account"]
AwsManagedTarget = Literal["rds", "aurora"]
DEFAULT_LOGICAL_DATABASE_NAME = "default"
RESERVED_OPTION_KEYS = frozenset(
    {
        "database",
        "file_path",
        "host",
        "password",
        "port",
        "user",
    }
)
logger = logging.getLogger(__name__)


class DatabaseConfigProtocol(Protocol):
    database_connection: ResolvedDatabaseConnection


@dataclass(frozen=True, slots=True)
class DatabaseCredentialConfig:
    user: str | None = field(default=None, repr=False)
    password: str | None = field(default=None, repr=False)
    user_key: str | None = field(default=None, repr=False)
    password_key: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "user", _optional_non_blank_string(self.user, "user"))
        object.__setattr__(
            self,
            "password",
            _optional_non_blank_string(self.password, "password"),
        )
        object.__setattr__(
            self,
            "user_key",
            _optional_non_blank_string(self.user_key, "user_key"),
        )
        object.__setattr__(
            self,
            "password_key",
            _optional_non_blank_string(self.password_key, "password_key"),
        )

    @property
    def has_keys(self) -> bool:
        return self.user_key is not None or self.password_key is not None

    @property
    def configured(self) -> bool:
        return any(
            value is not None
            for value in (
                self.user,
                self.password,
                self.user_key,
                self.password_key,
            )
        )

    def has_resolvable_values(self, source: SecretSource | None) -> bool:
        return (
            self.user is not None
            or self.password is not None
            or (source is not None and self.has_keys)
        )


@dataclass(frozen=True, slots=True)
class ResolvedDatabaseCredentials:
    user: str | None = field(default=None, repr=False)
    password: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class AwsClientSettings:
    region: str | None = None
    profile: str | None = None
    account_id: str | None = None
    partition: str = "aws"
    role_arn: str | None = None
    role_session_name: str | None = None
    external_id: str | None = field(default=None, repr=False)
    external_id_source: SecretSource | None = None
    external_id_key: str | None = field(default=None, repr=False)
    sso_region: str | None = None
    sso_account_id: str | None = None
    sso_role_name: str | None = None
    sso_start_url: str | None = None
    section_name: str = field(default=AWS_CONFIG_SECTION, compare=False, repr=False)

    @classmethod
    def from_values(
        cls,
        values: Mapping[str, Any],
        *,
        section_name: str,
    ) -> AwsClientSettings:
        unknown_fields = sorted(set(values) - _aws_client_fields())
        if unknown_fields:
            raise ConfigurationError(
                f"Unknown option(s) in [{section_name}] configuration: "
                + ", ".join(unknown_fields)
                + "."
            )
        return cls(
            region=cast(str | None, values.get("region")),
            profile=cast(str | None, values.get("profile")),
            account_id=cast(str | None, values.get("account_id")),
            partition=cast(str | None, values.get("partition")) or "aws",
            role_arn=cast(str | None, values.get("role_arn")),
            role_session_name=cast(str | None, values.get("role_session_name")),
            external_id=cast(str | None, values.get("external_id")),
            external_id_source=cast(
                SecretSource | None,
                values.get("external_id_source"),
            ),
            external_id_key=cast(str | None, values.get("external_id_key")),
            sso_region=cast(str | None, values.get("sso_region")),
            sso_account_id=cast(str | None, values.get("sso_account_id")),
            sso_role_name=cast(str | None, values.get("sso_role_name")),
            sso_start_url=cast(str | None, values.get("sso_start_url")),
            section_name=section_name,
        )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "region",
            _optional_aws_string(
                self.region,
                "region",
                section_name=self.section_name,
            ),
        )
        object.__setattr__(
            self,
            "profile",
            _optional_aws_string(
                self.profile,
                "profile",
                section_name=self.section_name,
            ),
        )
        object.__setattr__(
            self,
            "account_id",
            _optional_aws_string(
                self.account_id,
                "account_id",
                section_name=self.section_name,
            ),
        )
        object.__setattr__(
            self,
            "partition",
            _optional_aws_string(
                self.partition,
                "partition",
                section_name=self.section_name,
            )
            or "aws",
        )
        object.__setattr__(
            self,
            "role_arn",
            _optional_aws_string(
                self.role_arn,
                "role_arn",
                section_name=self.section_name,
            ),
        )
        object.__setattr__(
            self,
            "role_session_name",
            _optional_aws_string(
                self.role_session_name,
                "role_session_name",
                section_name=self.section_name,
            ),
        )
        object.__setattr__(
            self,
            "external_id",
            _optional_aws_string(
                self.external_id,
                "external_id",
                section_name=self.section_name,
            ),
        )
        external_id_source = (
            _optional_secret_source(
                self.external_id_source,
                name="external_id_source",
            )
            if self.external_id_source is not None
            else None
        )
        object.__setattr__(self, "external_id_source", external_id_source)
        object.__setattr__(
            self,
            "external_id_key",
            _optional_aws_string(
                self.external_id_key,
                "external_id_key",
                section_name=self.section_name,
            ),
        )
        object.__setattr__(
            self,
            "sso_region",
            _optional_aws_string(
                self.sso_region,
                "sso_region",
                section_name=self.section_name,
            ),
        )
        object.__setattr__(
            self,
            "sso_account_id",
            _optional_aws_string(
                self.sso_account_id,
                "sso_account_id",
                section_name=self.section_name,
            ),
        )
        object.__setattr__(
            self,
            "sso_role_name",
            _optional_aws_string(
                self.sso_role_name,
                "sso_role_name",
                section_name=self.section_name,
            ),
        )
        object.__setattr__(
            self,
            "sso_start_url",
            _optional_aws_string(
                self.sso_start_url,
                "sso_start_url",
                section_name=self.section_name,
            ),
        )
        if self.external_id is not None and self.external_id_key is not None:
            raise ConfigurationError(
                "external_id and external_id_key are mutually exclusive."
            )
        if self.external_id_key is not None and self.external_id_source is None:
            raise ConfigurationError(
                "external_id_source is required when external_id_key is configured."
            )

    @property
    def configured(self) -> bool:
        return (
            any(
                value is not None
                for value in (
                    self.region,
                    self.profile,
                    self.account_id,
                    self.role_arn,
                    self.role_session_name,
                    self.external_id,
                    self.external_id_source,
                    self.external_id_key,
                    self.sso_region,
                    self.sso_account_id,
                    self.sso_role_name,
                    self.sso_start_url,
                )
            )
            or self.partition != "aws"
        )

    def with_overrides(self, overrides: Mapping[str, Any]) -> AwsClientSettings:
        return AwsClientSettings(
            region=_aws_override(overrides, "region", self.region),
            profile=_aws_override(overrides, "profile", self.profile),
            account_id=_aws_override(overrides, "account_id", self.account_id),
            partition=_aws_override(overrides, "partition", self.partition) or "aws",
            role_arn=_aws_override(overrides, "role_arn", self.role_arn),
            role_session_name=_aws_override(
                overrides,
                "role_session_name",
                self.role_session_name,
            ),
            external_id=_aws_override(overrides, "external_id", self.external_id),
            external_id_source=_aws_override(
                overrides,
                "external_id_source",
                self.external_id_source,
            ),
            external_id_key=_aws_override(
                overrides,
                "external_id_key",
                self.external_id_key,
            ),
            sso_region=_aws_override(overrides, "sso_region", self.sso_region),
            sso_account_id=_aws_override(
                overrides,
                "sso_account_id",
                self.sso_account_id,
            ),
            sso_role_name=_aws_override(
                overrides,
                "sso_role_name",
                self.sso_role_name,
            ),
            sso_start_url=_aws_override(
                overrides,
                "sso_start_url",
                self.sso_start_url,
            ),
            section_name=DATABASE_AWS_CONFIG_SECTION,
        )

    def resolved_external_id(
        self,
        *,
        environ: object | None = None,
        secrets: SecretsCapability | None = None,
    ) -> str | None:
        if self.external_id is not None:
            return self.external_id
        if self.external_id_key is None:
            return None
        if self.external_id_source is None:
            raise ConfigurationError("external_id_source is required.")
        if self.external_id_source == ENVIRONMENT_SOURCE:
            return _resolve_credential_value(
                None,
                self.external_id_key,
                source=self.external_id_source,
                environ=environ,
                secrets=None,
                field_name="AWS external_id",
            )
        if secrets is None:
            raise ConfigurationError(
                "SecretsCapability is required to resolve AWS external_id."
            )
        try:
            return secrets.resolve(
                self.external_id_source, self.external_id_key
            ).reveal()
        except SecretsError as exc:
            raise ConfigurationError(
                "Failed to resolve AWS external_id: "
                f"source={self.external_id_source} key={self.external_id_key}."
            ) from exc


@dataclass(frozen=True, slots=True)
class AwsManagedDatabaseSettings:
    managed: AwsManagedTarget
    client: AwsClientSettings
    db_instance_identifier: str | None = None
    cluster_identifier: str | None = None
    engine: str | None = None
    endpoint: str | None = None
    port: int | None = None

    @classmethod
    def from_values(
        cls,
        values: Mapping[str, Any],
        *,
        shared: AwsClientSettings,
    ) -> AwsManagedDatabaseSettings | None:
        if not _aws_database_configured(values):
            return None
        unknown_fields = sorted(set(values) - _aws_database_fields())
        if unknown_fields:
            raise ConfigurationError(
                f"Unknown option(s) in [{DATABASE_AWS_CONFIG_SECTION}] "
                "configuration: " + ", ".join(unknown_fields) + "."
            )
        managed = _required_aws_managed_target(values.get("managed"))
        client = shared.with_overrides(values)
        return cls(
            managed=managed,
            client=client,
            db_instance_identifier=cast(
                str | None,
                values.get("db_instance_identifier"),
            ),
            cluster_identifier=cast(str | None, values.get("cluster_identifier")),
            engine=cast(str | None, values.get("engine")),
            endpoint=cast(str | None, values.get("endpoint")),
            port=cast(int | None, values.get("port")),
        )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "db_instance_identifier",
            _optional_aws_string(
                self.db_instance_identifier,
                "db_instance_identifier",
                section_name=DATABASE_AWS_CONFIG_SECTION,
            ),
        )
        object.__setattr__(
            self,
            "cluster_identifier",
            _optional_aws_string(
                self.cluster_identifier,
                "cluster_identifier",
                section_name=DATABASE_AWS_CONFIG_SECTION,
            ),
        )
        object.__setattr__(
            self,
            "engine",
            _optional_aws_string(
                self.engine,
                "engine",
                section_name=DATABASE_AWS_CONFIG_SECTION,
            ),
        )
        object.__setattr__(
            self,
            "endpoint",
            _optional_aws_string(
                self.endpoint,
                "endpoint",
                section_name=DATABASE_AWS_CONFIG_SECTION,
            ),
        )
        object.__setattr__(
            self,
            "port",
            _optional_positive_int(
                self.port,
                "port",
                section_name=DATABASE_AWS_CONFIG_SECTION,
            ),
        )
        if self.managed == "rds":
            if self.db_instance_identifier is None:
                raise ConfigurationError(
                    "db_instance_identifier is required for AWS RDS targets."
                )
            if self.cluster_identifier is not None:
                raise ConfigurationError(
                    "cluster_identifier is not valid for AWS RDS targets."
                )
        if self.managed == "aurora":
            if self.cluster_identifier is None:
                raise ConfigurationError(
                    "cluster_identifier is required for AWS Aurora targets."
                )
            if self.db_instance_identifier is not None:
                raise ConfigurationError(
                    "db_instance_identifier is not valid for AWS Aurora targets."
                )


@dataclass(frozen=True, slots=True)
class StructuredDatabaseConfig:
    backend: str
    database: str
    sa_database: str | None = None
    host: str | None = None
    port: int | None = None
    options: Mapping[str, Any] = field(default_factory=dict)
    credential_source: SecretSource | None = None
    runtime_credentials: DatabaseCredentialConfig = field(
        default_factory=DatabaseCredentialConfig,
        repr=False,
    )
    service_account_credentials: DatabaseCredentialConfig = field(
        default_factory=DatabaseCredentialConfig,
        repr=False,
    )
    aws: AwsManagedDatabaseSettings | None = None

    @classmethod
    def from_values(
        cls,
        values: Mapping[str, Any],
        *,
        shared_aws_values: Mapping[str, Any] | None = None,
        database_aws_values: Mapping[str, Any] | None = None,
    ) -> StructuredDatabaseConfig:
        unknown_fields = sorted(set(values) - _structured_database_fields())
        if unknown_fields:
            raise ConfigurationError(
                "Unknown option(s) in [app.database] configuration: "
                + ", ".join(unknown_fields)
                + "."
            )

        backend = values.get("backend")
        ignore_credentials = isinstance(backend, str) and backend.strip() == "sqlite"
        runtime_credentials = (
            DatabaseCredentialConfig()
            if ignore_credentials
            else DatabaseCredentialConfig(
                user=cast(str | None, values.get("user")),
                password=cast(str | None, values.get("password")),
                user_key=cast(str | None, values.get("user_key")),
                password_key=cast(str | None, values.get("password_key")),
            )
        )
        service_account_credentials = (
            DatabaseCredentialConfig()
            if ignore_credentials
            else DatabaseCredentialConfig(
                user=cast(str | None, values.get("sa_user")),
                password=cast(str | None, values.get("sa_password")),
                user_key=cast(str | None, values.get("sa_user_key")),
                password_key=cast(str | None, values.get("sa_password_key")),
            )
        )

        shared_aws = AwsClientSettings.from_values(
            shared_aws_values or {},
            section_name=AWS_CONFIG_SECTION,
        )
        database_aws = AwsManagedDatabaseSettings.from_values(
            database_aws_values or {},
            shared=shared_aws,
        )

        return cls(
            backend=cast(str, backend),
            database=cast(str, values.get("database")),
            sa_database=cast(str | None, values.get("sa_database")),
            host=cast(str | None, values.get("host")),
            port=cast(int | None, values.get("port")),
            options=cast(Mapping[str, Any], values.get("options") or {}),
            credential_source=cast(
                SecretSource | None,
                None if ignore_credentials else values.get("credential_source"),
            ),
            runtime_credentials=runtime_credentials,
            service_account_credentials=service_account_credentials,
            aws=database_aws,
        )

    def __post_init__(self) -> None:
        backend = _required_non_blank_string(self.backend, "backend")
        backend_info = database_backend_for_scheme(backend)
        if backend_info is None:
            raise ConfigurationError(database_url_support_error(f"{backend}://"))
        object.__setattr__(self, "backend", backend)
        object.__setattr__(
            self,
            "database",
            _required_non_blank_string(self.database, "database"),
        )
        sa_database = (
            _optional_non_blank_string(self.sa_database, "sa_database")
            if backend_info.tortoise_scheme != "sqlite"
            else None
        )
        if sa_database is None and backend_info.tortoise_scheme in {
            "asyncpg",
            "psycopg",
        }:
            sa_database = "postgres"
        if (
            backend_info.tortoise_scheme in {"asyncpg", "psycopg"}
            and sa_database == self.database
        ):
            raise ConfigurationError(
                "sa_database must differ from the target database."
            )
        object.__setattr__(self, "sa_database", sa_database)
        host = (
            None
            if backend_info.tortoise_scheme == "sqlite"
            else _optional_non_blank_string(self.host, "host")
        )
        port = (
            None
            if backend_info.tortoise_scheme == "sqlite"
            else _optional_positive_int(self.port, "port")
        )
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)
        object.__setattr__(
            self,
            "options",
            MappingProxyType(dict(_normalise_options(self.options))),
        )
        credential_source = (
            None
            if backend_info.tortoise_scheme == "sqlite"
            else _optional_secret_source(self.credential_source)
        )
        object.__setattr__(self, "credential_source", credential_source)
        if credential_source is not None and credential_source != ENVIRONMENT_SOURCE:
            object.__setattr__(
                self,
                "runtime_credentials",
                _with_default_database_credential_keys(
                    self.runtime_credentials,
                    database=self.database,
                    role="app",
                ),
            )
            object.__setattr__(
                self,
                "service_account_credentials",
                _with_default_database_credential_keys(
                    self.service_account_credentials,
                    database=self.database,
                    role="service-account",
                ),
            )
        _validate_database_credential_configuration(
            backend_info,
            self.credential_source,
            self.runtime_credentials,
            self.service_account_credentials,
        )

    @property
    def backend_info(self) -> DatabaseBackend:
        backend = database_backend_for_scheme(self.backend)
        if backend is None:
            raise ConfigurationError(database_url_support_error(f"{self.backend}://"))
        return backend

    def requires_secret_capability_for(self, purpose: CredentialPurpose) -> bool:
        requires_database_secret = (
            self.backend_info.tortoise_scheme != "sqlite"
            and self.credential_source is not None
            and self.credential_source != ENVIRONMENT_SOURCE
            and self.credential_config(purpose).has_keys
        )
        requires_aws_secret = (
            purpose == "service_account"
            and self.aws is not None
            and self.aws.client.external_id_key is not None
            and self.aws.client.external_id_source != ENVIRONMENT_SOURCE
        )
        return requires_database_secret or requires_aws_secret

    def credential_config(self, purpose: CredentialPurpose) -> DatabaseCredentialConfig:
        if purpose == "runtime":
            return self.runtime_credentials
        return self.service_account_credentials

    def credential_references(self) -> tuple[CredentialReference, ...]:
        source = self.credential_source
        if source is None or self.backend_info.tortoise_scheme == "sqlite":
            return ()
        return tuple(
            _database_credential_references(
                self.runtime_credentials,
                source=source,
                user_name="database-user",
                password_name="database-password",
                user_description="Configured runtime database username.",
                password_description="Configured runtime database password.",
            )
            + _database_credential_references(
                self.service_account_credentials,
                source=source,
                user_name="database-sa-user",
                password_name="database-sa-password",
                user_description=(
                    "Configured database service-account username for provisioning."
                ),
                password_description=(
                    "Configured database service-account password for provisioning."
                ),
            )
        )


@dataclass(frozen=True, slots=True)
class EffectiveDatabaseConfig:
    source: DatabaseConfigSource
    project_root: Path
    database_url: str | None = None
    structured: StructuredDatabaseConfig | None = None

    @classmethod
    def from_url(
        cls,
        database_url: str,
        *,
        project_root: Path,
    ) -> EffectiveDatabaseConfig:
        resolved_url = _normalise_database_url(database_url, project_root)
        return cls(source="url", project_root=project_root, database_url=resolved_url)

    @classmethod
    def from_structured(
        cls,
        structured: StructuredDatabaseConfig,
        *,
        project_root: Path,
    ) -> EffectiveDatabaseConfig:
        return cls(
            source="structured",
            project_root=project_root.resolve(),
            structured=structured,
        )

    def __post_init__(self) -> None:
        object.__setattr__(self, "project_root", self.project_root.resolve())
        if self.source == "url":
            if self.database_url is None:
                raise ConfigurationError("Database URL must be configured.")
            object.__setattr__(
                self,
                "database_url",
                _normalise_database_url(self.database_url, self.project_root),
            )
            if self.structured is not None:
                raise ConfigurationError(
                    "URL database config must not include structured settings."
                )
            return

        if self.source == "structured":
            if self.structured is None:
                raise ConfigurationError(
                    "Structured database config must include [app.database]."
                )
            if self.database_url is not None:
                raise ConfigurationError(
                    "Structured database config must not include database_url."
                )
            return

        raise ConfigurationError(f"Unsupported database config source: {self.source}.")

    def requires_secret_capability_for(self, purpose: CredentialPurpose) -> bool:
        return (
            self.structured.requires_secret_capability_for(purpose)
            if self.structured is not None
            else False
        )

    def resolve(
        self,
        *,
        environ: object | None = None,
        secrets: SecretsCapability | None = None,
        purpose: CredentialPurpose = "runtime",
    ) -> ResolvedDatabaseConnection:
        if self.source == "url":
            assert self.database_url is not None
            backend = database_backend_for_url(self.database_url)
            if backend is None:
                raise ConfigurationError(database_url_support_error(self.database_url))
            return ResolvedDatabaseConnection.from_url(
                self.database_url,
                backend=backend,
            )

        assert self.structured is not None
        return _resolve_structured_database_connection(
            self.structured,
            project_root=self.project_root,
            environ=environ,
            secrets=secrets,
            purpose=purpose,
        )

    def connection_metadata(self) -> ResolvedDatabaseConnection:
        if self.source == "url":
            assert self.database_url is not None
            backend = database_backend_for_url(self.database_url)
            if backend is None:
                raise ConfigurationError(database_url_support_error(self.database_url))
            return ResolvedDatabaseConnection.from_url(
                self.database_url,
                backend=backend,
            )

        assert self.structured is not None
        return ResolvedDatabaseConnection.from_structured(
            backend=self.structured.backend_info,
            credentials=_structured_backend_credentials(
                self.structured,
                project_root=self.project_root,
            ),
            aws=_resolve_aws_managed_database_settings(
                self.structured.aws,
                environ=None,
                secrets=None,
                purpose="runtime",
            ),
        )

    def credential_references(self) -> tuple[CredentialReference, ...]:
        if self.structured is None:
            return ()
        return self.structured.credential_references()


@dataclass(frozen=True, slots=True)
class ResolvedDatabaseConnection:
    source: DatabaseConfigSource
    backend: DatabaseBackend
    database_url: str | None = field(default=None, repr=False)
    credentials: Mapping[str, Any] = field(default_factory=dict, repr=False)
    sa_database: str | None = None
    aws: AwsManagedDatabaseSettings | None = None

    @classmethod
    def from_url(
        cls,
        database_url: str,
        *,
        backend: DatabaseBackend,
    ) -> ResolvedDatabaseConnection:
        return cls(source="url", backend=backend, database_url=database_url)

    @classmethod
    def from_structured(
        cls,
        *,
        backend: DatabaseBackend,
        credentials: Mapping[str, Any],
        sa_database: str | None = None,
        aws: AwsManagedDatabaseSettings | None = None,
    ) -> ResolvedDatabaseConnection:
        return cls(
            source="structured",
            backend=backend,
            credentials=MappingProxyType(dict(credentials)),
            sa_database=sa_database,
            aws=aws,
        )

    @property
    def tortoise_connection_config(self) -> str | dict[str, Any]:
        if self.database_url is not None:
            return tortoise_database_url(self.database_url)
        return {
            "engine": f"tortoise.backends.{self.backend.tortoise_scheme}",
            "credentials": dict(self.credentials),
        }

    @property
    def redacted_description(self) -> str:
        if self.database_url is not None:
            return f"database URL: {self.backend.scheme}://<redacted>"
        database = self.credentials.get("database") or self.credentials.get("file_path")
        return (
            "structured database config: "
            f"backend={self.backend.scheme}, database={database}"
        )


@dataclass(frozen=True, slots=True)
class ResolvedDatabaseRouteInstance:
    """One resolved database instance participating in logical route pools."""

    instance_name: str
    roles: frozenset[DatabaseRouteRole]
    weight: int
    connection: ResolvedDatabaseConnection

    @property
    def alias(self) -> str:
        if self.instance_name == DEFAULT_LOGICAL_DATABASE_NAME:
            return DEFAULT_LOGICAL_DATABASE_NAME
        return f"database_{self.instance_name}"


@dataclass(frozen=True, slots=True)
class ResolvedDatabaseRouting:
    """Resolved runtime connections and route-selection policy."""

    instances: tuple[ResolvedDatabaseRouteInstance, ...]
    default_rotation: DatabaseRotationPolicy = "queue"
    reader_rotation: DatabaseRotationPolicy = "queue"
    writer_rotation: DatabaseRotationPolicy = "queue"

    def __post_init__(self) -> None:
        if not self.instances:
            raise ConfigurationError("At least one database route is required.")
        if not any("default" in instance.roles for instance in self.instances):
            raise ConfigurationError("At least one default database route is required.")

    @property
    def connections(self) -> Mapping[str, ResolvedDatabaseConnection]:
        return MappingProxyType(
            {instance.alias: instance.connection for instance in self.instances}
        )

    def build_registry(self) -> DatabaseRouteRegistry:
        """Build the one runtime registry owned by a database lifecycle."""
        return DatabaseRouteRegistry(
            tuple(
                DatabaseRouteInstance(
                    name=DEFAULT_LOGICAL_DATABASE_NAME,
                    alias=instance.alias,
                    roles=instance.roles,
                    weight=instance.weight,
                )
                for instance in self.instances
            ),
            default_rotation=self.default_rotation,
            reader_rotation=self.reader_rotation,
            writer_rotation=self.writer_rotation,
        )


def effective_database_config_from_config(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
    *,
    project_root: Path,
    configured_database_url: str | None = None,
    database_url_override: str | None = None,
) -> EffectiveDatabaseConfig | None:
    if database_url_override is not None:
        return EffectiveDatabaseConfig.from_url(
            database_url_override,
            project_root=project_root,
        )

    app_values = _section_values(config, APP_CONFIG_SECTION)
    structured_values = _section_values(config, DATABASE_CONFIG_SECTION)
    shared_aws_values = _section_values(config, AWS_CONFIG_SECTION)
    database_aws_values = _section_values(config, DATABASE_AWS_CONFIG_SECTION)
    database_url = _configured_database_url(
        app_values.get(DATABASE_URL_FIELD),
        configured_database_url,
    )
    database_url_source = _field_source(
        config,
        f"{APP_CONFIG_SECTION}.{DATABASE_URL_FIELD}",
    )

    if (
        database_url is not None
        and database_url_source == DATABASE_URL_SOURCE_ENVIRONMENT
    ):
        if _structured_database_configured(structured_values):
            logger.info("Using DATABASE_URL, overriding config")
        return EffectiveDatabaseConfig.from_url(database_url, project_root=project_root)

    if _structured_database_configured(structured_values):
        if database_url is not None:
            raise ConfigurationError(
                "Configure either [app.database] or [app].database_url, not both."
            )
        return EffectiveDatabaseConfig.from_structured(
            StructuredDatabaseConfig.from_values(
                _database_connection_values(
                    structured_values,
                    section_name=DATABASE_CONFIG_SECTION,
                ),
                shared_aws_values=shared_aws_values,
                database_aws_values=database_aws_values,
            ),
            project_root=project_root,
        )

    if database_url is not None:
        return EffectiveDatabaseConfig.from_url(database_url, project_root=project_root)

    return None


def resolve_database_connection_from_config(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
    *,
    project_root: Path,
    configured_database_url: str | None = None,
    database_url_override: str | None = None,
    secrets: SecretsCapability | None = None,
    purpose: CredentialPurpose = "runtime",
    fallback_to_runtime_credentials: bool = False,
) -> ResolvedDatabaseConnection | None:
    effective = effective_database_config_from_config(
        config,
        project_root=project_root,
        configured_database_url=configured_database_url,
        database_url_override=database_url_override,
    )
    if effective is None:
        return None
    resolved_purpose = _database_credential_purpose(
        effective,
        purpose=purpose,
        fallback_to_runtime_credentials=fallback_to_runtime_credentials,
    )
    resolved_secrets = secrets
    if (
        effective.requires_secret_capability_for(resolved_purpose)
        and resolved_secrets is None
    ):
        resolved_secrets = _secrets_capability_from_config(config)
    return effective.resolve(
        environ=_config_environ(config),
        secrets=resolved_secrets,
        purpose=resolved_purpose,
    )


def resolve_database_routing_from_config(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
    *,
    project_root: Path,
    configured_database_url: str | None = None,
    database_url_override: str | None = None,
    secrets: SecretsCapability | None = None,
) -> ResolvedDatabaseRouting | None:
    """Resolve the base database and its inherited route-specific instances."""
    effective = effective_database_config_from_config(
        config,
        project_root=project_root,
        configured_database_url=configured_database_url,
        database_url_override=database_url_override,
    )
    if effective is None:
        return None
    if effective.source == "url":
        connection = effective.resolve(
            environ=_config_environ(config),
            secrets=secrets,
        )
        return ResolvedDatabaseRouting(
            instances=(
                ResolvedDatabaseRouteInstance(
                    instance_name=DEFAULT_LOGICAL_DATABASE_NAME,
                    roles=frozenset(DATABASE_ROUTE_ROLES),
                    weight=1,
                    connection=connection,
                ),
            )
        )

    base_values = _section_values(config, DATABASE_CONFIG_SECTION)
    shared_aws_values = _section_values(config, AWS_CONFIG_SECTION)
    base_aws_values = _section_values(config, DATABASE_AWS_CONFIG_SECTION)
    base_roles = _database_route_roles(base_values.get("role"), is_base=True)
    rotations = _database_rotation_policies(base_values)
    base_connection_values = _database_connection_values(
        base_values,
        section_name=DATABASE_CONFIG_SECTION,
    )
    base_structured = StructuredDatabaseConfig.from_values(
        base_connection_values,
        shared_aws_values=shared_aws_values,
        database_aws_values=base_aws_values,
    )
    resolved_secrets = secrets
    if (
        base_structured.requires_secret_capability_for("runtime")
        and resolved_secrets is None
    ):
        resolved_secrets = _secrets_capability_from_config(config)
    environ = _config_environ(config)
    instances = [
        ResolvedDatabaseRouteInstance(
            instance_name=DEFAULT_LOGICAL_DATABASE_NAME,
            roles=base_roles,
            weight=_database_route_weight(
                base_values.get("weight"),
                section_name=DATABASE_CONFIG_SECTION,
            ),
            connection=EffectiveDatabaseConfig.from_structured(
                base_structured,
                project_root=project_root,
            ).resolve(environ=environ, secrets=resolved_secrets),
        )
    ]
    for instance_name, instance_values in _database_instance_sections(config).items():
        if "backend" in instance_values:
            raise ConfigurationError(
                f"[{DATABASE_CONFIG_SECTION}.{instance_name}].backend is not allowed."
            )
        roles = _database_route_roles(instance_values.get("role"), is_base=False)
        rotation_fields = sorted(
            set(instance_values)
            & {"default_rotation", "reader_rotation", "writer_rotation"}
        )
        if rotation_fields:
            raise ConfigurationError(
                f"[{DATABASE_CONFIG_SECTION}.{instance_name}] does not allow "
                f"{', '.join(rotation_fields)}."
            )
        merged_values = dict(base_connection_values)
        merged_values.update(
            _database_connection_values(
                instance_values,
                section_name=f"{DATABASE_CONFIG_SECTION}.{instance_name}",
            )
        )
        instance_aws_values = dict(base_aws_values)
        instance_aws_values.update(
            _section_values(config, f"{DATABASE_CONFIG_SECTION}.{instance_name}.aws")
        )
        structured = StructuredDatabaseConfig.from_values(
            merged_values,
            shared_aws_values=shared_aws_values,
            database_aws_values=instance_aws_values,
        )
        if (
            structured.requires_secret_capability_for("runtime")
            and resolved_secrets is None
        ):
            resolved_secrets = _secrets_capability_from_config(config)
        instances.append(
            ResolvedDatabaseRouteInstance(
                instance_name=instance_name,
                roles=roles,
                weight=_database_route_weight(
                    instance_values.get("weight"),
                    section_name=f"{DATABASE_CONFIG_SECTION}.{instance_name}",
                ),
                connection=EffectiveDatabaseConfig.from_structured(
                    structured,
                    project_root=project_root,
                ).resolve(environ=environ, secrets=resolved_secrets),
            )
        )
    return ResolvedDatabaseRouting(
        instances=tuple(instances),
        default_rotation=rotations["default"],
        reader_rotation=rotations["reader"],
        writer_rotation=rotations["writer"],
    )


def database_connection_metadata_from_config(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
    *,
    project_root: Path,
    configured_database_url: str | None = None,
    database_url_override: str | None = None,
) -> ResolvedDatabaseConnection | None:
    effective = effective_database_config_from_config(
        config,
        project_root=project_root,
        configured_database_url=configured_database_url,
        database_url_override=database_url_override,
    )
    if effective is None:
        return None
    return effective.connection_metadata()


def _database_credential_purpose(
    effective: EffectiveDatabaseConfig,
    *,
    purpose: CredentialPurpose,
    fallback_to_runtime_credentials: bool,
) -> CredentialPurpose:
    if (
        purpose != "service_account"
        or not fallback_to_runtime_credentials
        or effective.structured is None
    ):
        return purpose

    service_account_credentials = effective.structured.service_account_credentials
    if service_account_credentials.has_resolvable_values(
        effective.structured.credential_source
    ):
        return purpose
    return "runtime"


def resolve_database_provisioning_connection_from_config(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
    *,
    project_root: Path,
    configured_database_url: str | None = None,
    secrets: SecretsCapability | None = None,
) -> ResolvedDatabaseConnection:
    connection = resolve_database_connection_from_config(
        config,
        project_root=project_root,
        configured_database_url=configured_database_url,
        secrets=secrets,
        purpose="service_account",
    )
    if connection is None:
        raise ConfigurationError("Database lifecycle requires database configuration.")
    if connection.backend.tortoise_scheme == "sqlite":
        return connection
    if connection.source == "url":
        raise ConfigurationError(
            "Database provisioning does not use application database_url "
            "configuration. Configure structured service-account credentials."
        )
    if connection.credentials.get("user") is None:
        raise ConfigurationError(
            "Database provisioning requires a service-account database user."
        )
    if connection.credentials.get("password") is None:
        raise ConfigurationError(
            "Database provisioning requires a service-account database password."
        )
    return connection


def _validate_database_credential_configuration(
    backend: DatabaseBackend,
    credential_source: SecretSource | None,
    runtime_credentials: DatabaseCredentialConfig,
    service_account_credentials: DatabaseCredentialConfig,
) -> None:
    keys_configured = (
        runtime_credentials.has_keys or service_account_credentials.has_keys
    )
    if backend.tortoise_scheme == "sqlite":
        return
    if credential_source is None:
        return
    if credential_source is not None and not keys_configured:
        raise ConfigurationError("missing credential keys")
    _reject_plain_and_key(
        runtime_credentials.user,
        runtime_credentials.user_key,
        "user",
        "user_key",
    )
    _reject_plain_and_key(
        runtime_credentials.password,
        runtime_credentials.password_key,
        "password",
        "password_key",
    )
    _reject_plain_and_key(
        service_account_credentials.user,
        service_account_credentials.user_key,
        "user",
        "user_key",
    )
    _reject_plain_and_key(
        service_account_credentials.password,
        service_account_credentials.password_key,
        "password",
        "password_key",
    )


def _resolve_structured_database_connection(
    structured: StructuredDatabaseConfig,
    *,
    project_root: Path,
    environ: object | None,
    secrets: SecretsCapability | None,
    purpose: CredentialPurpose,
) -> ResolvedDatabaseConnection:
    backend = structured.backend_info
    credentials = _structured_backend_credentials(
        structured,
        project_root=project_root,
    )
    aws = _resolve_aws_managed_database_settings(
        structured.aws,
        environ=environ,
        secrets=secrets,
        purpose=purpose,
    )
    if backend.tortoise_scheme == "sqlite":
        return ResolvedDatabaseConnection.from_structured(
            backend=backend,
            credentials=credentials,
            aws=aws,
        )
    resolved_credentials = _resolve_credentials(
        structured.credential_config(purpose),
        source=structured.credential_source,
        environ=environ,
        secrets=secrets,
        purpose=purpose,
    )
    if resolved_credentials.user is not None:
        credentials["user"] = resolved_credentials.user
    if resolved_credentials.password is not None:
        credentials["password"] = resolved_credentials.password
    return ResolvedDatabaseConnection.from_structured(
        backend=backend,
        credentials=credentials,
        sa_database=structured.sa_database if purpose == "service_account" else None,
        aws=aws,
    )


def _resolve_aws_managed_database_settings(
    aws: AwsManagedDatabaseSettings | None,
    *,
    environ: object | None,
    secrets: SecretsCapability | None,
    purpose: CredentialPurpose,
) -> AwsManagedDatabaseSettings | None:
    if aws is None or purpose != "service_account":
        return aws
    external_id = aws.client.resolved_external_id(environ=environ, secrets=secrets)
    if external_id is None or aws.client.external_id_key is None:
        return aws
    # Service-account settings carry the resolved value only; source/key remain
    # on the original runtime settings where they are still useful diagnostics.
    client = replace(
        aws.client,
        external_id=external_id,
        external_id_source=None,
        external_id_key=None,
    )
    return replace(aws, client=client)


def _structured_backend_credentials(
    structured: StructuredDatabaseConfig,
    *,
    project_root: Path,
) -> dict[str, Any]:
    credentials = dict(structured.options)
    conflicting_options = sorted(RESERVED_OPTION_KEYS & credentials.keys())
    if conflicting_options:
        raise ConfigurationError(
            "[app.database].options must not contain reserved database fields: "
            + ", ".join(conflicting_options)
            + "."
        )
    if structured.backend_info.tortoise_scheme == "sqlite":
        credentials["file_path"] = _sqlite_file_path(structured.database, project_root)
        return credentials

    credentials["database"] = structured.database
    if structured.host is not None:
        credentials["host"] = structured.host
    if structured.port is not None:
        credentials["port"] = structured.port
    if structured.backend_info.tortoise_scheme == "mssql":
        credentials.setdefault("driver", "ODBC Driver 18 for SQL Server")
    return credentials


def _sqlite_file_path(database: str, project_root: Path) -> str:
    if database == ":memory:":
        return database
    path = Path(database)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve().as_posix()


def _resolve_credentials(
    credential_config: DatabaseCredentialConfig,
    *,
    source: SecretSource | None,
    environ: object | None,
    secrets: SecretsCapability | None,
    purpose: CredentialPurpose,
) -> ResolvedDatabaseCredentials:
    return ResolvedDatabaseCredentials(
        user=_resolve_credential_value(
            credential_config.user,
            credential_config.user_key,
            source=source,
            environ=environ,
            secrets=secrets,
            field_name=f"{purpose} database user",
        ),
        password=_resolve_credential_value(
            credential_config.password,
            credential_config.password_key,
            source=source,
            environ=environ,
            secrets=secrets,
            field_name=f"{purpose} database password",
        ),
    )


def _database_credential_references(
    credentials: DatabaseCredentialConfig,
    *,
    source: SecretSource,
    user_name: str,
    password_name: str,
    user_description: str,
    password_description: str,
) -> tuple[CredentialReference, ...]:
    references: list[CredentialReference] = []
    if credentials.user_key is not None:
        references.append(
            CredentialReference(
                name=user_name,
                key=credentials.user_key,
                owner="database",
                description=user_description,
                source=source,
                required=True,
            )
        )
    if credentials.password_key is not None:
        references.append(
            CredentialReference(
                name=password_name,
                key=credentials.password_key,
                owner="database",
                description=password_description,
                source=source,
                required=True,
            )
        )
    return tuple(references)


def _with_default_database_credential_keys(
    credentials: DatabaseCredentialConfig,
    *,
    database: str,
    role: str,
) -> DatabaseCredentialConfig:
    user_key = credentials.user_key
    password_key = credentials.password_key
    if credentials.user is None and user_key is None:
        user_key = _default_database_credential_key(database, role, "user")
    if credentials.password is None and password_key is None:
        password_key = _default_database_credential_key(database, role, "password")
    return DatabaseCredentialConfig(
        user=credentials.user,
        password=credentials.password,
        user_key=user_key,
        password_key=password_key,
    )


def _default_database_credential_key(database: str, role: str, field_name: str) -> str:
    _validate_default_database_key_segment(database)
    return f"database/{database}/{role}/{field_name}"


def _validate_default_database_key_segment(database: str) -> None:
    if any(
        character == "/" or character.isspace() or not character.isprintable()
        for character in database
    ):
        raise ConfigurationError(
            "Database name cannot be used in default credential keys; "
            "configure explicit database credential keys."
        )


def _resolve_credential_value(
    plain_value: str | None,
    key: str | None,
    *,
    source: SecretSource | None,
    environ: object | None,
    secrets: SecretsCapability | None,
    field_name: str,
) -> str | None:
    if key is None:
        return plain_value
    if source is None:
        return plain_value
    if source == ENVIRONMENT_SOURCE:
        environment = environ if environ is not None else os.environ
        value = environment_get(environment, key)
        if value is None or not value.strip():
            raise ConfigurationError(
                f"Environment variable {key} for {field_name} must be set."
            )
        return value
    if secrets is None:
        raise ConfigurationError(
            f"SecretsCapability is required to resolve {field_name} from "
            f"source={source}, key={key}."
        )
    try:
        return secrets.resolve(source, key).reveal()
    except SecretsError as exc:
        raise ConfigurationError(
            f"Failed to resolve {field_name} from source={source}, key={key}."
        ) from exc


def _secrets_capability_from_config(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
) -> SecretsCapability:
    if not isinstance(config, ConfigService):
        raise ConfigurationError(
            "SecretsCapability is required to resolve database credentials from "
            "non-environment secret sources."
        )
    from wybra.secrets.capabilities import secrets_capability_from_config

    return secrets_capability_from_config(config)


def _config_environ(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
) -> object | None:
    if isinstance(config, ConfigService):
        return config.environ
    # Plain mappings do not carry an isolated environment; environment-source
    # credential keys fall back to os.environ in _resolve_credential_value.
    return None


def _section_values(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
    section_name: str,
) -> dict[str, Any]:
    if isinstance(config, ConfigService):
        return dict(config.get_config(section_name) or {})
    configured_section = config.get(section_name)
    if configured_section is None:
        return {}
    if not isinstance(configured_section, Mapping):
        raise ConfigurationError(f"Config section {section_name!r} must be a table.")
    return dict(configured_section)


def _field_source(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
    field_name: str,
) -> str | None:
    if not isinstance(config, ConfigService):
        return None
    return config.config.sources.get(field_name)


def _configured_database_url(
    configured_value: Any,
    fallback_value: str | None,
) -> str | None:
    value = configured_value if configured_value is not None else fallback_value
    if value is None:
        return None
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ConfigurationError("DATABASE_URL must not be blank.")


def _normalise_database_url(database_url: str, project_root: Path) -> str:
    if not isinstance(database_url, str) or not database_url.strip():
        raise ConfigurationError("DATABASE_URL must not be blank.")
    return resolve_database_url(database_url.strip(), project_root)


def _structured_database_configured(values: Mapping[str, Any]) -> bool:
    return any(value is not None for value in values.values())


def _database_instance_sections(
    config: ConfigService | Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    sections = config.config.values if isinstance(config, ConfigService) else config
    prefix = f"{DATABASE_CONFIG_SECTION}."
    instances: dict[str, dict[str, Any]] = {}
    for section_name, values in sections.items():
        if not section_name.startswith(prefix):
            continue
        instance_name = section_name.removeprefix(prefix)
        if not instance_name or "." in instance_name:
            continue
        if not isinstance(values, Mapping):
            raise ConfigurationError(
                f"Config section {section_name!r} must be a table."
            )
        instances[instance_name] = dict(values)
    return instances


def _database_connection_values(
    values: Mapping[str, Any],
    *,
    section_name: str,
) -> dict[str, Any]:
    allowed_fields = _structured_database_fields() | _database_route_fields()
    unknown_fields = sorted(set(values) - allowed_fields)
    if unknown_fields:
        raise ConfigurationError(
            f"Unknown option(s) in [{section_name}] configuration: "
            + ", ".join(unknown_fields)
            + "."
        )
    return {
        field_name: value
        for field_name, value in values.items()
        if field_name in _structured_database_fields()
    }


def _database_route_roles(
    value: object,
    *,
    is_base: bool,
) -> frozenset[DatabaseRouteRole]:
    if value is None:
        if is_base:
            return frozenset({"default"})
        raise ConfigurationError("Database route instances must configure role.")
    if not isinstance(value, str):
        raise ConfigurationError(
            "Database route role must be a comma-separated string."
        )
    roles = tuple(part.strip() for part in value.split(",") if part.strip())
    if not roles:
        raise ConfigurationError("Database route role must not be blank.")
    invalid_roles = sorted(set(roles) - DATABASE_ROUTE_ROLES)
    if invalid_roles:
        raise ConfigurationError(
            "Unknown database route role(s): " + ", ".join(invalid_roles) + "."
        )
    if len(set(roles)) != len(roles):
        raise ConfigurationError("Database route roles must not be duplicated.")
    return frozenset(cast(DatabaseRouteRole, role) for role in roles)


def _database_route_weight(value: object, *, section_name: str) -> int:
    if value is None:
        return 1
    return (
        _optional_positive_int(
            value,
            "weight",
            section_name=section_name,
        )
        or 1
    )


def _database_rotation_policies(
    values: Mapping[str, Any],
) -> dict[DatabaseRouteRole, DatabaseRotationPolicy]:
    policies: dict[DatabaseRouteRole, DatabaseRotationPolicy] = {}
    for role in ("default", "reader", "writer"):
        raw_value = values.get(f"{role}_rotation", "queue")
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ConfigurationError(f"{role}_rotation must be a non-blank string.")
        policy = raw_value.strip()
        if policy not in DATABASE_ROTATION_POLICIES:
            raise ConfigurationError(f"Unknown database route rotation: {policy}.")
        policies[role] = cast(DatabaseRotationPolicy, policy)
    return policies


def _database_route_fields() -> frozenset[str]:
    return frozenset(
        {
            "role",
            "weight",
            "default_rotation",
            "reader_rotation",
            "writer_rotation",
        }
    )


def _structured_database_fields() -> frozenset[str]:
    return frozenset(
        {
            "backend",
            "host",
            "port",
            "database",
            "sa_database",
            "options",
            "credential_source",
            "user",
            "password",
            "user_key",
            "password_key",
            "sa_user",
            "sa_password",
            "sa_user_key",
            "sa_password_key",
        }
    )


def _aws_client_fields() -> frozenset[str]:
    return frozenset(
        {
            "region",
            "profile",
            "account_id",
            "partition",
            "role_arn",
            "role_session_name",
            "external_id",
            "external_id_source",
            "external_id_key",
            "sso_region",
            "sso_account_id",
            "sso_role_name",
            "sso_start_url",
        }
    )


def _aws_database_fields() -> frozenset[str]:
    return frozenset(
        {
            "managed",
            "db_instance_identifier",
            "cluster_identifier",
            "engine",
            "endpoint",
            "port",
            *_aws_client_fields(),
        }
    )


def _aws_database_configured(values: Mapping[str, Any]) -> bool:
    return any(value is not None for value in values.values())


def _aws_override[AwsOverrideT](
    values: Mapping[str, Any],
    field_name: str,
    default: AwsOverrideT,
) -> AwsOverrideT:
    value = values.get(field_name)
    return default if value is None else cast(AwsOverrideT, value)


def _required_aws_managed_target(value: object) -> AwsManagedTarget:
    if isinstance(value, str):
        target = value.strip().lower()
        if target in {"rds", "aurora"}:
            return cast(AwsManagedTarget, target)
    raise ConfigurationError("AWS database managed target must be rds or aurora.")


def _normalise_options(value: object) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigurationError("[app.database].options must be a table.")
    options: dict[str, Any] = {}
    for key, option_value in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ConfigurationError(
                "[app.database].options keys must be non-blank strings."
            )
        options[key.strip()] = option_value
    return options


def _optional_secret_source(
    value: object,
    *,
    name: str = "credential_source",
) -> SecretSource | None:
    if value is None:
        return None
    return normalise_secret_source(value, name=name)


def _optional_aws_string(
    value: object,
    field_name: str,
    *,
    section_name: str,
) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ConfigurationError(
        f"[{section_name}].{field_name} must be a non-blank string."
    )


def _required_non_blank_string(value: object, field_name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ConfigurationError(f"[app.database].{field_name} must be a non-blank string.")


def _optional_non_blank_string(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ConfigurationError(f"[app.database].{field_name} must be a non-blank string.")


def _optional_positive_int(
    value: object,
    field_name: str,
    *,
    section_name: str = DATABASE_CONFIG_SECTION,
) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ConfigurationError(
            f"[{section_name}].{field_name} must be a positive integer."
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = int(value)
        except ValueError as exc:
            raise ConfigurationError(
                f"[{section_name}].{field_name} must be a positive integer."
            ) from exc
    else:
        raise ConfigurationError(
            f"[{section_name}].{field_name} must be a positive integer."
        )
    if parsed <= 0:
        raise ConfigurationError(
            f"[{section_name}].{field_name} must be a positive integer."
        )
    return parsed


def _reject_plain_and_key(
    plain_value: str | None,
    key_value: str | None,
    plain_field: str,
    key_field: str,
) -> None:
    if plain_value is None or key_value is None:
        return
    raise ConfigurationError(
        f"[app.database].{plain_field} and [app.database].{key_field} "
        "are mutually exclusive."
    )


__all__ = (
    "CredentialPurpose",
    "AwsClientSettings",
    "AwsManagedDatabaseSettings",
    "AwsManagedTarget",
    "DatabaseConfigProtocol",
    "DatabaseConfigSource",
    "DatabaseCredentialConfig",
    "EffectiveDatabaseConfig",
    "ResolvedDatabaseConnection",
    "ResolvedDatabaseCredentials",
    "ResolvedDatabaseRouteInstance",
    "ResolvedDatabaseRouting",
    "StructuredDatabaseConfig",
    "database_connection_metadata_from_config",
    "effective_database_config_from_config",
    "resolve_database_connection_from_config",
    "resolve_database_routing_from_config",
    "resolve_database_provisioning_connection_from_config",
)
