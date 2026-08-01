import ast
import asyncio
import importlib
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.exceptions import OperationalError
from tortoise.models import Model
from tortoise.transactions import in_transaction

import wybra.db.migrate as migrate_module
import wybra.db.provisioning.core as provisioning_core
import wybra.db.provisioning.mssql as mssql_provisioning
import wybra.db.provisioning.mysql as mysql_provisioning
import wybra.db.urls as database_urls
from support_database import sqlite_file_url
from wybra import SiteCapabilityError
from wybra.config import ConfigService, MappingConfigSource
from wybra.core.exceptions import ConfigurationError
from wybra.db import DatabaseCapability
from wybra.db.capabilities import (
    DatabaseCapabilityError,
    WybraDatabaseCapability,
    tortoise_connection,
    tortoise_transaction,
)
from wybra.db.config import ENV_DATABASE_URL
from wybra.db.config import module_config as db_module_config
from wybra.db.models import Model as WybraModel
from wybra.db.persistence import (
    Database,
    close_database,
    close_database_connections,
    create_database,
)
from wybra.db.provisioning import (
    AwsManagedDatabaseMetadata,
    Boto3RdsMetadataClient,
    CredentialTransition,
    DatabaseFamily,
    DatabaseMaintenanceRequest,
    DatabaseProvisioningConfigurationError,
    DatabaseProvisioningOperationError,
    DestroyDatabaseRequest,
    ProvisioningContext,
    SQLiteProvisioner,
    UnsupportedFamilyProvisioner,
    clear_test_database_data,
    database_family_for_aws_engine,
    database_family_for_backend,
    destroy_database,
    provisioner_for_family,
    provisioning_context,
    run_database_maintenance,
)
from wybra.db.provisioning.mariadb import MariaDBProvisioner
from wybra.db.provisioning.mssql import SQLServerProvisioner, quote_mssql_identifier
from wybra.db.provisioning.mysql import MySQLProvisioner, quote_mysql_identifier
from wybra.db.provisioning.postgresql import PostgreSQLProvisioner
from wybra.db.routing import DatabaseRouteInstance, DatabaseRouteRegistry
from wybra.db.settings import (
    AwsClientSettings,
    AwsManagedDatabaseSettings,
    EffectiveDatabaseConfig,
    ResolvedDatabaseConnection,
    StructuredDatabaseConfig,
    database_connection_metadata_from_config,
    resolve_database_connection_from_config,
    resolve_database_provisioning_connection_from_config,
    resolve_database_routing_from_config,
)
from wybra.db.sql import ident, param, quote_sql_identifier, render_sql, trusted_sql
from wybra.db.surfaces import (
    DataCompositionError,
    discover_migration_version_locations,
    discover_model_package,
    migration_version_location_for_configured_module,
    migration_version_locations_from_modules,
    model_package_name,
    model_packages_from_modules,
)
from wybra.db.urls import (
    available_database_url_schemes,
    database_url_support_error,
    is_supported_database_url,
    parse_sqlite_database_url,
    redact_database_url,
    redact_database_urls,
    resolve_database_url,
    safe_database_error_message,
    supported_database_url_schemes,
    tortoise_database_url,
)
from wybra.db.validation import validate_persistence
from wybra.services.secrets import MissingSecretError, SecretValue
from wybra.site import start
from wybra.tools.settings import load_project_settings
from wybra.tools.validation.core import ValidationResult


@dataclass(frozen=True, slots=True)
class _PersistenceSettings:
    database_url: str | None
    migrations_root: Path | None
    database_connection: object | None = None
    configured_modules: tuple[str, ...] = ()

    @property
    def modules(self) -> tuple[str, ...]:
        return self.configured_modules


@dataclass(frozen=True, slots=True)
class _MigrationCommandSettings:
    database_url: str
    project_root: Path
    migrations_root: Path | None = None
    app_config: None = None

    @property
    def modules(self) -> tuple[str, ...]:
        return ()


class _RecordingSecretsCapability:
    def __init__(self, values: dict[tuple[str, str], str]) -> None:
        self.values = values

    def resolve(self, source: str, key: str) -> SecretValue:
        return SecretValue(self.values[(source, key)], source=source, key=key)

    def exists(self, source: str, key: str) -> bool:
        return (source, key) in self.values


class _RecordingAwsRdsMetadataClient:
    def __init__(self, metadata: AwsManagedDatabaseMetadata) -> None:
        self.metadata = metadata
        self.targets: list[AwsManagedDatabaseSettings] = []

    def describe(
        self,
        target: AwsManagedDatabaseSettings,
    ) -> AwsManagedDatabaseMetadata:
        self.targets.append(target)
        return self.metadata


class _FailingSecretsCapability:
    def resolve(self, source: str, key: str) -> SecretValue:
        raise MissingSecretError(source=source, key=key)

    def exists(self, source: str, key: str) -> bool:
        return False


class _FakeBoto3Module:
    def __init__(self) -> None:
        self.session = self
        self.assume_role_calls: list[dict[str, str]] = []

    def Session(self, **kwargs: str) -> _FakeBoto3Session:
        return _FakeBoto3Session(self, kwargs)


class _FakeBoto3Session:
    def __init__(
        self,
        module: _FakeBoto3Module,
        kwargs: dict[str, str],
    ) -> None:
        self.module = module
        self.kwargs = kwargs

    def client(self, service_name: str, *, region_name: str | None = None) -> object:
        if service_name == "sts":
            return _FakeStsClient(self.module)
        if service_name == "rds":
            return _FakeRdsClient()
        raise AssertionError(f"Unexpected fake boto3 service: {service_name}.")


class _FakeStsClient:
    def __init__(self, module: _FakeBoto3Module) -> None:
        self.module = module

    def assume_role(self, **kwargs: str) -> dict[str, dict[str, str]]:
        self.module.assume_role_calls.append(kwargs)
        return {
            "Credentials": {
                "AccessKeyId": "access-key",
                "SecretAccessKey": "secret-key",
                "SessionToken": "session-token",
            }
        }


class _FakeRdsClient:
    def describe_db_instances(
        self,
        *,
        DBInstanceIdentifier: str,
    ) -> dict[str, object]:
        return {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": DBInstanceIdentifier,
                    "Engine": "postgres",
                    "Endpoint": {
                        "Address": "uniquode.rds.amazonaws.com",
                        "Port": 5432,
                    },
                }
            ]
        }


# Real backend lifecycle coverage lives under tests_integration. The recording
# connectors below remain fast unit support for SQL rendering, validation,
# safety branches, missing-driver paths, and redaction checks.
class _RecordingPostgreSQLConnection:
    def __init__(self, fetch_values: list[object]) -> None:
        self.fetch_values = fetch_values
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def execute(self, query: str, *args: object) -> object:
        self.executed.append((query, args))
        return "OK"

    async def fetchval(self, query: str, *args: object) -> object:
        self.queries.append((query, args))
        if not self.fetch_values:
            raise AssertionError(f"No fake PostgreSQL response for query: {query}")
        return self.fetch_values.pop(0)

    async def close(self) -> object:
        self.closed = True


class _RecordingPostgreSQLConnector:
    def __init__(self, *connections: _RecordingPostgreSQLConnection) -> None:
        self.connections = list(connections)
        self.credentials: list[dict[str, object]] = []

    async def __call__(
        self,
        credentials: Mapping[str, object],
    ) -> _RecordingPostgreSQLConnection:
        self.credentials.append(dict(credentials))
        if not self.connections:
            raise AssertionError("Unexpected PostgreSQL connection attempt.")
        return self.connections.pop(0)


class _RecordingMySQLConnection:
    def __init__(
        self,
        fetch_values: list[object],
        fetch_rows: list[tuple[tuple[object, ...], ...]] | None = None,
    ) -> None:
        self.fetch_values = fetch_values
        self.fetch_rows = fetch_rows or []
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def execute(self, query: str, *args: object) -> object:
        self.executed.append((query, args))
        return "OK"

    async def fetchval(self, query: str, *args: object) -> object:
        self.queries.append((query, args))
        if not self.fetch_values:
            raise AssertionError(f"No fake MySQL response for query: {query}")
        return self.fetch_values.pop(0)

    async def fetchall(
        self, query: str, *args: object
    ) -> tuple[tuple[object, ...], ...]:
        self.queries.append((query, args))
        if not self.fetch_rows:
            raise AssertionError(f"No fake MySQL rows for query: {query}")
        return self.fetch_rows.pop(0)

    async def close(self) -> object:
        self.closed = True


class _RecordingMySQLConnector:
    def __init__(self, *connections: _RecordingMySQLConnection) -> None:
        self.connections = list(connections)
        self.credentials: list[dict[str, object]] = []

    async def __call__(
        self,
        credentials: Mapping[str, object],
    ) -> _RecordingMySQLConnection:
        self.credentials.append(dict(credentials))
        if not self.connections:
            raise AssertionError("Unexpected MySQL connection attempt.")
        return self.connections.pop(0)


class _RecordingSQLServerConnection:
    def __init__(self, fetch_values: list[object]) -> None:
        self.fetch_values = fetch_values
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def execute(self, query: str, *args: object) -> object:
        self.executed.append((query, args))
        return "OK"

    async def fetchval(self, query: str, *args: object) -> object:
        self.queries.append((query, args))
        if not self.fetch_values:
            raise AssertionError(f"No fake SQL Server response for query: {query}")
        return self.fetch_values.pop(0)

    async def close(self) -> object:
        self.closed = True


class _RecordingSQLServerConnector:
    def __init__(self, *connections: _RecordingSQLServerConnection) -> None:
        self.connections = list(connections)
        self.credentials: list[dict[str, object]] = []

    async def __call__(
        self,
        credentials: Mapping[str, object],
    ) -> _RecordingSQLServerConnection:
        self.credentials.append(dict(credentials))
        if not self.connections:
            raise AssertionError("Unexpected SQL Server connection attempt.")
        return self.connections.pop(0)


class _RecordingTestDatabaseConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def execute_query(
        self,
        query: str,
        values: list[object] | None = None,
    ) -> tuple[int, list[dict[str, object]]]:
        assert values is None
        self.queries.append(query)
        return 0, []


def _recorded_mssql_statements(
    *connections: _RecordingSQLServerConnection,
) -> str:
    return "\n".join(
        query for connection in connections for query, _args in connection.executed
    )


def _recorded_mssql_queries(
    *connections: _RecordingSQLServerConnection,
) -> str:
    return "\n".join(
        query for connection in connections for query, _args in connection.queries
    )


def _simulate_pymysql_format(query: str, args: tuple[object, ...]) -> str:
    return query % tuple("<param>" for _arg in args)


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported_modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    imported_modules.update(
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    return imported_modules


def _persistence_settings(
    tmp_path: Path,
    *,
    database_url: str | None = "sqlite:///local.sqlite3",
    database_connection: object | None = None,
    migrations_root: Path | None = None,
    modules: tuple[str, ...] = (),
) -> _PersistenceSettings:
    return _PersistenceSettings(
        database_url=database_url,
        database_connection=database_connection,
        migrations_root=migrations_root,
        configured_modules=modules,
    )


def _failed_check_descriptions(result_errors: tuple[str, ...]) -> str:
    return "\n".join(result_errors)


def _database_config_source(tmp_path: Path) -> MappingConfigSource:
    return MappingConfigSource(
        {
            "app": {
                "modules": ("wybra.db",),
                "database_url": sqlite_file_url(tmp_path / "app.sqlite3"),
            }
        }
    )


def _structured_database_config_source(
    tmp_path: Path,
    *,
    database: str = "structured.sqlite3",
) -> MappingConfigSource:
    return MappingConfigSource(
        {
            "app": {
                "modules": ("wybra.db",),
                "project_root": tmp_path,
            },
            "app.database": {
                "backend": "sqlite",
                "database": database,
            },
        }
    )


class TestDatabaseConfigurationAndRuntime:
    def test_wybra_db_package_imports(self) -> None:
        package = importlib.import_module("wybra.db")

        assert package.__name__ == "wybra.db"

    @pytest.mark.anyio
    async def test_wybra_db_setup_site_registers_database_capability(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(FastAPI(), config_source=_database_config_source(tmp_path))

        try:
            database = site.require_capability(DatabaseCapability)

            assert site.has_capability(DatabaseCapability) is True
            assert isinstance(database, DatabaseCapability)
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_database_capability_exposes_opaque_route_selection(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(FastAPI(), config_source=_database_config_source(tmp_path))
        database = site.require_capability(DatabaseCapability)
        try:
            route = database.database().default()

            assert route.database_name == "default"
            assert route.role == "default"
            assert not hasattr(route, "alias")
        finally:
            await database.close()

    def test_structured_database_routing_inherits_base_instance_settings(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "sqlite",
                            "database": "primary.sqlite3",
                            "reader_rotation": "queue",
                        },
                        "app.database.replica_one": {
                            "role": "reader",
                            "database": "replica.sqlite3",
                            "weight": 2,
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
            discover_module_config=False,
        )

        routing = resolve_database_routing_from_config(
            config,
            project_root=tmp_path,
        )

        assert routing is not None
        assert tuple(routing.connections) == ("default", "database_replica_one")
        registry = routing.build_registry()
        assert registry.connection().default().role == "default"
        assert registry.connection().for_read().role == "reader"
        assert registry.connection().for_write().role == "writer"

    def test_structured_database_routing_rejects_secondary_backend_override(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "sqlite",
                            "database": "primary.sqlite3",
                        },
                        "app.database.replica_one": {
                            "role": "reader",
                            "backend": "postgresql",
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
            discover_module_config=False,
        )

        with pytest.raises(ConfigurationError, match="backend is not allowed"):
            resolve_database_routing_from_config(config, project_root=tmp_path)

    @pytest.mark.parametrize(
        "field_name",
        ("default_rotation", "reader_rotation", "writer_rotation"),
    )
    def test_structured_database_routing_rejects_secondary_rotation_policy(
        self,
        tmp_path: Path,
        field_name: str,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "sqlite",
                            "database": "primary.sqlite3",
                        },
                        "app.database.replica_one": {
                            "role": "reader",
                            field_name: "random",
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
            discover_module_config=False,
        )

        with pytest.raises(ConfigurationError, match=field_name):
            resolve_database_routing_from_config(config, project_root=tmp_path)

    def test_structured_database_routing_reports_secondary_weight_context(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "sqlite",
                            "database": "primary.sqlite3",
                        },
                        "app.database.replica_one": {
                            "role": "reader",
                            "weight": 0,
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
            discover_module_config=False,
        )

        with pytest.raises(
            ConfigurationError,
            match=r"\[app\.database\.replica_one\]\.weight",
        ):
            resolve_database_routing_from_config(config, project_root=tmp_path)

    def test_reader_and_writer_fallbacks_use_default_selection_policy(self) -> None:
        registry = DatabaseRouteRegistry(
            (
                DatabaseRouteInstance(
                    name="default",
                    alias="first",
                    roles=frozenset({"default"}),
                ),
                DatabaseRouteInstance(
                    name="default",
                    alias="second",
                    roles=frozenset({"default"}),
                ),
            ),
            default_rotation="queue",
            reader_rotation="default",
            writer_rotation="random",
        )

        first_default = registry.connection().default()
        reader_fallback = registry.connection().for_read()
        writer_fallback = registry.connection().for_write()

        assert registry.alias_for(first_default) == "first"
        assert registry.alias_for(reader_fallback) == "second"
        assert registry.alias_for(writer_fallback) == "first"
        assert reader_fallback.role == "reader"
        assert writer_fallback.role == "writer"

    def test_weighted_selection_uses_configured_weights_without_expansion(
        self,
    ) -> None:
        registry = DatabaseRouteRegistry(
            (
                DatabaseRouteInstance(
                    name="default",
                    alias="primary",
                    roles=frozenset({"reader"}),
                    weight=10_000,
                ),
                DatabaseRouteInstance(
                    name="default",
                    alias="secondary",
                    roles=frozenset({"reader"}),
                    weight=1,
                ),
            ),
            reader_rotation="weighted",
        )

        for _ in range(10_000):
            assert registry.alias_for(registry.connection().for_read()) == "primary"
        assert registry.alias_for(registry.connection().for_read()) == "secondary"
        assert registry.alias_for(registry.connection().for_read()) == "primary"

    def test_reader_and_writer_policies_select_independently(self) -> None:
        registry = DatabaseRouteRegistry(
            (
                DatabaseRouteInstance(
                    name="default",
                    alias="reader_one",
                    roles=frozenset({"reader"}),
                ),
                DatabaseRouteInstance(
                    name="default",
                    alias="reader_two",
                    roles=frozenset({"reader"}),
                ),
                DatabaseRouteInstance(
                    name="default",
                    alias="writer_one",
                    roles=frozenset({"writer"}),
                    weight=2,
                ),
                DatabaseRouteInstance(
                    name="default",
                    alias="writer_two",
                    roles=frozenset({"writer"}),
                ),
            ),
            reader_rotation="queue",
            writer_rotation="weighted",
        )

        assert registry.alias_for(registry.connection().for_read()) == "reader_one"
        assert registry.alias_for(registry.connection().for_write()) == "writer_one"
        assert registry.alias_for(registry.connection().for_read()) == "reader_two"
        assert registry.alias_for(registry.connection().for_write()) == "writer_one"
        assert registry.alias_for(registry.connection().for_write()) == "writer_two"

    @pytest.mark.anyio
    async def test_failed_write_is_not_replayed_on_another_writer(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": ("wybra.db",),
                        "project_root": tmp_path,
                    },
                    "app.database": {
                        "backend": "sqlite",
                        "database": "default.sqlite3",
                    },
                    "app.database.writer_one": {
                        "role": "writer",
                        "database": "writer-one.sqlite3",
                    },
                    "app.database.writer_two": {
                        "role": "writer",
                        "database": "writer-two.sqlite3",
                    },
                }
            ),
        )
        database = site.require_capability(DatabaseCapability)
        try:
            first_writer = database.database().for_write()
            second_writer = database.database().for_write()

            with pytest.raises(OperationalError):
                await tortoise_connection(database, first_writer).execute_query(
                    "insert into missing_records values (1)"
                )

            _row_count, rows = await tortoise_connection(
                database, second_writer
            ).execute_query(
                "select name from sqlite_master where type = 'table' "
                "and name = 'missing_records'"
            )

            assert rows == []
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_route_bound_transaction_keeps_its_selected_writer(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": ("wybra.db",),
                        "project_root": tmp_path,
                    },
                    "app.database": {
                        "backend": "sqlite",
                        "database": "default.sqlite3",
                    },
                    "app.database.writer_one": {
                        "role": "writer",
                        "database": "writer-one.sqlite3",
                    },
                    "app.database.writer_two": {
                        "role": "writer",
                        "database": "writer-two.sqlite3",
                    },
                }
            ),
        )
        database = site.require_capability(DatabaseCapability)
        try:
            selected_writer = database.database().for_write()
            later_writer = database.database().for_write()

            async with tortoise_transaction(database, selected_writer) as connection:
                await connection.execute_script(
                    "create table transaction_records (value text not null)"
                )
                await connection.execute_query(
                    "insert into transaction_records values (?)", ["pinned"]
                )

            _row_count, selected_rows = await tortoise_connection(
                database, selected_writer
            ).execute_query("select value from transaction_records")
            _row_count, later_rows = await tortoise_connection(
                database, later_writer
            ).execute_query(
                "select name from sqlite_master where type = 'table' "
                "and name = 'transaction_records'"
            )

            assert [row["value"] for row in selected_rows] == ["pinned"]
            assert later_rows == []
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_explicit_writer_route_overrides_the_default_read_route(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": ("wybra.db",),
                        "project_root": tmp_path,
                    },
                    "app.database": {
                        "backend": "sqlite",
                        "database": "default.sqlite3",
                    },
                    "app.database.reader": {
                        "role": "reader",
                        "database": "reader.sqlite3",
                    },
                    "app.database.writer": {
                        "role": "writer",
                        "database": "writer.sqlite3",
                    },
                }
            ),
        )
        database = site.require_capability(DatabaseCapability)
        try:
            reader_route = database.database().for_read()
            writer_route = database.database().for_write()

            await tortoise_connection(database, writer_route).execute_script(
                "create table writer_records (value text not null)"
            )
            await tortoise_connection(database, writer_route).execute_query(
                "insert into writer_records values (?)", ["writer"]
            )
            _row_count, writer_rows = await tortoise_connection(
                database, writer_route
            ).execute_query("select value from writer_records")
            _row_count, reader_rows = await tortoise_connection(
                database, reader_route
            ).execute_query(
                "select name from sqlite_master where type = 'table' "
                "and name = 'writer_records'"
            )

            assert [row["value"] for row in writer_rows] == ["writer"]
            assert reader_rows == []
        finally:
            await database.close()

    def test_load_and_adaptive_selection_use_route_metrics(self) -> None:
        registry = DatabaseRouteRegistry(
            (
                DatabaseRouteInstance(
                    name="default",
                    alias="reader_one",
                    roles=frozenset({"reader"}),
                ),
                DatabaseRouteInstance(
                    name="default",
                    alias="reader_two",
                    roles=frozenset({"reader"}),
                ),
                DatabaseRouteInstance(
                    name="default",
                    alias="fallback",
                    roles=frozenset({"default"}),
                ),
            ),
            reader_rotation="load",
        )
        first_reader = registry.connection().for_read()
        started = registry.begin_statement(first_reader)

        assert registry.alias_for(registry.connection().for_read()) == "reader_two"

        registry.end_statement(first_reader, started)
        registry.reader_rotation = "adaptive"
        slow_reader = registry.connection().for_read()
        slow_started = registry.begin_statement(slow_reader)
        registry.end_statement(slow_reader, slow_started)

        assert registry.alias_for(registry.connection().for_read()) == "reader_two"

    @pytest.mark.anyio
    async def test_tortoise_uses_a_default_role_instance_for_static_fallback(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": ("wybra.db",),
                        "project_root": tmp_path,
                    },
                    "app.database": {
                        "backend": "sqlite",
                        "database": "reader.sqlite3",
                        "role": "reader",
                    },
                    "app.database.primary": {
                        "database": "writer.sqlite3",
                        "role": "default,writer",
                    },
                }
            ),
        )
        database = site.require_capability(DatabaseCapability)
        try:
            assert isinstance(database, WybraDatabaseCapability)
            applications = database._database.config["apps"]
            assert isinstance(applications, dict)
            assert {
                application["default_connection"]
                for application in applications.values()
            } == {"database_primary"}
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_database_capability_records_statement_and_transaction_metrics(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(FastAPI(), config_source=_database_config_source(tmp_path))
        database = site.require_capability(DatabaseCapability)
        try:
            assert isinstance(database, WybraDatabaseCapability)
            route = database.database().for_write()
            connection = tortoise_connection(database, route)

            await connection.execute_query("select 1")

            active_statements, active_transactions, latency = (
                database._database.routes.metrics(route)
            )
            assert active_statements == 0
            assert active_transactions == 0
            assert latency is not None

            async with tortoise_transaction(database, route) as transaction:
                assert database._database.routes.metrics(route)[1] == 1
                await transaction.execute_query("select 1")

            assert database._database.routes.metrics(route)[1] == 0
            assert database._database.routes.metrics(route)[2] is not None

            with database._database.context:
                async with in_transaction(
                    database._database.routes.alias_for(route)
                ) as transaction:
                    assert database._database.routes.metrics(route)[1] == 1
                    await transaction.execute_query("select 1")

            assert database._database.routes.metrics(route)[1] == 0
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_wybra_db_setup_site_resolves_relative_database_url(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": ("wybra.db",),
                        "project_root": tmp_path,
                        "database_url": "sqlite:///relative.sqlite3",
                    }
                }
            ),
        )
        database = site.require_capability(DatabaseCapability)
        try:
            async with tortoise_transaction(
                database,
                database.database().for_write(),
            ) as connection:
                await connection.execute_script(
                    "CREATE TABLE runtime_probe (id INTEGER)"
                )

            assert (tmp_path / "relative.sqlite3").exists()
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_wybra_db_setup_site_uses_structured_sqlite_config(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=_structured_database_config_source(tmp_path),
        )
        database = site.require_capability(DatabaseCapability)
        try:
            async with tortoise_transaction(
                database,
                database.database().for_write(),
            ) as connection:
                await connection.execute_script(
                    "CREATE TABLE structured_probe (id INTEGER)"
                )

            assert (tmp_path / "structured.sqlite3").exists()
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_wybra_db_setup_site_requires_database_url(self) -> None:
        with pytest.raises(SiteCapabilityError, match="database_url"):
            await start(
                FastAPI(),
                config_source=MappingConfigSource({"app": {"modules": ("wybra.db",)}}),
            )

    def test_structured_database_config_builds_sqlite_connection(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService([_structured_database_config_source(tmp_path)])

        connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
        )

        assert connection is not None
        assert connection.tortoise_connection_config == {
            "engine": "tortoise.backends.sqlite",
            "credentials": {
                "file_path": (tmp_path / "structured.sqlite3").resolve().as_posix()
            },
        }

    def test_structured_sqlite_config_resolves_relative_path_from_project_root(
        self,
        tmp_path: Path,
    ) -> None:
        config_directory = tmp_path / "config"
        project_root = tmp_path / "project"
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app": {
                            "project_root": project_root,
                        },
                        "app.database": {
                            "backend": "sqlite",
                            "database": "structured.sqlite3",
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_connection_from_config(
            config,
            project_root=project_root,
        )

        assert config_directory != project_root
        assert connection is not None
        assert (
            connection.credentials["file_path"]
            == (project_root / "structured.sqlite3").resolve().as_posix()
        )

    def test_database_configuration_rejects_structured_and_url_sources(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app": {
                            "modules": ("wybra.db",),
                            "database_url": "sqlite:///direct.sqlite3",
                        },
                        "app.database": {
                            "backend": "sqlite",
                            "database": "structured.sqlite3",
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        with pytest.raises(
            ConfigurationError,
            match=r"either \[app\.database\] or \[app\]\.database_url",
        ):
            resolve_database_connection_from_config(
                config,
                project_root=tmp_path,
            )

    def test_database_url_environment_overrides_structured_database_config(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        env_url = sqlite_file_url(tmp_path / "env.sqlite3")
        ConfigService.set_runtime_environment({ENV_DATABASE_URL: env_url})
        config = ConfigService(
            [_structured_database_config_source(tmp_path)],
            config_defs=(db_module_config,),
        )

        with caplog.at_level("INFO", logger="wybra.db.settings"):
            connection = resolve_database_connection_from_config(
                config,
                project_root=tmp_path,
            )

        assert connection is not None
        assert connection.source == "url"
        assert connection.database_url == env_url
        assert "Using DATABASE_URL, overriding config" in caplog.text

    def test_structured_database_config_resolves_environment_credentials(
        self,
        tmp_path: Path,
    ) -> None:
        ConfigService.set_runtime_environment(
            {
                "WYBRA_DB_USER": "app_user",
                "WYBRA_DB_PASSWORD": "app_password",
            }
        )
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "postgresql",
                            "host": "/var/run/postgresql",
                            "database": "uniquode",
                            "credential_source": "environment",
                            "user_key": "WYBRA_DB_USER",
                            "password_key": "WYBRA_DB_PASSWORD",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
        )

        assert connection is not None
        assert connection.tortoise_connection_config == {
            "engine": "tortoise.backends.asyncpg",
            "credentials": {
                "database": "uniquode",
                "host": "/var/run/postgresql",
                "user": "app_user",
                "password": "app_password",
            },
        }

    def test_structured_mariadb_config_uses_tortoise_mysql_backend(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "mariadb",
                            "host": "db.example",
                            "database": "uniquode",
                            "user": "app_user",
                            "password": "app_password",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
        )

        assert connection is not None
        assert connection.backend.scheme == "mariadb"
        assert connection.tortoise_connection_config == {
            "engine": "tortoise.backends.mysql",
            "credentials": {
                "database": "uniquode",
                "host": "db.example",
                "user": "app_user",
                "password": "app_password",
            },
        }

    def test_structured_mssql_config_defaults_tortoise_driver(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "mssql",
                            "host": "db.example",
                            "port": 1433,
                            "database": "uniquode",
                            "user": "app_user",
                            "password": "app_password",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
        )

        assert connection is not None
        assert connection.tortoise_connection_config == {
            "engine": "tortoise.backends.mssql",
            "credentials": {
                "database": "uniquode",
                "host": "db.example",
                "port": 1433,
                "user": "app_user",
                "password": "app_password",
                "driver": "ODBC Driver 18 for SQL Server",
            },
        }

    def test_structured_database_config_applies_database_aws_overrides(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.aws": {
                            "region": "ap-southeast-2",
                            "profile": "shared-profile",
                            "partition": "aws-us-gov",
                            "role_arn": "arn:aws-us-gov:iam::123456789012:role/wybra",
                            "role_session_name": "shared-session",
                            "sso_region": "us-east-1",
                            "sso_account_id": "123456789012",
                            "sso_role_name": "DatabaseAccess",
                            "sso_start_url": "https://example.awsapps.com/start",
                        },
                        "app.database": {
                            "backend": "postgresql",
                            "host": "rds-postgresql.example.aws",
                            "database": "uniquode",
                            "user": "app_user",
                            "password": "app_password",
                        },
                        "app.database.aws": {
                            "managed": "rds",
                            "region": "ap-south-1",
                            "db_instance_identifier": "uniquode-postgresql",
                            "engine": "postgres",
                            "endpoint": "rds-postgresql.example.aws",
                            "port": 5432,
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
        )

        assert connection is not None
        assert connection.aws is not None
        assert connection.aws.managed == "rds"
        assert connection.aws.db_instance_identifier == "uniquode-postgresql"
        assert connection.aws.client.region == "ap-south-1"
        assert connection.aws.client.profile == "shared-profile"
        assert connection.aws.client.partition == "aws-us-gov"
        assert (
            connection.aws.client.role_arn
            == "arn:aws-us-gov:iam::123456789012:role/wybra"
        )
        assert connection.aws.client.role_session_name == "shared-session"
        assert connection.aws.client.sso_region == "us-east-1"
        assert connection.aws.endpoint == "rds-postgresql.example.aws"
        assert connection.aws.port == 5432

    def test_structured_database_metadata_preserves_database_aws_settings(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.aws": {
                            "region": "ap-southeast-2",
                            "profile": "shared-profile",
                        },
                        "app.database": {
                            "backend": "postgresql",
                            "host": "rds-postgresql.example.aws",
                            "database": "uniquode",
                        },
                        "app.database.aws": {
                            "managed": "rds",
                            "region": "ap-south-1",
                            "db_instance_identifier": "uniquode-postgresql",
                            "engine": "postgres",
                            "endpoint": "rds-postgresql.example.aws",
                            "port": 5432,
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = database_connection_metadata_from_config(
            config,
            project_root=tmp_path,
        )

        assert connection is not None
        assert connection.aws is not None
        assert connection.aws.managed == "rds"
        assert connection.aws.db_instance_identifier == "uniquode-postgresql"
        assert connection.aws.client.region == "ap-south-1"
        assert connection.aws.client.profile == "shared-profile"
        assert connection.aws.endpoint == "rds-postgresql.example.aws"
        assert connection.aws.port == 5432
        assert "user" not in connection.credentials
        assert "password" not in connection.credentials

    def test_database_aws_external_id_key_is_resolved_for_service_account_only(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.aws": {
                            "region": "ap-southeast-2",
                            "role_arn": "arn:aws:iam::123456789012:role/wybra",
                            "external_id_source": "keychain",
                            "external_id_key": "aws/rds/external-id",
                        },
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                            "user": "app_user",
                            "password": "app_password",
                            "sa_user": "service_user",
                            "sa_password": "service_password",
                        },
                        "app.database.aws": {
                            "managed": "aurora",
                            "cluster_identifier": "uniquode-cluster",
                            "engine": "aurora-postgresql",
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
        )
        secrets = _RecordingSecretsCapability(
            {("keychain", "aws/rds/external-id"): "resolved-external-id"}
        )

        runtime_connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
        )
        service_account_connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
            secrets=secrets,
            purpose="service_account",
        )

        assert runtime_connection is not None
        assert runtime_connection.aws is not None
        assert runtime_connection.aws.client.external_id is None
        assert runtime_connection.aws.client.external_id_key == "aws/rds/external-id"
        assert service_account_connection is not None
        assert service_account_connection.aws is not None
        assert (
            service_account_connection.aws.client.external_id == "resolved-external-id"
        )
        assert service_account_connection.aws.client.external_id_source is None
        assert service_account_connection.aws.client.external_id_key is None
        assert service_account_connection.credentials["user"] == "service_user"

    def test_database_aws_external_id_key_resolution_reports_source_and_key(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.aws": {
                            "region": "ap-southeast-2",
                            "role_arn": "arn:aws:iam::123456789012:role/wybra",
                            "external_id_source": "keychain",
                            "external_id_key": "aws/rds/external-id",
                        },
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                            "user": "app_user",
                            "password": "app_password",
                            "sa_user": "service_user",
                            "sa_password": "service_password",
                        },
                        "app.database.aws": {
                            "managed": "aurora",
                            "cluster_identifier": "uniquode-cluster",
                            "engine": "aurora-postgresql",
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        with pytest.raises(
            ConfigurationError,
            match=r"source=keychain key=aws/rds/external-id",
        ):
            resolve_database_connection_from_config(
                config,
                project_root=tmp_path,
                secrets=_FailingSecretsCapability(),
                purpose="service_account",
            )

    def test_shared_aws_config_error_names_app_aws_section(
        self, tmp_path: Path
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.aws": {"region": " "},
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                        },
                        "app.database.aws": {
                            "managed": "rds",
                            "db_instance_identifier": "uniquode-postgresql",
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        with pytest.raises(
            ConfigurationError,
            match=r"\[app\.aws\]\.region must be a non-blank string",
        ):
            resolve_database_connection_from_config(config, project_root=tmp_path)

    def test_database_aws_config_error_names_database_aws_section(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                        },
                        "app.database.aws": {
                            "managed": "rds",
                            "db_instance_identifier": "uniquode-postgresql",
                            "endpoint": " ",
                        },
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        with pytest.raises(
            ConfigurationError,
            match=r"\[app\.database\.aws\]\.endpoint must be a non-blank string",
        ):
            resolve_database_connection_from_config(config, project_root=tmp_path)

    def test_project_settings_resolve_environment_database_credentials(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        config_path.write_text(
            """
            [app]
            modules = ["wybra.db"]

            [app.templates]
            auto_reload = false
            cache_size = 400

            [app.assets]
            url_path = "/static/"

            [app.database]
            backend = "postgresql"
            database = "uniquode"
            credential_source = "environment"
            user_key = "UNIQUODE_DB_USER"
            password_key = "UNIQUODE_DB_PASSWORD"
            """,
            encoding="utf-8",
        )

        settings = load_project_settings(
            project_root=tmp_path,
            environ={
                "APP_CONFIG": config_path.as_posix(),
                "UNIQUODE_DB_USER": "app_user",
                "UNIQUODE_DB_PASSWORD": "app_password",
            },
            read_dotenv=False,
        )

        assert settings.database_connection is not None
        assert settings.database_connection.tortoise_connection_config == {
            "engine": "tortoise.backends.asyncpg",
            "credentials": {
                "database": "uniquode",
                "user": "app_user",
                "password": "app_password",
            },
        }

    def test_project_settings_resolve_service_account_database_credentials(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        config_path.write_text(
            """
            [app]
            modules = ["wybra.db"]

            [app.templates]
            auto_reload = false
            cache_size = 400

            [app.assets]
            url_path = "/static/"

            [app.database]
            backend = "postgresql"
            database = "uniquode"
            user = "app_user"
            password = "app_password"
            sa_user = "admin_user"
            sa_password = "admin_password"
            """,
            encoding="utf-8",
        )

        settings = load_project_settings(
            project_root=tmp_path,
            environ={"APP_CONFIG": config_path.as_posix()},
            read_dotenv=False,
            database_credential_purpose="service_account",
        )

        assert settings.database_connection is not None
        assert settings.database_connection.tortoise_connection_config == {
            "engine": "tortoise.backends.asyncpg",
            "credentials": {
                "database": "uniquode",
                "user": "admin_user",
                "password": "admin_password",
            },
        }
        assert settings.database_connection.sa_database == "postgres"

    def test_project_settings_resolve_service_account_database_override(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        config_path.write_text(
            """
            [app]
            modules = ["wybra.db"]

            [app.templates]
            auto_reload = false
            cache_size = 400

            [app.assets]
            url_path = "/static/"

            [app.database]
            backend = "postgresql"
            database = "uniquode"
            sa_database = "cluster_admin"
            user = "app_user"
            password = "app_password"
            sa_user = "admin_user"
            sa_password = "admin_password"
            """,
            encoding="utf-8",
        )

        settings = load_project_settings(
            project_root=tmp_path,
            environ={"APP_CONFIG": config_path.as_posix()},
            read_dotenv=False,
            database_credential_purpose="service_account",
        )

        assert settings.database_connection is not None
        assert settings.database_connection.credentials["database"] == "uniquode"
        assert settings.database_connection.sa_database == "cluster_admin"

    def test_structured_postgresql_config_rejects_target_as_service_database(
        self,
    ) -> None:
        with pytest.raises(ConfigurationError, match="sa_database must differ"):
            StructuredDatabaseConfig.from_values(
                {
                    "backend": "postgresql",
                    "database": "postgres",
                }
            )

    def test_project_settings_service_account_can_fallback_to_runtime_credentials(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        config_path.write_text(
            """
            [app]
            modules = ["wybra.db"]

            [app.templates]
            auto_reload = false
            cache_size = 400

            [app.assets]
            url_path = "/static/"

            [app.database]
            backend = "postgresql"
            database = "uniquode"
            user = "app_user"
            password = "app_password"
            """,
            encoding="utf-8",
        )

        settings = load_project_settings(
            project_root=tmp_path,
            environ={"APP_CONFIG": config_path.as_posix()},
            read_dotenv=False,
            database_credential_purpose="service_account",
            fallback_to_runtime_credentials=True,
        )

        assert settings.database_connection is not None
        assert settings.database_connection.tortoise_connection_config == {
            "engine": "tortoise.backends.asyncpg",
            "credentials": {
                "database": "uniquode",
                "user": "app_user",
                "password": "app_password",
            },
        }

    def test_structured_database_config_resolves_secret_credentials(
        self,
        tmp_path: Path,
    ) -> None:
        ConfigService.set_runtime_environment({"WYBRA_DB_USER": "app_user"})
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                            "credential_source": "keychain",
                            "user_key": "database/app/user",
                            "password_key": "database/app/password",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )
        secrets = _RecordingSecretsCapability(
            {
                ("keychain", "database/app/user"): "app_user",
                ("keychain", "database/app/password"): "app_password",
            }
        )

        connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
            secrets=secrets,
        )

        assert connection is not None
        assert connection.credentials["user"] == "app_user"
        assert connection.credentials["password"] == "app_password"
        assert "app_password" not in repr(connection)
        assert "app_password" not in connection.redacted_description

    def test_structured_database_config_exposes_credential_references(self) -> None:
        config = StructuredDatabaseConfig.from_values(
            {
                "backend": "postgresql",
                "database": "uniquode",
                "credential_source": "keychain",
                "user_key": "database/app/user",
                "password_key": "database/app/password",
                "sa_user_key": "database/app/service-account-user",
                "sa_password_key": "database/app/service-account-password",
            }
        )

        references = config.credential_references()

        assert [
            (
                reference.name,
                reference.key,
                reference.owner,
                reference.source,
                reference.required,
                reference.description,
            )
            for reference in references
        ] == [
            (
                "database-user",
                "database/app/user",
                "database",
                "keychain",
                True,
                "Configured runtime database username.",
            ),
            (
                "database-password",
                "database/app/password",
                "database",
                "keychain",
                True,
                "Configured runtime database password.",
            ),
            (
                "database-sa-user",
                "database/app/service-account-user",
                "database",
                "keychain",
                True,
                "Configured database service-account username for provisioning.",
            ),
            (
                "database-sa-password",
                "database/app/service-account-password",
                "database",
                "keychain",
                True,
                "Configured database service-account password for provisioning.",
            ),
        ]
        assert all(not hasattr(reference, "value") for reference in references)

    def test_structured_database_config_defaults_keychain_credential_references(
        self,
    ) -> None:
        config = StructuredDatabaseConfig.from_values(
            {
                "backend": "postgresql",
                "database": "uniquode",
                "credential_source": "keychain",
            }
        )

        references = config.credential_references()

        assert {reference.name: reference.key for reference in references} == {
            "database-user": "database/uniquode/app/user",
            "database-password": "database/uniquode/app/password",
            "database-sa-user": "database/uniquode/service-account/user",
            "database-sa-password": ("database/uniquode/service-account/password"),
        }

    def test_structured_database_config_default_keys_allow_unicode_database_name(
        self,
    ) -> None:
        config = StructuredDatabaseConfig.from_values(
            {
                "backend": "postgresql",
                "database": "uniquodé",
                "credential_source": "keychain",
            }
        )

        assert {
            reference.name: reference.key
            for reference in config.credential_references()
        }["database-user"] == "database/uniquodé/app/user"

    @pytest.mark.parametrize("database", ("tenant/prod", "tenant prod", "tenant\nprod"))
    def test_structured_database_config_rejects_unsafe_default_key_database_segment(
        self,
        database: str,
    ) -> None:
        with pytest.raises(
            ConfigurationError,
            match="Database name cannot be used in default credential keys",
        ):
            StructuredDatabaseConfig.from_values(
                {
                    "backend": "postgresql",
                    "database": database,
                    "credential_source": "keychain",
                }
            )

    def test_structured_database_config_allows_unsafe_name_with_explicit_keys(
        self,
    ) -> None:
        config = StructuredDatabaseConfig.from_values(
            {
                "backend": "postgresql",
                "database": "tenant/prod",
                "credential_source": "keychain",
                "user_key": "database/tenant-prod/app/user",
                "password_key": "database/tenant-prod/app/password",
                "sa_user_key": "database/tenant-prod/service-account/user",
                "sa_password_key": "database/tenant-prod/service-account/password",
            }
        )

        assert {
            reference.name: reference.key
            for reference in config.credential_references()
        }["database-user"] == "database/tenant-prod/app/user"

    @pytest.mark.parametrize(
        ("credential_fields", "expected_keys"),
        (
            (
                {"user_key": "custom/app/user"},
                {
                    "database-user": "custom/app/user",
                    "database-password": "database/uniquode/app/password",
                    "database-sa-user": ("database/uniquode/service-account/user"),
                    "database-sa-password": (
                        "database/uniquode/service-account/password"
                    ),
                },
            ),
            (
                {"sa_password_key": "custom/service-account/password"},
                {
                    "database-user": "database/uniquode/app/user",
                    "database-password": "database/uniquode/app/password",
                    "database-sa-user": ("database/uniquode/service-account/user"),
                    "database-sa-password": "custom/service-account/password",
                },
            ),
            (
                {"user": "plain_user", "password_key": "custom/app/password"},
                {
                    "database-password": "custom/app/password",
                    "database-sa-user": ("database/uniquode/service-account/user"),
                    "database-sa-password": (
                        "database/uniquode/service-account/password"
                    ),
                },
            ),
        ),
    )
    def test_structured_database_config_exposes_partial_credential_references(
        self,
        credential_fields: dict[str, str],
        expected_keys: dict[str, str],
    ) -> None:
        config = StructuredDatabaseConfig.from_values(
            {
                "backend": "postgresql",
                "database": "uniquode",
                "credential_source": "keychain",
                **credential_fields,
            }
        )

        assert {
            reference.name: reference.key
            for reference in config.credential_references()
        } == expected_keys

    def test_structured_database_config_preserves_non_keychain_credential_source(
        self,
    ) -> None:
        config = StructuredDatabaseConfig.from_values(
            {
                "backend": "postgresql",
                "database": "uniquode",
                "credential_source": "environment",
                "user_key": "WYBRA_DB_USER",
                "password_key": "WYBRA_DB_PASSWORD",
            }
        )

        references = config.credential_references()

        assert [reference.name for reference in references] == [
            "database-user",
            "database-password",
        ]
        assert {reference.source for reference in references} == {"environment"}

    def test_effective_database_config_delegates_credential_references(
        self,
        tmp_path: Path,
    ) -> None:
        structured = StructuredDatabaseConfig.from_values(
            {
                "backend": "postgresql",
                "database": "uniquode",
                "credential_source": "keychain",
                "user_key": "database/app/user",
            }
        )
        effective = EffectiveDatabaseConfig.from_structured(
            structured,
            project_root=tmp_path,
        )

        assert effective.credential_references() == structured.credential_references()

    def test_effective_database_url_config_exposes_no_credential_references(
        self,
        tmp_path: Path,
    ) -> None:
        effective = EffectiveDatabaseConfig.from_url(
            "postgresql://app_user:secret@db.example/uniquode",
            project_root=tmp_path,
        )

        assert effective.credential_references() == ()

    def test_structured_sqlite_config_exposes_no_credential_references(self) -> None:
        config = StructuredDatabaseConfig.from_values(
            {
                "backend": "sqlite",
                "database": "local.sqlite3",
                "credential_source": "keychain",
                "user_key": "database/app/user",
                "password_key": "database/app/password",
            }
        )

        assert config.credential_references() == ()

    def test_structured_database_config_rejects_plain_and_key_credentials(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                            "credential_source": "environment",
                            "user": "plain_user",
                            "user_key": "WYBRA_DB_USER",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        with pytest.raises(ConfigurationError, match="mutually exclusive"):
            resolve_database_connection_from_config(config, project_root=tmp_path)

    def test_structured_database_config_ignores_keys_without_credential_source(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                            "user": "plain_user",
                            "user_key": "WYBRA_DB_USER",
                            "password_key": "WYBRA_DB_PASSWORD",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_connection_from_config(
            config, project_root=tmp_path
        )

        assert connection is not None
        assert connection.credentials == {
            "database": "uniquode",
            "user": "plain_user",
        }

    def test_structured_database_config_rejects_credential_source_without_keys(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                            "credential_source": "environment",
                            "user": "database/app/user",
                            "password": "database/app/password",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        with pytest.raises(
            ConfigurationError,
            match="missing credential keys",
        ):
            resolve_database_connection_from_config(config, project_root=tmp_path)

    @pytest.mark.parametrize(
        "credential_fields",
        (
            {"user": "app_user"},
            {"password": "app_password"},
            {"user": "app_user", "user_key": "WYBRA_DB_USER"},
            {"credential_source": "environment", "user_key": "WYBRA_DB_USER"},
            {"credential_source": "keychain", "user_key": "database/app/user"},
            {"sa_user": "admin_user"},
            {"sa_password": "admin_password"},
        ),
    )
    def test_structured_sqlite_config_ignores_credentials(
        self,
        tmp_path: Path,
        credential_fields: dict[str, str],
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "sqlite",
                            "database": "structured.sqlite3",
                            **credential_fields,
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_connection_from_config(
            config, project_root=tmp_path
        )

        assert connection is not None
        assert connection.credentials == {
            "file_path": (tmp_path / "structured.sqlite3").resolve().as_posix()
        }

    @pytest.mark.parametrize(
        "sqlite_fields",
        (
            {"host": "db.internal"},
            {"port": 5432},
            {"host": "db.internal", "port": 5432},
        ),
    )
    def test_structured_sqlite_config_ignores_network_fields(
        self,
        tmp_path: Path,
        sqlite_fields: dict[str, str | int],
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "sqlite",
                            "database": "structured.sqlite3",
                            **sqlite_fields,
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_connection_from_config(
            config, project_root=tmp_path
        )

        assert connection is not None
        assert connection.credentials == {
            "file_path": (tmp_path / "structured.sqlite3").resolve().as_posix()
        }

    def test_service_account_credentials_are_separate_from_runtime_credentials(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                            "user": "app_user",
                            "password": "app_password",
                            "sa_user": "admin_user",
                            "sa_password": "admin_password",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        runtime_connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
        )
        service_account_connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
            purpose="service_account",
        )

        assert runtime_connection is not None
        assert service_account_connection is not None
        assert runtime_connection.credentials["user"] == "app_user"
        assert runtime_connection.credentials["password"] == "app_password"
        assert service_account_connection.credentials["user"] == "admin_user"
        assert service_account_connection.credentials["password"] == "admin_password"

    def test_runtime_connection_does_not_require_service_account_secret_source(
        self,
        tmp_path: Path,
    ) -> None:
        config = {
            "app.database": {
                "backend": "postgresql",
                "database": "uniquode",
                "credential_source": "keychain",
                "user": "app_user",
                "password": "app_password",
                "sa_user_key": "database/app/service-account-user",
                "sa_password_key": "database/app/service-account-password",
            }
        }

        connection = resolve_database_connection_from_config(
            config,
            project_root=tmp_path,
        )

        assert connection is not None
        assert connection.credentials["user"] == "app_user"
        assert connection.credentials["password"] == "app_password"

    def test_service_account_connection_requires_secret_source_for_service_keys(
        self,
        tmp_path: Path,
    ) -> None:
        config = {
            "app.database": {
                "backend": "postgresql",
                "database": "uniquode",
                "credential_source": "keychain",
                "user": "app_user",
                "password": "app_password",
                "sa_user_key": "database/app/service-account-user",
                "sa_password_key": "database/app/service-account-password",
            }
        }

        with pytest.raises(
            ConfigurationError,
            match="SecretsCapability is required to resolve database credentials",
        ):
            resolve_database_connection_from_config(
                config,
                project_root=tmp_path,
                purpose="service_account",
            )

    def test_provisioning_connection_resolves_service_account_keys(
        self,
        tmp_path: Path,
    ) -> None:
        ConfigService.set_runtime_environment(
            {
                "WYBRA_DB_ADMIN_USER": "admin_user",
                "WYBRA_DB_ADMIN_PASSWORD": "admin_password",
            }
        )
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                            "credential_source": "environment",
                            "user": "app_user",
                            "password": "app_password",
                            "sa_user_key": "WYBRA_DB_ADMIN_USER",
                            "sa_password_key": "WYBRA_DB_ADMIN_PASSWORD",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_provisioning_connection_from_config(
            config,
            project_root=tmp_path,
        )

        assert connection.credentials["user"] == "admin_user"
        assert connection.credentials["password"] == "admin_password"

    def test_provisioning_connection_rejects_runtime_credentials_only(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app.database": {
                            "backend": "postgresql",
                            "database": "uniquode",
                            "user": "app_user",
                            "password": "app_password",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        with pytest.raises(ConfigurationError, match="service-account database user"):
            resolve_database_provisioning_connection_from_config(
                config,
                project_root=tmp_path,
            )

    def test_provisioning_connection_accepts_sqlite_backend(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [_structured_database_config_source(tmp_path)],
            config_defs=(db_module_config,),
        )

        connection = resolve_database_provisioning_connection_from_config(
            config,
            project_root=tmp_path,
        )

        assert connection.source == "structured"
        assert connection.backend.tortoise_scheme == "sqlite"
        assert (
            connection.credentials["file_path"]
            == (tmp_path / "structured.sqlite3").as_posix()
        )

    def test_provisioning_connection_rejects_application_database_url(
        self,
        tmp_path: Path,
    ) -> None:
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "app": {
                            "database_url": "postgresql://admin:secret@db.example/app",
                        }
                    }
                )
            ],
            config_defs=(db_module_config,),
        )

        with pytest.raises(
            ConfigurationError,
            match="does not use application database_url",
        ):
            resolve_database_provisioning_connection_from_config(
                config,
                project_root=tmp_path,
            )

    @pytest.mark.anyio
    async def test_database_capability_provides_clean_sessions(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(FastAPI(), config_source=_database_config_source(tmp_path))
        database = site.require_capability(DatabaseCapability)
        try:
            first_connection = tortoise_connection(
                database,
                database.database().default(),
            )
            second_connection = tortoise_connection(
                database,
                database.database().default(),
            )

            assert first_connection is second_connection
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_database_capability_transaction_commits_and_rolls_back(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(FastAPI(), config_source=_database_config_source(tmp_path))
        database = site.require_capability(DatabaseCapability)
        try:
            async with tortoise_transaction(
                database,
                database.database().for_write(),
            ) as connection:
                await connection.execute_script(
                    "create table records (value text not null)"
                )
                await connection.execute_query(
                    "insert into records values (?)",
                    ["committed"],
                )

            with pytest.raises(RuntimeError, match="rollback"):
                async with tortoise_transaction(
                    database,
                    database.database().for_write(),
                ) as connection:
                    await connection.execute_query(
                        "insert into records values (?)",
                        ["rolled-back"],
                    )
                    raise RuntimeError("rollback")

            _row_count, rows = await tortoise_connection(
                database,
                database.database().default(),
            ).execute_query("select value from records order by value")

            assert [row["value"] for row in rows] == ["committed"]
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_database_capability_selects_default_read_and_write_routes(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(FastAPI(), config_source=_database_config_source(tmp_path))
        database = site.require_capability(DatabaseCapability)
        try:
            default_route = database.database().default()
            reader_route = database.database().for_read()
            writer_route = database.database().for_write()

            default_connection = tortoise_connection(database, default_route)
            assert tortoise_connection(database, reader_route) is default_connection
            assert tortoise_connection(database, writer_route) is default_connection
            async with tortoise_transaction(
                database, writer_route
            ) as writer_connection:
                assert callable(writer_connection.execute_query)
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_close_database_connections_does_not_reconnect_cross_loop_client(
        self,
        tmp_path: Path,
    ) -> None:
        database = await create_database(
            sqlite_file_url(tmp_path / "loop-switch.sqlite3"),
            modules=("wybra.db",),
        )
        connection = database.connection()
        other_loop = asyncio.new_event_loop()
        try:
            connection._bound_loop = other_loop

            def fail_create_connection(_alias: str) -> BaseDBAsyncClient:
                raise AssertionError("close must not create replacement connections")

            database.context.connections._create_connection = fail_create_connection

            await close_database_connections(database)

            assert database._connections == []
            assert database.context.connections._copy_storage() == {}
        finally:
            other_loop.close()
            if database._connections:
                await close_database_connections(database)

    @pytest.mark.anyio
    async def test_close_database_connections_restores_connection_factory(
        self,
        tmp_path: Path,
    ) -> None:
        database = await create_database(
            sqlite_file_url(tmp_path / "restore-factory.sqlite3"),
            modules=("wybra.db",),
        )

        try:
            assert "_create_connection" in database.context.connections.__dict__

            await close_database_connections(database)

            assert "_create_connection" not in database.context.connections.__dict__
        finally:
            if database._connections:
                await close_database_connections(database)

    @pytest.mark.anyio
    async def test_close_database_connections_can_keep_tracking_active(
        self,
        tmp_path: Path,
    ) -> None:
        database = await create_database(
            sqlite_file_url(tmp_path / "keep-tracking.sqlite3"),
            modules=("wybra.db",),
        )

        try:
            first_connection = database.connection()
            assert first_connection in database._connections

            await close_database_connections(database, restore_create_connection=False)

            assert database._connections == []
            assert "_create_connection" in database.context.connections.__dict__

            second_connection = database.connection()

            assert second_connection in database._connections
            assert second_connection is not first_connection

            await close_database(database)

            assert "_create_connection" not in database.context.connections.__dict__
            assert database._connections == []
        finally:
            if database._connections:
                await close_database(database)

    @pytest.mark.anyio
    async def test_create_close_database_cycles_do_not_rewrap_connection_factory(
        self,
        tmp_path: Path,
    ) -> None:
        for index in range(2):
            database = await create_database(
                sqlite_file_url(tmp_path / f"cycle-{index}.sqlite3"),
                modules=("wybra.db",),
            )

            try:
                database.connection()
                await close_database(database)

                assert "_create_connection" not in database.context.connections.__dict__
                assert database._connections == []
            finally:
                if database._connections:
                    await close_database_connections(database)

    @pytest.mark.anyio
    async def test_database_capability_rejects_unknown_connection_name(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(FastAPI(), config_source=_database_config_source(tmp_path))
        database = site.require_capability(DatabaseCapability)
        try:
            with pytest.raises(
                DatabaseCapabilityError, match="Unknown logical database"
            ):
                database.database("analytics")
        finally:
            await database.close()

    @pytest.mark.anyio
    async def test_database_capability_rejects_use_after_close(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(FastAPI(), config_source=_database_config_source(tmp_path))
        database = site.require_capability(DatabaseCapability)

        await database.close()

        with pytest.raises(
            DatabaseCapabilityError, match="Database capability is closed"
        ):
            database.database()

    @pytest.mark.anyio
    async def test_database_capability_attempts_all_distinct_closes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        first_database = cast(Database, object())
        closed_databases: list[Database] = []

        async def close_or_fail(database: Database) -> None:
            closed_databases.append(database)
            raise RuntimeError("close failed")

        monkeypatch.setattr(
            "wybra.db.capabilities.close_database",
            close_or_fail,
        )
        database = WybraDatabaseCapability(first_database)

        with pytest.raises(DatabaseCapabilityError, match="error_count=1"):
            await database.close()

        assert closed_databases == [first_database]

    def test_wybra_db_modules_do_not_import_application_or_auth_packages(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        forbidden_modules = ("wybra.auth", "host_app")
        wybra_db_files = sorted((project_root / "src/wybra/db").rglob("*.py"))

        assert wybra_db_files
        for path in wybra_db_files:
            imported_modules = _imported_modules(path)
            assert not any(
                module == forbidden_module or module.startswith(f"{forbidden_module}.")
                for module in imported_modules
                for forbidden_module in forbidden_modules
            )

    def test_wybra_db_package_is_included_in_build_modules(self) -> None:
        pyproject = tomllib.loads(
            (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()
        )

        assert "wybra" in pyproject["tool"]["uv"]["build-backend"]["module-name"]

    def test_wybra_db_models_exposes_tortoise_model_base(self) -> None:
        assert WybraModel is Model

    def test_wybra_db_owns_database_url_helpers(self, tmp_path: Path) -> None:
        database_url = resolve_database_url("sqlite:///local.sqlite3", tmp_path)

        sqlite_url = parse_sqlite_database_url(database_url)

        assert sqlite_url is not None
        assert sqlite_url.path == tmp_path / "local.sqlite3"
        assert sqlite_url.is_absolute is True
        assert (
            redact_database_url("postgresql://user:password@host.example/app")
            == "postgresql://***:***@host.example/app"
        )

    def test_database_url_parser_handles_windows_absolute_sqlite_path(self) -> None:
        sqlite_url = parse_sqlite_database_url("sqlite:///C:/data/app.sqlite3")

        assert sqlite_url is not None
        assert sqlite_url.path.as_posix() == "C:/data/app.sqlite3"
        assert sqlite_url.is_absolute is True

    @pytest.mark.parametrize(
        "database_url",
        (
            "sqlite:////tmp/absolute.sqlite3",
            "sqlite:///C:/data/app.sqlite3",
            "postgresql://user:password@example.test/app",
        ),
    )
    def test_resolve_database_url_leaves_absolute_and_non_sqlite_urls_unchanged(
        self,
        tmp_path: Path,
        database_url: str,
    ) -> None:
        assert resolve_database_url(database_url, tmp_path) == database_url

    @pytest.mark.parametrize(
        ("database_url", "suffix"),
        (
            ("sqlite:///app.db", ""),
            ("sqlite:///app.db?cache=shared", "?cache=shared"),
            ("sqlite:///app.db?mode=rwc#fragment", "?mode=rwc#fragment"),
        ),
    )
    def test_resolve_database_url_preserves_relative_suffix(
        self,
        tmp_path: Path,
        database_url: str,
        suffix: str,
    ) -> None:
        assert resolve_database_url(database_url, tmp_path) == (
            f"{sqlite_file_url(tmp_path / 'app.db')}{suffix}"
        )

    @pytest.mark.parametrize(
        ("database_url", "expected_error"),
        (
            ("", "Database URL must not be empty."),
            (
                "ftp://example.com/database",
                "Database URL must use a supported Tortoise database scheme:",
            ),
            (
                "sqlite://:memory:",
                "SQLite database URL must not force in-memory storage.",
            ),
        ),
    )
    def test_validate_persistence_reports_database_url_failures(
        self,
        tmp_path: Path,
        database_url: str,
        expected_error: str,
    ) -> None:
        result = validate_persistence(
            _persistence_settings(tmp_path, database_url=database_url)
        )

        assert any(expected_error in error for error in result.errors)
        assert not result.is_ok

    def test_validate_persistence_reports_missing_database_connection(
        self,
        tmp_path: Path,
    ) -> None:
        result = validate_persistence(
            _persistence_settings(tmp_path, database_url=None)
        )

        assert any(
            check.description == "database connection is not configured"
            and not check.passed
            for check in result.checks
        )
        assert "Database connection must be configured." in result.errors
        assert not result.is_ok

    def test_supported_database_url_schemes_cover_tortoise_backends(self) -> None:
        assert supported_database_url_schemes() == (
            "sqlite",
            "postgresql",
            "postgres",
            "asyncpg",
            "psycopg",
            "mysql",
            "mariadb",
            "mssql",
            "oracle",
        )

    @pytest.mark.parametrize(
        ("scheme", "expected_family"),
        (
            ("sqlite", "sqlite"),
            ("postgresql", "postgresql"),
            ("postgres", "postgresql"),
            ("asyncpg", "postgresql"),
            ("psycopg", "postgresql"),
            ("mysql", "mysql"),
            ("mariadb", "mariadb"),
            ("mssql", "mssql"),
            ("oracle", "oracle"),
        ),
    )
    def test_database_provisioning_family_uses_backend_family(
        self,
        scheme: str,
        expected_family: str,
    ) -> None:
        backend = database_urls.database_backend_for_scheme(scheme)

        assert backend is not None
        assert database_family_for_backend(backend) == expected_family

    @pytest.mark.parametrize(
        ("engine", "expected_family"),
        (
            ("postgres", "postgresql"),
            ("postgresql", "postgresql"),
            ("aurora-postgresql", "postgresql"),
            ("mysql", "mysql"),
            ("aurora", "mysql"),
            ("aurora-mysql", "mysql"),
            ("mariadb", "mariadb"),
            ("sqlserver-ee", "mssql"),
        ),
    )
    def test_database_family_for_aws_engine_maps_supported_engines(
        self,
        engine: str,
        expected_family: str,
    ) -> None:
        assert database_family_for_aws_engine(engine) == expected_family

    @pytest.mark.parametrize("engine", ("oracle-ee", "db2"))
    def test_database_family_for_aws_engine_rejects_unsupported_engines(
        self,
        engine: str,
    ) -> None:
        with pytest.raises(
            DatabaseProvisioningConfigurationError, match="Unsupported|Oracle"
        ):
            database_family_for_aws_engine(engine)


def _aws_postgresql_connections(
    tmp_path: Path,
    *,
    endpoint: str = "uniquode.rds.amazonaws.com",
    port: int = 5432,
) -> tuple[ResolvedDatabaseConnection, ResolvedDatabaseConnection]:
    config = ConfigService(
        [
            MappingConfigSource(
                {
                    "app.aws": {
                        "region": "ap-southeast-2",
                        "account_id": "123456789012",
                    },
                    "app.database": {
                        "backend": "postgresql",
                        "host": endpoint,
                        "port": port,
                        "database": "uniquode",
                        "user": "app_user",
                        "password": "app_password",
                        "sa_user": "service_user",
                        "sa_password": "service_password",
                    },
                    "app.database.aws": {
                        "managed": "rds",
                        "db_instance_identifier": "uniquode-postgresql",
                        "engine": "postgres",
                    },
                }
            )
        ],
        config_defs=(db_module_config,),
    )
    runtime_connection = resolve_database_connection_from_config(
        config,
        project_root=tmp_path,
    )
    provisioning_connection = resolve_database_connection_from_config(
        config,
        project_root=tmp_path,
        purpose="service_account",
    )

    assert runtime_connection is not None
    assert provisioning_connection is not None
    return runtime_connection, provisioning_connection


class TestAwsDatabaseMetadata:
    def test_aws_managed_database_context_validates_target_metadata(
        self,
        tmp_path: Path,
    ) -> None:
        runtime_connection, provisioning_connection = _aws_postgresql_connections(
            tmp_path
        )
        metadata_client = _RecordingAwsRdsMetadataClient(
            AwsManagedDatabaseMetadata(
                managed=cast(Any, "RDS"),
                identifier="uniquode-postgresql",
                engine="postgres",
                endpoint="uniquode.rds.amazonaws.com",
                port=5432,
                arn="arn:aws:rds:ap-southeast-2:123456789012:db:uniquode-postgresql",
            )
        )

        context = provisioning_context(
            runtime_connection=runtime_connection,
            provisioning_connection=provisioning_connection,
            project_root=tmp_path,
            modules=(),
            aws_metadata_client=metadata_client,
        )

        assert context.family == "postgresql"
        assert len(metadata_client.targets) == 1
        target = metadata_client.targets[0]
        assert target.db_instance_identifier == "uniquode-postgresql"
        assert target.client.region == "ap-southeast-2"

    def test_boto3_rds_metadata_client_does_not_resolve_external_id_key(self) -> None:
        boto3 = _FakeBoto3Module()
        client = Boto3RdsMetadataClient(import_module=lambda _name: boto3)
        metadata = client.describe(
            AwsManagedDatabaseSettings(
                managed="rds",
                client=AwsClientSettings(
                    region="ap-southeast-2",
                    role_arn="arn:aws:iam::123456789012:role/wybra",
                    external_id_source="keychain",
                    external_id_key="aws/rds/external-id",
                ),
                db_instance_identifier="uniquode-postgresql",
            )
        )

        assert metadata.identifier == "uniquode-postgresql"
        assert boto3.assume_role_calls == [
            {
                "RoleArn": "arn:aws:iam::123456789012:role/wybra",
                "RoleSessionName": "wybra-database-provisioning",
            }
        ]

    @pytest.mark.parametrize(
        ("metadata", "expected_message"),
        (
            (
                AwsManagedDatabaseMetadata(
                    managed="rds",
                    identifier="uniquode-postgresql",
                    engine="mysql",
                    endpoint="uniquode.rds.amazonaws.com",
                    port=5432,
                ),
                "engine does not match",
            ),
            (
                AwsManagedDatabaseMetadata(
                    managed="rds",
                    identifier="uniquode-postgresql",
                    engine="postgres",
                    endpoint="other.rds.amazonaws.com",
                    port=5432,
                ),
                "endpoint mismatch",
            ),
            (
                AwsManagedDatabaseMetadata(
                    managed="rds",
                    identifier="uniquode-postgresql",
                    engine="postgres",
                    endpoint="uniquode.rds.amazonaws.com",
                    port=6543,
                ),
                "port mismatch",
            ),
        ),
    )
    def test_aws_managed_database_context_rejects_target_mismatch(
        self,
        tmp_path: Path,
        metadata: AwsManagedDatabaseMetadata,
        expected_message: str,
    ) -> None:
        runtime_connection, provisioning_connection = _aws_postgresql_connections(
            tmp_path
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match=expected_message,
        ) as exc_info:
            provisioning_context(
                runtime_connection=runtime_connection,
                provisioning_connection=provisioning_connection,
                project_root=tmp_path,
                modules=(),
                aws_metadata_client=_RecordingAwsRdsMetadataClient(metadata),
            )

        assert "app_password" not in str(exc_info.value)
        assert "service_password" not in str(exc_info.value)

    def test_boto3_rds_metadata_client_reports_missing_optional_dependency(
        self,
        tmp_path: Path,
    ) -> None:
        runtime_connection, _ = _aws_postgresql_connections(tmp_path)
        assert runtime_connection.aws is not None

        def missing_import(name: str) -> object:
            raise ImportError(name)

        client = Boto3RdsMetadataClient(import_module=missing_import)

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match=r"wybra\[aws\]",
        ):
            client.describe(runtime_connection.aws)

    def test_database_provisioner_registry_rejects_unsupported_family(self) -> None:
        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="Unsupported database family",
        ):
            provisioner_for_family(cast(DatabaseFamily, "firebird"), {})


def _database_connection(
    scheme: str,
    credentials: dict[str, object],
) -> ResolvedDatabaseConnection:
    backend = database_urls.database_backend_for_scheme(scheme)

    assert backend is not None
    return ResolvedDatabaseConnection.from_structured(
        backend=backend,
        credentials=credentials,
    )


def _sqlite_provisioning_context() -> ProvisioningContext:
    return ProvisioningContext(
        family="sqlite",
        runtime_connection=_database_connection("sqlite", {"file_path": ":memory:"}),
        provisioning_connection=_database_connection(
            "sqlite",
            {"file_path": ":memory:"},
        ),
        project_root=Path.cwd(),
        modules=(),
    )


def _postgresql_provisioning_context(
    *,
    provisioning_connection: ResolvedDatabaseConnection | None,
    runtime_credentials: dict[str, object] | None = None,
) -> ProvisioningContext:
    return ProvisioningContext(
        family="postgresql",
        runtime_connection=_database_connection(
            "postgresql",
            runtime_credentials
            or {"database": "app", "user": "app", "password": "secret"},
        ),
        provisioning_connection=provisioning_connection,
        project_root=Path.cwd(),
        modules=(),
    )


def _mysql_provisioning_context(
    *,
    provisioning_connection: ResolvedDatabaseConnection | None,
    runtime_credentials: dict[str, object] | None = None,
) -> ProvisioningContext:
    return ProvisioningContext(
        family="mysql",
        runtime_connection=_database_connection(
            "mysql",
            runtime_credentials
            or {"database": "app", "user": "app", "password": "secret"},
        ),
        provisioning_connection=provisioning_connection,
        project_root=Path.cwd(),
        modules=(),
    )


def _mariadb_provisioning_context(
    *,
    provisioning_connection: ResolvedDatabaseConnection | None,
    runtime_credentials: dict[str, object] | None = None,
) -> ProvisioningContext:
    return ProvisioningContext(
        family="mariadb",
        runtime_connection=_database_connection(
            "mariadb",
            runtime_credentials
            or {"database": "app", "user": "app", "password": "secret"},
        ),
        provisioning_connection=provisioning_connection,
        project_root=Path.cwd(),
        modules=(),
    )


def _mssql_provisioning_context(
    *,
    provisioning_connection: ResolvedDatabaseConnection | None,
    runtime_credentials: dict[str, object] | None = None,
) -> ProvisioningContext:
    return ProvisioningContext(
        family="mssql",
        runtime_connection=_database_connection(
            "mssql",
            runtime_credentials
            or {
                "database": "app",
                "schema": "app_schema",
                "role": "app_role",
                "user": "app",
                "password": "secret",
            },
        ),
        provisioning_connection=provisioning_connection,
        project_root=Path.cwd(),
        modules=(),
    )


class TestDatabaseProvisioning:
    @pytest.mark.anyio
    @pytest.mark.parametrize(
        ("database_url", "expected_queries"),
        (
            (
                "sqlite://:memory:",
                (
                    "PRAGMA foreign_keys = OFF",
                    'DELETE FROM "child"',
                    'DELETE FROM "parent"',
                    "PRAGMA foreign_keys = ON",
                ),
            ),
            (
                "postgresql://user:password@db.example/app",
                ('TRUNCATE TABLE "child", "parent" RESTART IDENTITY CASCADE',),
            ),
            (
                "mysql://user:password@db.example/app",
                (
                    "SET FOREIGN_KEY_CHECKS = 0",
                    "DELETE FROM `child`",
                    "DELETE FROM `parent`",
                    "SET FOREIGN_KEY_CHECKS = 1",
                ),
            ),
            (
                "mariadb://user:password@db.example/app",
                (
                    "SET FOREIGN_KEY_CHECKS = 0",
                    "DELETE FROM `child`",
                    "DELETE FROM `parent`",
                    "SET FOREIGN_KEY_CHECKS = 1",
                ),
            ),
            (
                "mssql://user:password@db.example/app",
                (
                    "ALTER TABLE [child] NOCHECK CONSTRAINT ALL",
                    "ALTER TABLE [parent] NOCHECK CONSTRAINT ALL",
                    "DELETE FROM [child]",
                    "DELETE FROM [parent]",
                    "ALTER TABLE [child] WITH CHECK CHECK CONSTRAINT ALL",
                    "ALTER TABLE [parent] WITH CHECK CHECK CONSTRAINT ALL",
                ),
            ),
        ),
    )
    async def test_clear_test_database_data_uses_backend_specific_cleanup(
        self,
        database_url: str,
        expected_queries: tuple[str, ...],
    ) -> None:
        connection = _RecordingTestDatabaseConnection()

        await clear_test_database_data(
            cast(BaseDBAsyncClient, connection),
            database_url=database_url,
            table_names=("child", "parent"),
        )

        assert connection.queries == list(expected_queries)

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        ("credentials", "expected_message"),
        (
            (None, "requires service-account credentials"),
            (
                {"database": "app", "password": "secret"},
                "requires a service-account database user",
            ),
            (
                {"database": "app", "user": "app_sa"},
                "requires a service-account database password",
            ),
        ),
    )
    async def test_unsupported_database_provisioner_requires_service_account_credentials(  # noqa: E501
        self,
        credentials: dict[str, object] | None,
        expected_message: str,
    ) -> None:
        provisioner = UnsupportedFamilyProvisioner("mysql")
        context = _mysql_provisioning_context(
            provisioning_connection=(
                None
                if credentials is None
                else _database_connection("mysql", credentials)
            ),
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match=expected_message,
        ):
            await provisioner.initialise(context)
        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match=expected_message,
        ):
            await provisioner.destroy(context, DestroyDatabaseRequest(confirm="app"))

    @pytest.mark.anyio
    async def test_unsupported_database_provisioner_rejects_unimplemented_operations(
        self,
    ) -> None:
        provisioner = UnsupportedFamilyProvisioner("mysql")
        context = ProvisioningContext(
            family="mysql",
            runtime_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app", "password": "secret"},
            ),
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "secret"},
            ),
            project_root=Path.cwd(),
            modules=(),
        )

        with pytest.raises(
            DatabaseProvisioningOperationError,
            match="mysql init provisioning is not implemented",
        ):
            await provisioner.initialise(context)
        with pytest.raises(
            DatabaseProvisioningOperationError,
            match="mysql destroy is not implemented",
        ):
            await provisioner.destroy(context, DestroyDatabaseRequest(confirm="app"))
        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="Unknown mysql maintenance task",
        ):
            await provisioner.run_maintenance(
                context,
                DatabaseMaintenanceRequest(task="vacuum"),
            )

    def test_mssql_provisioner_is_registered_for_mssql_family(self) -> None:
        assert isinstance(provisioner_for_family("mssql"), SQLServerProvisioner)

    @pytest.mark.anyio
    async def test_mssql_provisioner_initialises_database_principals_and_grants(
        self,
    ) -> None:
        maintenance = _RecordingSQLServerConnection(
            [
                0,  # database exists
                0,  # login exists
            ]
        )
        target = _RecordingSQLServerConnection(
            [
                0,  # schema exists
                0,  # database user exists
                0,  # role exists
                0,  # role membership exists
                0,  # migration recorder table exists
            ]
        )
        connector = _RecordingSQLServerConnector(maintenance, target)
        provisioner = SQLServerProvisioner(connector=connector)
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {
                    "database": "app",
                    "host": "db.example",
                    "user": "app_sa",
                    "password": "admin_secret",
                },
            ),
        )

        results = await provisioner.initialise(context)

        assert connector.credentials == [
            {
                "database": "master",
                "host": "db.example",
                "user": "app_sa",
                "password": "admin_secret",
            },
            {
                "database": "app",
                "host": "db.example",
                "user": "app_sa",
                "password": "admin_secret",
            },
        ]
        assert [(result.status, result.phase) for result in results] == [
            ("created", "init"),
            ("created", "init"),
            ("created", "init"),
            ("created", "init"),
            ("created", "init"),
            ("updated", "init"),
            ("noop", "init"),
        ]
        executed_sql = _recorded_mssql_statements(maintenance, target)
        assert "CREATE DATABASE [app]" in executed_sql
        assert "CREATE LOGIN ' + QUOTENAME(?) +" in executed_sql
        assert "WITH PASSWORD = N" in executed_sql
        assert "CREATE SCHEMA [app_schema]" in executed_sql
        assert (
            "CREATE USER [app] FOR LOGIN [app] WITH DEFAULT_SCHEMA = [app_schema]"
            in executed_sql
        )
        assert "CREATE ROLE [app_role]" in executed_sql
        assert "ALTER ROLE [app_role] ADD MEMBER [app]" in executed_sql
        assert (
            "GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::[app_schema] TO [app_role]"
        ) in executed_sql
        assert "secret" not in executed_sql
        assert "secret" not in " ".join(result.message for result in results)
        assert maintenance.executed[1][1] == ("app", "secret")
        assert maintenance.closed
        assert target.closed

    @pytest.mark.anyio
    async def test_mssql_provisioner_reuses_existing_objects_and_reports_migrations(
        self,
    ) -> None:
        maintenance = _RecordingSQLServerConnection(
            [
                1,  # database exists
                1,  # login exists
            ]
        )
        target = _RecordingSQLServerConnection(
            [
                1,  # schema exists
                1,  # database user exists
                1,  # role exists
                1,  # role membership exists
                1,  # migration recorder table exists
                "2",  # migration count
            ]
        )
        connector = _RecordingSQLServerConnector(maintenance, target)
        provisioner = SQLServerProvisioner(connector=connector)
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.initialise(context)

        assert [result.status for result in results] == [
            "skipped",
            "skipped",
            "skipped",
            "skipped",
            "skipped",
            "updated",
            "noop",
        ]
        assert any("contains 2 record" in result.message for result in results)
        executed_sql = _recorded_mssql_statements(maintenance, target)
        assert "CREATE DATABASE" not in executed_sql
        assert "CREATE LOGIN" not in executed_sql
        assert "CREATE SCHEMA" not in executed_sql
        assert "CREATE USER" not in executed_sql
        assert "CREATE ROLE" not in executed_sql
        assert "ALTER LOGIN [app] WITH PASSWORD = ?" not in executed_sql
        assert "ALTER USER [app] WITH LOGIN = [app]" in executed_sql
        assert "ALTER ROLE [app_role] ADD MEMBER [app]" not in executed_sql

    @pytest.mark.anyio
    async def test_mssql_init_reports_external_login_prerequisite_without_password(
        self,
    ) -> None:
        maintenance = _RecordingSQLServerConnection(
            [
                1,  # externally managed login exists
                0,  # database exists
            ]
        )
        target = _RecordingSQLServerConnection(
            [
                0,  # schema exists
                0,  # database user exists
                0,  # role exists
                0,  # role membership exists
                0,  # migration recorder table exists
            ]
        )
        connector = _RecordingSQLServerConnector(maintenance, target)
        provisioner = SQLServerProvisioner(connector=connector)
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
            runtime_credentials={
                "database": "app",
                "schema": "app_schema",
                "role": "app_role",
                "user": "app",
            },
        )

        results = await provisioner.initialise(context)

        assert [(result.status, result.phase) for result in results] == [
            ("created", "init"),
            ("noop", "init"),
            ("created", "init"),
            ("created", "init"),
            ("created", "init"),
            ("updated", "init"),
            ("noop", "init"),
        ]
        assert any("externally managed" in result.message for result in results)
        assert connector.credentials == [
            {"database": "master", "user": "app_sa", "password": "admin_secret"},
            {"database": "app", "user": "app_sa", "password": "admin_secret"},
        ]
        executed_sql = _recorded_mssql_statements(target)
        assert (
            "CREATE USER [app] FOR LOGIN [app] WITH DEFAULT_SCHEMA = [app_schema]"
            in executed_sql
        )
        assert maintenance.closed
        assert target.closed

    @pytest.mark.anyio
    async def test_mssql_init_requires_existing_external_login_before_database_changes(
        self,
    ) -> None:
        maintenance = _RecordingSQLServerConnection(
            [
                0,  # externally managed login exists
            ]
        )
        connector = _RecordingSQLServerConnector(maintenance)
        provisioner = SQLServerProvisioner(connector=connector)
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
            runtime_credentials={
                "database": "app",
                "schema": "app_schema",
                "role": "app_role",
                "user": "app",
            },
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="application login must exist",
        ):
            await provisioner.initialise(context)

        executed_sql = _recorded_mssql_statements(maintenance)
        assert "CREATE DATABASE" not in executed_sql
        assert maintenance.closed

    @pytest.mark.anyio
    async def test_mssql_destroy_requires_confirmed_database_and_service_database(
        self,
    ) -> None:
        provisioner = SQLServerProvisioner(
            connector=_RecordingSQLServerConnector(_RecordingSQLServerConnection([]))
        )
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="confirmation does not match",
        ):
            await provisioner.destroy(context, DestroyDatabaseRequest(confirm="other"))

        service_connection = context.provisioning_connection
        assert service_connection is not None
        context = _mssql_provisioning_context(
            provisioning_connection=ResolvedDatabaseConnection.from_structured(
                backend=service_connection.backend,
                credentials=service_connection.credentials,
                sa_database="app",
            ),
        )
        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="target database must differ",
        ):
            await provisioner.destroy(context, DestroyDatabaseRequest(confirm="app"))

    @pytest.mark.anyio
    async def test_mssql_destroy_removes_configured_database_and_safe_login(
        self,
    ) -> None:
        maintenance = _RecordingSQLServerConnection(
            [
                1,  # database exists
                1,  # login exists
                0,  # login has server role memberships
                0,  # login has non-baseline server permissions
            ]
        )
        connector = _RecordingSQLServerConnector(maintenance)
        provisioner = SQLServerProvisioner(connector=connector)
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        assert [(result.status, result.phase) for result in results] == [
            ("removed", "destroy"),
            ("removed", "destroy"),
        ]
        executed_sql = _recorded_mssql_statements(maintenance)
        assert "ALTER DATABASE [app] SET SINGLE_USER WITH ROLLBACK IMMEDIATE" in (
            executed_sql
        )
        assert "DROP DATABASE [app]" in executed_sql
        assert "DROP LOGIN [app]" in executed_sql
        queried_sql = _recorded_mssql_queries(maintenance)
        assert "permission_name <> ?" in queried_sql
        assert any(
            args == ("app", "CONNECT SQL") for _query, args in maintenance.queries
        )

    @pytest.mark.anyio
    async def test_mssql_destroy_skips_service_account_runtime_login(self) -> None:
        maintenance = _RecordingSQLServerConnection(
            [
                1,  # database exists
                1,  # login exists
            ]
        )
        connector = _RecordingSQLServerConnector(maintenance)
        provisioner = SQLServerProvisioner(connector=connector)
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        executed_sql = _recorded_mssql_statements(maintenance)
        assert "DROP DATABASE [app]" in executed_sql
        assert "DROP LOGIN" not in executed_sql
        assert any("users are the same" in result.message for result in results)

    @pytest.mark.anyio
    async def test_mssql_destroy_skips_login_with_external_dependencies(self) -> None:
        maintenance = _RecordingSQLServerConnection(
            [
                1,  # database exists
                1,  # login exists
                0,  # login has server role memberships
                1,  # login has non-baseline server permissions
            ]
        )
        connector = _RecordingSQLServerConnector(maintenance)
        provisioner = SQLServerProvisioner(connector=connector)
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        executed_sql = _recorded_mssql_statements(maintenance)
        assert "DROP DATABASE [app]" in executed_sql
        assert "DROP LOGIN" not in executed_sql
        assert any("dependencies were detected" in result.message for result in results)

    @pytest.mark.anyio
    async def test_mssql_maintenance_tasks_execute_privilege_repair_and_state(
        self,
    ) -> None:
        repair_connection = _RecordingSQLServerConnection([0])
        state_connection = _RecordingSQLServerConnection(
            [
                1,  # migration recorder table exists
                0,  # migration count
            ]
        )
        prerequisite_connection = _RecordingSQLServerConnection([1])
        connector = _RecordingSQLServerConnector(
            repair_connection,
            state_connection,
            prerequisite_connection,
        )
        provisioner = SQLServerProvisioner(connector=connector)
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        assert [task.name for task in provisioner.maintenance_tasks(context)] == [
            "repair-privs",
            "migrations",
            "prerequisites",
        ]
        repair_result = await provisioner.run_maintenance(
            context,
            DatabaseMaintenanceRequest(task="repair-privs"),
        )
        state_result = await provisioner.run_maintenance(
            context,
            DatabaseMaintenanceRequest(task="migrations"),
        )
        prerequisite_result = await provisioner.run_maintenance(
            context,
            DatabaseMaintenanceRequest(task="prerequisites"),
        )

        assert repair_result[0].status == "updated"
        assert "GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::[app_schema]" in (
            _recorded_mssql_statements(repair_connection)
        )
        assert state_result[0].message == "Tortoise migration recorder table is empty."
        assert prerequisite_result[0].phase == "maintenance"
        assert "managed application login exists" in prerequisite_result[0].message

    @pytest.mark.anyio
    async def test_mssql_validate_prerequisites_reports_missing_external_login(
        self,
    ) -> None:
        prerequisite_connection = _RecordingSQLServerConnection([0])
        connector = _RecordingSQLServerConnector(prerequisite_connection)
        provisioner = SQLServerProvisioner(connector=connector)
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
            runtime_credentials={
                "database": "app",
                "schema": "app_schema",
                "role": "app_role",
                "user": "app",
            },
        )

        result = await provisioner.run_maintenance(
            context,
            DatabaseMaintenanceRequest(task="prerequisites"),
        )

        assert result[0].phase == "maintenance"
        assert "externally managed" in result[0].message

    def test_mssql_driver_credentials_escape_dsn_values(self) -> None:
        credentials = mssql_provisioning._driver_credentials(
            {
                "driver": "Driver}18",
                "host": "db.example;Encrypt=no",
                "port": "1433",
                "database": "app;other=value",
                "user": "app;UID=other",
                "password": "secret}value;Trusted_Connection=yes",
                " Encrypt ": "yes",
            }
        )

        assert credentials["autocommit"] is True
        assert credentials["dsn"] == (
            "DRIVER={Driver}}18};SERVER={db.example;Encrypt=no,1433};"
            "DATABASE={app;other=value};UID={app;UID=other};"
            "PWD={secret}}value;Trusted_Connection=yes};Encrypt={yes}"
        )

    def test_mssql_driver_credentials_reject_unsupported_odbc_attributes(self) -> None:
        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="Unsupported SQL Server ODBC attribute",
        ):
            mssql_provisioning._driver_credentials(
                {
                    "database": "app",
                    "user": "app",
                    "password": "secret",
                    "Trusted_Connection": "yes",
                }
            )

    @pytest.mark.anyio
    async def test_mssql_provisioner_reports_missing_driver(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def import_module(_module_name: str) -> object:
            raise ImportError("missing")

        monkeypatch.setattr(
            mssql_provisioning.importlib, "import_module", import_module
        )
        provisioner = SQLServerProvisioner()
        context = _mssql_provisioning_context(
            provisioning_connection=_database_connection(
                "mssql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match=r"wybra\[mssql\]",
        ):
            await provisioner.initialise(context)

    def test_mysql_provisioner_is_registered_for_mysql_family(self) -> None:
        assert isinstance(provisioner_for_family("mysql"), MySQLProvisioner)

    def test_mariadb_provisioner_is_registered_for_mariadb_family(self) -> None:
        assert isinstance(provisioner_for_family("mariadb"), MariaDBProvisioner)

    @pytest.mark.anyio
    async def test_mysql_provisioner_initialises_database_user_and_grants(self) -> None:
        connection = _RecordingMySQLConnection(
            [
                0,  # database exists
                0,  # user exists
                0,  # migration recorder table exists
            ]
        )
        connector = _RecordingMySQLConnector(connection)
        provisioner = MySQLProvisioner(connector=connector)
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {
                    "database": "app",
                    "host": "db.example",
                    "user": "app_sa",
                    "password": "admin_secret",
                },
            ),
        )

        results = await provisioner.initialise(context)

        assert connector.credentials == [
            {
                "host": "db.example",
                "user": "app_sa",
                "password": "admin_secret",
            }
        ]
        assert [(result.status, result.phase) for result in results] == [
            ("created", "init"),
            ("created", "init"),
            ("skipped", "init"),
            ("noop", "init"),
        ]
        executed_sql = "\n".join(query for query, _args in connection.executed)
        assert "CREATE DATABASE `app`" in executed_sql
        assert "CREATE USER 'app'@'%%' IDENTIFIED BY %s" in executed_sql
        assert "GRANT SELECT, INSERT, UPDATE, DELETE ON `app`.* TO 'app'@'%'" in (
            executed_sql
        )
        assert "secret" not in executed_sql
        assert "secret" not in " ".join(result.message for result in results)
        assert connection.executed[1][1] == ("secret",)
        assert _simulate_pymysql_format(*connection.executed[1]) == (
            "CREATE USER 'app'@'%' IDENTIFIED BY <param>"
        )
        grant_query, grant_args = connection.executed[2]
        assert grant_args == ()
        assert "TO 'app'@'%'" in grant_query
        assert connection.closed

    @pytest.mark.anyio
    async def test_mysql_provisioner_escapes_percent_usernames_in_parameterised_sql(
        self,
    ) -> None:
        connection = _RecordingMySQLConnection(
            [
                0,  # database exists
                0,  # user exists
                0,  # migration recorder table exists
            ]
        )
        provisioner = MySQLProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
            runtime_credentials={
                "database": "app",
                "user": "app%ops",
                "password": "secret",
            },
        )

        await provisioner.initialise(context)

        assert connection.executed[1][0] == (
            "CREATE USER 'app%%ops'@'%%' IDENTIFIED BY %s"
        )
        assert _simulate_pymysql_format(*connection.executed[1]) == (
            "CREATE USER 'app%ops'@'%' IDENTIFIED BY <param>"
        )
        grant_query, grant_args = connection.executed[2]
        assert grant_args == ()
        assert "TO 'app%ops'@'%'" in grant_query

    @pytest.mark.anyio
    async def test_mysql_provisioner_reuses_existing_objects_and_reports_migrations(
        self,
    ) -> None:
        connection = _RecordingMySQLConnection(
            [
                1,  # database exists
                1,  # user exists
                1,  # migration recorder table exists
                "2",  # migration count
            ]
        )
        provisioner = MySQLProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.initialise(context)

        assert [result.status for result in results] == [
            "skipped",
            "skipped",
            "skipped",
            "noop",
        ]
        assert any(
            result.message == "Refreshed MySQL application user password: app"
            for result in results
        )
        assert any("contains 2 record" in result.message for result in results)
        executed_sql = "\n".join(query for query, _args in connection.executed)
        assert "CREATE DATABASE" not in executed_sql
        assert "CREATE USER" not in executed_sql
        assert "ALTER USER 'app'@'%%' IDENTIFIED BY %s" in executed_sql
        assert "GRANT SELECT, INSERT, UPDATE, DELETE ON `app`.* TO 'app'@'%'" in (
            executed_sql
        )
        assert _simulate_pymysql_format(*connection.executed[0]) == (
            "ALTER USER 'app'@'%' IDENTIFIED BY <param>"
        )

    @pytest.mark.anyio
    async def test_mysql_provisioner_rejects_invalid_count_result(self) -> None:
        connection = _RecordingMySQLConnection(["not-count"])
        provisioner = MySQLProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        with pytest.raises(
            DatabaseProvisioningOperationError,
            match="MySQL count result was invalid.",
        ):
            await provisioner.initialise(context)

        assert connection.closed

    @pytest.mark.anyio
    async def test_mysql_destroy_requires_confirmed_database(self) -> None:
        provisioner = MySQLProvisioner(
            connector=_RecordingMySQLConnector(_RecordingMySQLConnection([]))
        )
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="confirmation does not match",
        ):
            await provisioner.destroy(context, DestroyDatabaseRequest(confirm="other"))

    @pytest.mark.anyio
    async def test_mysql_destroy_removes_configured_database_and_safe_user(
        self,
    ) -> None:
        connection = _RecordingMySQLConnection(
            [
                1,  # database exists
                1,  # user exists
                99,  # current connection id
            ],
            fetch_rows=[
                (
                    ("GRANT USAGE ON *.* TO 'app'@'%'",),
                    ("GRANT SELECT, INSERT, UPDATE, DELETE ON `app`.* TO 'app'@'%'",),
                ),
                ((101,),),
            ],
        )
        provisioner = MySQLProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        assert [(result.status, result.phase) for result in results] == [
            ("removed", "destroy"),
            ("removed", "destroy"),
        ]
        executed_sql = "\n".join(query for query, _args in connection.executed)
        assert "KILL 101" in executed_sql
        assert "DROP DATABASE `app`" in executed_sql
        assert "DROP USER 'app'@'%'" in executed_sql
        show_grants_query, show_grants_args = connection.queries[2]
        assert show_grants_query == "SHOW GRANTS FOR 'app'@'%'"
        assert show_grants_args == ()
        processlist_query, processlist_args = connection.queries[4]
        assert "FROM performance_schema.threads" in processlist_query
        assert processlist_args == ("app", 99)

    @pytest.mark.anyio
    async def test_mysql_destroy_falls_back_to_information_schema_processlist(
        self,
    ) -> None:
        class ProcessListFallbackConnection(_RecordingMySQLConnection):
            async def fetchall(
                self, query: str, *args: object
            ) -> tuple[tuple[object, ...], ...]:
                self.queries.append((query, args))
                if query == "SHOW GRANTS FOR 'app'@'%'":
                    return (
                        ("GRANT USAGE ON *.* TO 'app'@'%'",),
                        (
                            (
                                "GRANT SELECT, INSERT, UPDATE, DELETE ON `app`.* "
                                "TO 'app'@'%'"
                            ),
                        ),
                    )
                if "FROM performance_schema.threads" in query:
                    raise RuntimeError("performance_schema denied")
                if "FROM INFORMATION_SCHEMA.PROCESSLIST" in query:
                    return ((101,),)
                raise AssertionError(f"Unexpected MySQL query: {query}")

        connection = ProcessListFallbackConnection(
            [
                1,  # database exists
                1,  # user exists
                99,  # current connection id
            ]
        )
        provisioner = MySQLProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        assert [(result.status, result.phase) for result in results] == [
            ("removed", "destroy"),
            ("removed", "destroy"),
        ]
        executed_sql = "\n".join(query for query, _args in connection.executed)
        assert "KILL 101" in executed_sql
        processlist_queries = [
            (query, args)
            for query, args in connection.queries
            if "performance_schema.threads" in query
            or "INFORMATION_SCHEMA.PROCESSLIST" in query
        ]
        assert processlist_queries == [
            (
                "SELECT PROCESSLIST_ID FROM performance_schema.threads "
                "WHERE PROCESSLIST_DB = %s AND PROCESSLIST_ID <> %s",
                ("app", 99),
            ),
            (
                (
                    "SELECT ID FROM INFORMATION_SCHEMA.PROCESSLIST "
                    "WHERE DB = %s AND ID <> %s"
                ),
                ("app", 99),
            ),
        ]

    @pytest.mark.anyio
    async def test_mysql_destroy_skips_service_account_runtime_user(self) -> None:
        connection = _RecordingMySQLConnection(
            [
                1,  # database exists
                1,  # user exists
                99,  # current connection id
            ],
            fetch_rows=[((101,),)],
        )
        provisioner = MySQLProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        executed_sql = "\n".join(query for query, _args in connection.executed)
        assert "KILL 101" in executed_sql
        assert "DROP DATABASE `app`" in executed_sql
        assert "DROP USER" not in executed_sql
        assert any("users are the same" in result.message for result in results)

    @pytest.mark.anyio
    async def test_mysql_destroy_skips_user_with_external_grants(self) -> None:
        connection = _RecordingMySQLConnection(
            [
                1,  # database exists
                1,  # user exists
                99,  # current connection id
            ],
            fetch_rows=[
                (
                    ("GRANT USAGE ON *.* TO 'app'@'%'",),
                    ("GRANT SELECT ON `other_app`.* TO 'app'@'%'",),
                ),
                ((101,),),
            ],
        )
        provisioner = MySQLProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        executed_sql = "\n".join(query for query, _args in connection.executed)
        assert "KILL 101" in executed_sql
        assert "DROP DATABASE `app`" in executed_sql
        assert "DROP USER" not in executed_sql
        assert any("grants outside app" in result.message for result in results)

    @pytest.mark.anyio
    async def test_mysql_destroy_rejects_missing_connection_id(self) -> None:
        connection = _RecordingMySQLConnection(
            [
                1,  # database exists
                1,  # user exists
                None,  # current connection id
            ],
            fetch_rows=[
                (
                    ("GRANT USAGE ON *.* TO 'app'@'%'",),
                    ("GRANT SELECT, INSERT, UPDATE, DELETE ON `app`.* TO 'app'@'%'",),
                ),
            ],
        )
        provisioner = MySQLProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        with pytest.raises(
            DatabaseProvisioningOperationError,
            match="Failed to terminate MySQL sessions",
        ):
            await provisioner.destroy(context, DestroyDatabaseRequest(confirm="app"))

        assert connection.closed

    @pytest.mark.anyio
    async def test_mysql_maintenance_tasks_execute_privilege_repair_and_state(
        self,
    ) -> None:
        repair_connection = _RecordingMySQLConnection([])
        state_connection = _RecordingMySQLConnection(
            [
                1,  # migration recorder table exists
                0,  # migration count
            ]
        )
        connector = _RecordingMySQLConnector(repair_connection, state_connection)
        provisioner = MySQLProvisioner(connector=connector)
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        assert [task.name for task in provisioner.maintenance_tasks(context)] == [
            "repair-privs",
            "migrations",
        ]
        repair_result = await provisioner.run_maintenance(
            context,
            DatabaseMaintenanceRequest(task="repair-privs"),
        )
        state_result = await provisioner.run_maintenance(
            context,
            DatabaseMaintenanceRequest(task="migrations"),
        )

        assert repair_result[0].status == "skipped"
        assert "GRANT SELECT, INSERT, UPDATE, DELETE ON `app`.* TO 'app'@'%'" in (
            "\n".join(query for query, _args in repair_connection.executed)
        )
        assert state_result[0].message == "Tortoise migration recorder table is empty."

    @pytest.mark.anyio
    async def test_mysql_provisioner_reports_missing_driver(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def import_module(_module_name: str) -> object:
            raise ImportError("missing")

        monkeypatch.setattr(
            mysql_provisioning.importlib, "import_module", import_module
        )
        provisioner = MySQLProvisioner()
        context = _mysql_provisioning_context(
            provisioning_connection=_database_connection(
                "mysql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match=r"wybra\[mysql\]",
        ):
            await provisioner.initialise(context)

    @pytest.mark.anyio
    async def test_mariadb_provisioner_initialises_with_mysql_compatible_sql(
        self,
    ) -> None:
        connection = _RecordingMySQLConnection(
            [
                0,  # database exists
                0,  # user exists
                0,  # migration recorder table exists
            ]
        )
        connector = _RecordingMySQLConnector(connection)
        provisioner = MariaDBProvisioner(connector=connector)
        context = _mariadb_provisioning_context(
            provisioning_connection=_database_connection(
                "mariadb",
                {
                    "database": "app",
                    "host": "db.example",
                    "user": "app_sa",
                    "password": "admin_secret",
                },
            ),
        )

        results = await provisioner.initialise(context)

        assert connector.credentials == [
            {
                "host": "db.example",
                "user": "app_sa",
                "password": "admin_secret",
            }
        ]
        assert {result.family for result in results} == {"mariadb"}
        assert any(
            result.message == "Created MariaDB database: app" for result in results
        )
        assert any(
            result.message == "Created MariaDB application user: app"
            for result in results
        )
        executed_sql = "\n".join(query for query, _args in connection.executed)
        queried_sql = "\n".join(query for query, _args in connection.queries)
        assert "CREATE DATABASE `app`" in executed_sql
        assert "CREATE USER 'app'@'%%' IDENTIFIED BY %s" in executed_sql
        assert "GRANT SELECT, INSERT, UPDATE, DELETE ON `app`.* TO 'app'@'%'" in (
            executed_sql
        )
        assert "FROM mysql.user" in queried_sql
        assert "secret" not in executed_sql
        assert "secret" not in " ".join(result.message for result in results)
        assert connection.closed

    @pytest.mark.anyio
    async def test_mariadb_destroy_uses_mariadb_labels_and_mysql_compatible_safety(
        self,
    ) -> None:
        connection = _RecordingMySQLConnection(
            [
                1,  # database exists
                1,  # user exists
                99,  # current connection id
            ],
            fetch_rows=[
                (
                    ("GRANT USAGE ON *.* TO 'app'@'%'",),
                    ("GRANT SELECT, INSERT, UPDATE, DELETE ON `app`.* TO 'app'@'%'",),
                ),
                ((101,),),
            ],
        )
        provisioner = MariaDBProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mariadb_provisioning_context(
            provisioning_connection=_database_connection(
                "mariadb",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        assert [(result.family, result.status, result.phase) for result in results] == [
            ("mariadb", "removed", "destroy"),
            ("mariadb", "removed", "destroy"),
        ]
        assert any(
            "Removed MariaDB database: app" in result.message for result in results
        )
        executed_sql = "\n".join(query for query, _args in connection.executed)
        assert "KILL 101" in executed_sql
        assert "DROP DATABASE `app`" in executed_sql
        assert "DROP USER 'app'@'%'" in executed_sql

    @pytest.mark.anyio
    async def test_mariadb_destroy_reports_unclassified_role_grants_as_unsupported(
        self,
    ) -> None:
        connection = _RecordingMySQLConnection(
            [
                1,  # database exists
                1,  # user exists
                99,  # current connection id
            ],
            fetch_rows=[
                (("GRANT app_role TO 'app'@'%'",),),
                ((101,),),
            ],
        )
        provisioner = MariaDBProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mariadb_provisioning_context(
            provisioning_connection=_database_connection(
                "mariadb",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        assert [(result.family, result.status, result.phase) for result in results] == [
            ("mariadb", "removed", "destroy"),
            ("mariadb", "unsupported", "destroy"),
        ]
        assert any(
            result.message
            == (
                "Skipped MariaDB application user removal because grant scope could "
                "not be classified safely."
            )
            for result in results
        )
        executed_sql = "\n".join(query for query, _args in connection.executed)
        assert "DROP DATABASE `app`" in executed_sql
        assert "DROP USER" not in executed_sql

    @pytest.mark.anyio
    async def test_mariadb_provisioner_uses_mariadb_label_for_count_errors(
        self,
    ) -> None:
        connection = _RecordingMySQLConnection(["not-count"])
        provisioner = MariaDBProvisioner(connector=_RecordingMySQLConnector(connection))
        context = _mariadb_provisioning_context(
            provisioning_connection=_database_connection(
                "mariadb",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        with pytest.raises(
            DatabaseProvisioningOperationError,
            match="MariaDB count result was invalid.",
        ):
            await provisioner.initialise(context)

    @pytest.mark.anyio
    async def test_mariadb_provisioner_reports_missing_driver(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def import_module(_module_name: str) -> object:
            raise ImportError("missing")

        monkeypatch.setattr(
            mysql_provisioning.importlib, "import_module", import_module
        )
        provisioner = MariaDBProvisioner()
        context = _mariadb_provisioning_context(
            provisioning_connection=_database_connection(
                "mariadb",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match=r"wybra\[mariadb\]",
        ):
            await provisioner.initialise(context)

    @pytest.mark.anyio
    async def test_postgresql_provisioner_initialises_database_role_schema_and_grants(
        self,
    ) -> None:
        maintenance = _RecordingPostgreSQLConnection(
            [
                False,  # database exists
                False,  # role exists
            ]
        )
        target = _RecordingPostgreSQLConnection(
            [
                False,  # schema exists
                False,  # migration recorder table exists
            ]
        )
        connector = _RecordingPostgreSQLConnector(maintenance, target)
        provisioner = PostgreSQLProvisioner(connector=connector)
        provisioning_connection = _database_connection(
            "postgresql",
            {"database": "app", "user": "app_sa", "password": "admin_secret"},
        )
        provisioning_connection = ResolvedDatabaseConnection.from_structured(
            backend=provisioning_connection.backend,
            credentials=provisioning_connection.credentials,
            sa_database="cluster_admin",
        )
        context = _postgresql_provisioning_context(
            provisioning_connection=provisioning_connection,
        )

        results = await provisioner.initialise(context)

        assert connector.credentials[0]["database"] == "cluster_admin"
        assert connector.credentials[1]["database"] == "app"
        assert [(result.status, result.phase) for result in results] == [
            ("created", "init"),
            ("created", "init"),
            ("created", "init"),
            ("skipped", "init"),
            ("noop", "init"),
        ]
        executed_sql = "\n".join(query for query, _args in maintenance.executed)
        executed_sql += "\n" + "\n".join(query for query, _args in target.executed)
        assert 'CREATE DATABASE "app" OWNER "app_sa"' in executed_sql
        assert 'CREATE ROLE "app" LOGIN PASSWORD' in executed_sql
        assert 'CREATE SCHEMA "public" AUTHORIZATION "app_sa"' in executed_sql
        assert 'GRANT CONNECT ON DATABASE "app" TO "app"' in executed_sql
        assert "admin_secret" not in " ".join(result.message for result in results)
        assert maintenance.closed
        assert target.closed

    @pytest.mark.anyio
    async def test_postgresql_provisioner_reuses_objects_and_reports_migrations(
        self,
    ) -> None:
        maintenance = _RecordingPostgreSQLConnection(
            [
                True,  # database exists
                "app_sa",  # database owner
                True,  # role exists
            ]
        )
        target = _RecordingPostgreSQLConnection(
            [
                True,  # schema exists
                "app_sa",  # schema owner
                True,  # migration recorder table exists
                2,  # migration count
            ]
        )
        connector = _RecordingPostgreSQLConnector(maintenance, target)
        provisioner = PostgreSQLProvisioner(connector=connector)
        context = _postgresql_provisioning_context(
            provisioning_connection=_database_connection(
                "postgresql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.initialise(context)

        assert [result.status for result in results] == [
            "skipped",
            "skipped",
            "skipped",
            "skipped",
            "noop",
        ]
        assert any("contains 2 record" in result.message for result in results)
        executed_sql = "\n".join(query for query, _args in maintenance.executed)
        executed_sql += "\n" + "\n".join(query for query, _args in target.executed)
        assert "CREATE DATABASE" not in executed_sql
        assert "CREATE SCHEMA" not in executed_sql
        assert 'ALTER ROLE "app" WITH PASSWORD' in executed_sql
        assert 'GRANT USAGE ON SCHEMA "public" TO "app"' in executed_sql

    @pytest.mark.anyio
    async def test_postgresql_destroy_requires_confirmed_database_and_sa_database(
        self,
    ) -> None:
        provisioner = PostgreSQLProvisioner(
            connector=_RecordingPostgreSQLConnector(_RecordingPostgreSQLConnection([]))
        )
        context = _postgresql_provisioning_context(
            provisioning_connection=_database_connection(
                "postgresql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="confirmation does not match",
        ):
            await provisioner.destroy(context, DestroyDatabaseRequest(confirm="other"))

        service_connection = context.provisioning_connection
        assert service_connection is not None
        context = _postgresql_provisioning_context(
            provisioning_connection=ResolvedDatabaseConnection.from_structured(
                backend=service_connection.backend,
                credentials=service_connection.credentials,
                sa_database="app",
            ),
        )
        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="different service-account database",
        ):
            await provisioner.destroy(context, DestroyDatabaseRequest(confirm="app"))

    @pytest.mark.anyio
    async def test_postgresql_init_requires_distinct_service_account_database(
        self,
    ) -> None:
        provisioner = PostgreSQLProvisioner(
            connector=_RecordingPostgreSQLConnector(_RecordingPostgreSQLConnection([]))
        )
        service_connection = _database_connection(
            "postgresql",
            {"database": "app", "user": "app_sa", "password": "admin_secret"},
        )
        context = _postgresql_provisioning_context(
            provisioning_connection=ResolvedDatabaseConnection.from_structured(
                backend=service_connection.backend,
                credentials=service_connection.credentials,
                sa_database="app",
            ),
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="different service-account database",
        ):
            await provisioner.initialise(context)

    @pytest.mark.anyio
    async def test_postgresql_destroy_removes_configured_database_and_role(
        self,
    ) -> None:
        maintenance = _RecordingPostgreSQLConnection(
            [
                True,  # database exists
                True,  # role exists
                False,  # role has dependencies outside target database
            ]
        )
        connector = _RecordingPostgreSQLConnector(maintenance)
        provisioner = PostgreSQLProvisioner(connector=connector)
        context = _postgresql_provisioning_context(
            provisioning_connection=_database_connection(
                "postgresql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        assert [(result.status, result.phase) for result in results] == [
            ("removed", "destroy"),
            ("removed", "destroy"),
        ]
        executed_sql = "\n".join(query for query, _args in maintenance.executed)
        assert "pg_terminate_backend" in executed_sql
        assert 'DROP DATABASE "app"' in executed_sql
        assert 'DROP ROLE "app"' in executed_sql

    @pytest.mark.anyio
    async def test_postgresql_destroy_skips_service_account_runtime_role(self) -> None:
        maintenance = _RecordingPostgreSQLConnection(
            [
                True,  # database exists
                True,  # role exists
            ]
        )
        connector = _RecordingPostgreSQLConnector(maintenance)
        provisioner = PostgreSQLProvisioner(connector=connector)
        context = _postgresql_provisioning_context(
            provisioning_connection=_database_connection(
                "postgresql",
                {"database": "app", "user": "app", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        executed_sql = "\n".join(query for query, _args in maintenance.executed)
        assert 'DROP DATABASE "app"' in executed_sql
        assert "DROP ROLE" not in executed_sql
        assert any("roles are the same" in result.message for result in results)

    @pytest.mark.anyio
    async def test_postgresql_destroy_skips_role_with_external_dependencies(
        self,
    ) -> None:
        maintenance = _RecordingPostgreSQLConnection(
            [
                True,  # database exists
                True,  # role exists
                True,  # role has dependencies outside target database
            ]
        )
        connector = _RecordingPostgreSQLConnector(maintenance)
        provisioner = PostgreSQLProvisioner(connector=connector)
        context = _postgresql_provisioning_context(
            provisioning_connection=_database_connection(
                "postgresql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        results = await provisioner.destroy(
            context, DestroyDatabaseRequest(confirm="app")
        )

        executed_sql = "\n".join(query for query, _args in maintenance.executed)
        assert 'DROP DATABASE "app"' in executed_sql
        assert "DROP ROLE" not in executed_sql
        assert any("dependencies outside app" in result.message for result in results)

    @pytest.mark.anyio
    async def test_postgresql_maintenance_tasks_execute_privilege_repair_and_state(
        self,
    ) -> None:
        repair_connection = _RecordingPostgreSQLConnection([])
        state_connection = _RecordingPostgreSQLConnection(
            [
                True,  # migration recorder table exists
                0,  # migration count
            ]
        )
        connector = _RecordingPostgreSQLConnector(repair_connection, state_connection)
        provisioner = PostgreSQLProvisioner(connector=connector)
        context = _postgresql_provisioning_context(
            provisioning_connection=_database_connection(
                "postgresql",
                {"database": "app", "user": "app_sa", "password": "admin_secret"},
            ),
        )

        assert [task.name for task in provisioner.maintenance_tasks(context)] == [
            "repair-privs",
            "migrations",
            "analyse",
            "extensions",
        ]
        repair_result = await provisioner.run_maintenance(
            context,
            DatabaseMaintenanceRequest(task="repair-privs"),
        )
        state_result = await provisioner.run_maintenance(
            context,
            DatabaseMaintenanceRequest(task="migrations"),
        )

        assert repair_result[0].status == "skipped"
        assert 'GRANT CONNECT ON DATABASE "app" TO "app"' in "\n".join(
            query for query, _args in repair_connection.executed
        )
        assert state_result[0].message == "Tortoise migration recorder table is empty."

    @pytest.mark.anyio
    async def test_database_lifecycle_guards_reject_blank_requests(self) -> None:
        context = _sqlite_provisioning_context()

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="Destroy confirmation must not be blank",
        ):
            await destroy_database(context, DestroyDatabaseRequest(confirm=" "))
        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="Maintenance task name must not be blank",
        ):
            await run_database_maintenance(
                context,
                DatabaseMaintenanceRequest(task=" "),
            )

    @pytest.mark.anyio
    async def test_database_maintenance_requires_confirmation_for_protected_tasks(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        context = _sqlite_provisioning_context()
        ran: list[str] = []

        class ConfirmingProvisioner:
            family: DatabaseFamily = "sqlite"

            async def initialise(
                self,
                _context: ProvisioningContext,
            ) -> tuple[Any, ...]:
                return ()

            async def destroy(
                self,
                _context: ProvisioningContext,
                _request: DestroyDatabaseRequest,
            ) -> tuple[Any, ...]:
                return ()

            def maintenance_tasks(
                self,
                _context: ProvisioningContext,
            ) -> tuple[provisioning_core.DatabaseMaintenanceTask, ...]:
                return (
                    provisioning_core.DatabaseMaintenanceTask(
                        name="protected",
                        description="Run protected maintenance.",
                        requires_confirmation=True,
                    ),
                )

            async def run_maintenance(
                self,
                _context: ProvisioningContext,
                request: DatabaseMaintenanceRequest,
            ) -> tuple[Any, ...]:
                ran.append(request.task)
                return ()

            def quote_identifier(self, identifier: str) -> str:
                return identifier

        monkeypatch.setattr(
            provisioning_core,
            "DEFAULT_PROVISIONERS",
            {"sqlite": ConfirmingProvisioner()},
        )

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="requires confirmation",
        ):
            await run_database_maintenance(
                context,
                DatabaseMaintenanceRequest(task="protected"),
            )

        result = await run_database_maintenance(
            context,
            DatabaseMaintenanceRequest(task="protected", confirm="protected"),
        )

        assert result == ()
        assert ran == ["protected"]

    @pytest.mark.anyio
    async def test_sqlite_provisioner_rejects_unknown_maintenance_task(self) -> None:
        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="Unknown sqlite maintenance task",
        ):
            await SQLiteProvisioner().run_maintenance(
                _sqlite_provisioning_context(),
                DatabaseMaintenanceRequest(task="vacuum"),
            )

    def test_database_provisioning_sql_renderer_quotes_identifiers(self) -> None:
        database = 'app"db'

        assert quote_sql_identifier('app"db') == '"app""db"'
        assert quote_mysql_identifier("app`db") == "`app``db`"
        assert quote_mssql_identifier("app]db") == "[app]]db]"

        rendered = render_sql(
            t"create database {ident(database)} owner {ident('owner')}",
            dialect="postgresql",
        )

        assert rendered.statement == 'create database "app""db" owner "owner"'
        assert rendered.parameters == ()

    def test_database_provisioning_sql_renderer_binds_parameters_by_dialect(
        self,
    ) -> None:
        sqlite = render_sql(
            t"select * from {ident('app_table')} where name = {param('secret')}",
            dialect="sqlite",
        )
        postgresql = render_sql(
            t"select {param('first')}, {param('second')}",
            dialect="postgresql",
        )
        mysql = render_sql(
            t"select {param('first')}, {param('second')}",
            dialect="mysql",
            quote_identifier=quote_mysql_identifier,
        )
        mssql = render_sql(
            t"select {param('first')}, {param('second')}",
            dialect="mssql",
            quote_identifier=quote_mssql_identifier,
        )

        assert sqlite.statement == 'select * from "app_table" where name = ?'
        assert sqlite.parameters == ("secret",)
        assert postgresql.statement == "select $1, $2"
        assert postgresql.parameters == ("first", "second")
        assert mysql.statement == "select %s, %s"
        assert mysql.parameters == ("first", "second")
        assert mssql.statement == "select ?, ?"
        assert mssql.parameters == ("first", "second")

    def test_database_provisioning_sql_renderer_requires_typed_interpolations(
        self,
    ) -> None:
        unsafe_value = "database"

        with pytest.raises(TypeError, match=r"ident\(\), param\(\), or trusted_sql"):
            render_sql(t"select * from {unsafe_value}", dialect="sqlite")

    def test_database_provisioning_sql_renderer_allows_trusted_sql(self) -> None:
        rendered = render_sql(
            t"select * from users order by {trusted_sql('created_at desc')}",
            dialect="sqlite",
        )

        assert rendered.statement == "select * from users order by created_at desc"

    def test_database_credential_transition_rejects_blank_values(self) -> None:
        transition = CredentialTransition(
            current=" current ",
            previous=(" previous ",),
        )

        assert transition.current == "current"
        assert transition.previous == ("previous",)

        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="Current credential value",
        ):
            CredentialTransition(current=" ")
        with pytest.raises(
            DatabaseProvisioningConfigurationError,
            match="Previous credential values",
        ):
            CredentialTransition(current="current", previous=(" ",))

    def test_database_url_support_uses_available_backend_modules(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        available_modules = {"aiosqlite", "asyncpg", "asyncodbc", "pyodbc"}

        def find_spec(module_name: str):
            return object() if module_name in available_modules else None

        monkeypatch.setattr(database_urls.importlib.util, "find_spec", find_spec)

        assert available_database_url_schemes() == (
            "sqlite",
            "postgresql",
            "postgres",
            "asyncpg",
            "mssql",
            "oracle",
        )
        assert is_supported_database_url("postgresql://user:password@host.example/app")
        assert is_supported_database_url("mssql://user:password@host.example/app")
        assert not is_supported_database_url("mysql://user:password@host.example/app")
        assert not is_supported_database_url("mariadb://user:password@host.example/app")

    def test_database_url_support_error_names_missing_backend_extra(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        available_modules = {"aiosqlite"}

        def find_spec(module_name: str):
            return object() if module_name in available_modules else None

        monkeypatch.setattr(database_urls.importlib.util, "find_spec", find_spec)

        assert database_url_support_error(
            "postgresql://user:password@host.example/app"
        ) == (
            "Database URL scheme postgresql:// requires the wybra[postgresql] "
            "optional dependency."
        )
        assert database_url_support_error(
            "mariadb://user:password@host.example/app"
        ) == (
            "Database URL scheme mariadb:// requires the wybra[mariadb] "
            "optional dependency."
        )

    @pytest.mark.anyio
    async def test_create_database_rejects_unavailable_backend(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        available_modules = {"aiosqlite"}

        def find_spec(module_name: str):
            return object() if module_name in available_modules else None

        monkeypatch.setattr(database_urls.importlib.util, "find_spec", find_spec)

        with pytest.raises(
            ConfigurationError,
            match=r"Database URL scheme postgresql:// requires the wybra\[postgresql\]",
        ):
            await create_database(
                "postgresql://user:password@host.example/app",
                modules=(),
            )

    @pytest.mark.parametrize(
        ("database_url", "expected"),
        (
            (
                "postgresql://user:password@host.example/app",
                "asyncpg://user:password@host.example/app",
            ),
            (
                "postgres://user:password@host.example/app",
                "asyncpg://user:password@host.example/app",
            ),
            (
                "asyncpg://user:password@host.example/app",
                "asyncpg://user:password@host.example/app",
            ),
            (
                "psycopg://user:password@host.example/app",
                "psycopg://user:password@host.example/app",
            ),
            (
                "mysql://user:password@host.example/app",
                "mysql://user:password@host.example/app",
            ),
            (
                "mariadb://user:password@host.example/app",
                "mysql://user:password@host.example/app",
            ),
            (
                "sqlite://:memory:",
                "sqlite://:memory:",
            ),
            (
                "sqlite:///app.db",
                "sqlite:///app.db",
            ),
            (
                "mssql://user:password@host.example/app",
                "mssql://user:password@host.example/app",
            ),
            (
                "oracle://user:password@host.example/app",
                "oracle://user:password@host.example/app",
            ),
            (
                "ftp://host.example/app",
                "ftp://host.example/app",
            ),
        ),
    )
    def test_tortoise_database_url_normalises_public_scheme(
        self,
        database_url: str,
        expected: str,
    ) -> None:
        assert tortoise_database_url(database_url) == expected

    def test_validate_persistence_requires_tortoise_migration_files(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        package_root = tmp_path / "models_with_empty_migrations"
        package_root.mkdir()
        (package_root / "__init__.py").write_text("", encoding="utf-8")
        (package_root / "models.py").write_text(
            (
                "from tortoise import fields\n"
                "from tortoise.models import Model\n\n"
                "class Example(Model):\n"
                "    id = fields.IntField(primary_key=True)\n"
            ),
            encoding="utf-8",
        )
        (package_root / "migrations").mkdir(parents=True)
        monkeypatch.syspath_prepend(str(tmp_path))
        importlib.invalidate_caches()

        result = validate_persistence(
            _persistence_settings(tmp_path, modules=("models_with_empty_migrations",))
        )

        assert "At least one Tortoise migration file is required." in result.errors
        assert (
            "Development database initialisation requires migrations." in result.errors
        )
        assert not _check_passed(
            result,
            "development database initialisation command is available",
        )

    def test_validate_persistence_fails_initialisation_when_module_discovery_fails(
        self,
        tmp_path: Path,
    ) -> None:
        result = validate_persistence(
            _persistence_settings(tmp_path, modules=("missing_data_module",))
        )

        assert "Module migration version location discovery failed:" in (
            _failed_check_descriptions(result.errors)
        )
        assert (
            "Development database initialisation requires migrations." in result.errors
        )
        assert not _check_passed(
            result,
            "development database initialisation command is available",
        )

    def test_validate_persistence_requires_migrations_for_configured_model_surface(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        package_root = tmp_path / "models_without_migrations"
        package_root.mkdir()
        (package_root / "__init__.py").write_text("", encoding="utf-8")
        (package_root / "models.py").write_text(
            (
                "from tortoise import fields\n"
                "from tortoise.models import Model\n\n"
                "class Example(Model):\n"
                "    id = fields.IntField(primary_key=True)\n"
            ),
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        importlib.invalidate_caches()

        result = validate_persistence(
            _persistence_settings(tmp_path, modules=("models_without_migrations",))
        )

        assert (
            "At least one configured module migration version location is required."
            in result.errors
        )
        assert (
            "Development database initialisation requires migrations." in result.errors
        )
        assert not _check_passed(
            result,
            "development database initialisation command is available",
        )

    def test_validate_persistence_accepts_configured_model_surface_with_migration(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        package_root = tmp_path / "models_with_migrations"
        package_root.mkdir()
        (package_root / "__init__.py").write_text("", encoding="utf-8")
        (package_root / "models.py").write_text(
            (
                "from tortoise import fields\n"
                "from tortoise.models import Model\n\n"
                "class Example(Model):\n"
                "    id = fields.IntField(primary_key=True)\n"
            ),
            encoding="utf-8",
        )
        migrations_root = package_root / "migrations"
        migrations_root.mkdir(parents=True)
        (migrations_root / "0001_initial.py").write_text(
            "revision = '0001'\ndown_revision = None\n",
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        importlib.invalidate_caches()

        result = validate_persistence(
            _persistence_settings(tmp_path, modules=("models_with_migrations",))
        )

        assert (
            "At least one configured module migration version location is required."
            not in result.errors
        )
        assert "At least one Tortoise migration file is required." not in result.errors
        assert _check_passed(
            result,
            "development database initialisation command is available",
        )


def _check_passed(result: ValidationResult, description_prefix: str) -> bool:
    return any(
        check.description.startswith(description_prefix) and check.passed
        for check in result.checks
    )


class TestDatabaseMigrationsAndRedaction:
    def test_redact_database_url_masks_sensitive_query_parameters(self) -> None:
        assert redact_database_url(
            "postgresql://user:password@host.example/app"
            "?sslmode=require&password=query-secret&token=abc&application_name=app%40local"
        ) == (
            "postgresql://***:***@host.example/app"
            "?sslmode=require&password=%2A%2A%2A&token=%2A%2A%2A"
            "&application_name=app%40local"
        )
        assert redact_database_url(
            "postgresql://host.example/app?api_key=secret&sslmode=require"
        ) == ("postgresql://host.example/app?api_key=%2A%2A%2A&sslmode=require")

    def test_redact_database_urls_masks_bare_postgresql_urls_in_messages(self) -> None:
        assert redact_database_urls(
            "failed for postgresql://user:secret@host.example/app and "
            "mysql://admin:admin-secret@host.example/app"
        ) == (
            "failed for postgresql://***:***@host.example/app and "
            "mysql://***:***@host.example/app"
        )

    def test_safe_database_error_message_redacts_database_urls(self) -> None:
        error = RuntimeError("failed for postgresql://user:secret@host.example/app")

        assert (
            safe_database_error_message(error)
            == "failed for postgresql://***:***@host.example/app"
        )

    def test_migration_version_locations_are_discovered_from_configured_modules(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        package_root = tmp_path / "host_app"
        package_root.mkdir()
        (package_root / "__init__.py").write_text("", encoding="utf-8")
        monkeypatch.syspath_prepend(str(tmp_path))
        importlib.invalidate_caches()

        version_locations = migration_version_locations_from_modules(
            ("host_app", "wybra.auth")
        )

        assert len(version_locations) == 2
        assert version_locations[0].as_posix().endswith("wybra/sessions/migrations")
        assert version_locations[1].as_posix().endswith("wybra/auth/migrations")
        assert discover_migration_version_locations("host_app") == ()

    def test_run_migration_dispatches_through_tortoise_backend_boundary(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        settings = _MigrationCommandSettings(
            database_url=sqlite_file_url(tmp_path / "dispatch.sqlite3"),
            project_root=tmp_path,
        )
        calls: list[migrate_module.MigrationContext] = []

        class RecordingMigrationBackend:
            async def heads(
                self,
                context: migrate_module.MigrationContext,
                _app_labels: tuple[str, ...],
            ) -> None:
                calls.append(context)

        backend = RecordingMigrationBackend()
        monkeypatch.setattr(migrate_module, "TortoiseMigrationBackend", lambda: backend)

        result = migrate_module._run_migration(
            lambda _database_url: settings,
            None,
            None,
            lambda migration_backend, context: migration_backend.heads(context, ()),
        )

        assert result == 0
        assert len(calls) == 1
        assert calls[0].settings is settings

    def test_core_sessions_revision_location_requires_module_config(self) -> None:
        with pytest.raises(DataCompositionError, match="wybra.sessions"):
            migration_version_location_for_configured_module(
                "wybra.sessions",
                (),
            )

    def test_model_packages_from_modules_uses_conventional_models_surface(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        package_root = tmp_path / "models_surface_app"
        package_root.mkdir()
        (package_root / "__init__.py").write_text("", encoding="utf-8")
        (package_root / "models.py").write_text(
            (
                "from tortoise import fields\n"
                "from tortoise.models import Model\n\n"
                "class Example(Model):\n"
                "    id = fields.IntField(primary_key=True)\n"
            ),
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        importlib.invalidate_caches()

        assert model_package_name("models_surface_app") == "models_surface_app.models"
        assert model_packages_from_modules(("models_surface_app",)) == (
            "wybra.sessions.models",
            "models_surface_app.models",
        )
        assert (
            discover_model_package("models_surface_app") == "models_surface_app.models"
        )

    def test_discover_model_package_ignores_modules_without_tortoise_models(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        package_root = tmp_path / "bad_models_surface_app"
        package_root.mkdir()
        (package_root / "__init__.py").write_text("", encoding="utf-8")
        (package_root / "models.py").write_text(
            "metadata = object()\n", encoding="utf-8"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        importlib.invalidate_caches()

        assert discover_model_package("bad_models_surface_app") is None
