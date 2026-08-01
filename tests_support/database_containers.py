from __future__ import annotations

import importlib
import json
import os
import secrets
import string
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Final

import pytest
from testcontainers.community.mysql import MySqlContainer
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import ExecWaitStrategy

from wybra.db.provisioning.mysql import quote_mysql_identifier
from wybra.db.sql import ident, render_sql, trusted_sql

DEFAULT_POSTGRES_IMAGE: Final = "postgres:17-alpine"
DEFAULT_MYSQL_IMAGE: Final = "mysql:8.4"
DEFAULT_MARIADB_IMAGE: Final = "mariadb:11.4"
DEFAULT_MSSQL_IMAGE: Final = "mcr.microsoft.com/mssql/server:2022-CU18-ubuntu-22.04"

POSTGRES_IMAGE_ENV: Final = "WYBRA_TESTCONTAINERS_POSTGRES_IMAGE"
MYSQL_IMAGE_ENV: Final = "WYBRA_TESTCONTAINERS_MYSQL_IMAGE"
MARIADB_IMAGE_ENV: Final = "WYBRA_TESTCONTAINERS_MARIADB_IMAGE"
MSSQL_IMAGE_ENV: Final = "WYBRA_TESTCONTAINERS_MSSQL_IMAGE"

_DEFAULT_IMAGES: Final = {
    POSTGRES_IMAGE_ENV: DEFAULT_POSTGRES_IMAGE,
    MYSQL_IMAGE_ENV: DEFAULT_MYSQL_IMAGE,
    MARIADB_IMAGE_ENV: DEFAULT_MARIADB_IMAGE,
    MSSQL_IMAGE_ENV: DEFAULT_MSSQL_IMAGE,
}

_APP_USER_ALPHABET: Final = string.ascii_lowercase + string.digits


@dataclass(frozen=True, slots=True)
class ContainerImageConfig:
    postgres: str
    mysql: str
    mariadb: str
    mssql: str

    @classmethod
    def from_environment(cls, root: Path | None = None) -> ContainerImageConfig:
        return cls(
            postgres=_image_value(POSTGRES_IMAGE_ENV, root=root),
            mysql=_image_value(MYSQL_IMAGE_ENV, root=root),
            mariadb=_image_value(MARIADB_IMAGE_ENV, root=root),
            mssql=_image_value(MSSQL_IMAGE_ENV, root=root),
        )


@dataclass(frozen=True, slots=True)
class ContainerDatabaseConfig:
    backend: str
    host: str
    port: int
    database: str
    runtime_user: str = field(repr=False)
    runtime_password: str = field(repr=False)
    service_user: str = field(repr=False)
    service_password: str = field(repr=False)
    service_database: str | None = None
    options: dict[str, str] = field(default_factory=dict, repr=False)

    @property
    def redacted_metadata(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "runtime_user": _redacted(self.runtime_user),
            "runtime_password": "<redacted>",
            "service_user": _redacted(self.service_user),
            "service_password": "<redacted>",
            "service_database": self.service_database,
        }

    def write_app_config(
        self,
        path: Path,
        *,
        modules: tuple[str, ...] = ("wybra.sessions",),
    ) -> Path:
        modules_value = ", ".join(_toml_string(module) for module in modules)
        lines = [
            "[app]",
            f"modules = [{modules_value}]",
            'deployment_environment = "local"',
            "",
            "[app.templates]",
            "auto_reload = true",
            "cache_size = 0",
            "",
            "[app.assets]",
            'url_path = "/static/"',
            "",
            "[app.runserver]",
            'asgi_app = "test_app:app"',
            'reload_env = "APP_RELOAD"',
            "",
            "[app.database]",
            f"backend = {_toml_string(self.backend)}",
            f"host = {_toml_string(self.host)}",
            f"port = {self.port}",
            f"database = {_toml_string(self.database)}",
            f"user = {_toml_string(self.runtime_user)}",
            f"password = {_toml_string(self.runtime_password)}",
            f"sa_user = {_toml_string(self.service_user)}",
            f"sa_password = {_toml_string(self.service_password)}",
        ]
        if self.service_database is not None:
            lines.append(f"sa_database = {_toml_string(self.service_database)}")
        if self.options:
            lines.extend(("", "[app.database.options]"))
            lines.extend(
                f"{key} = {_toml_string(value)}"
                for key, value in sorted(self.options.items())
            )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path


@dataclass(frozen=True, slots=True)
class PostgreSQLContainerService:
    host: str
    port: int
    user: str
    password: str = field(repr=False)
    database: str

    def runtime_config(self) -> ContainerDatabaseConfig:
        suffix = _random_identifier(12)
        return ContainerDatabaseConfig(
            backend="postgresql",
            host=self.host,
            port=self.port,
            database=f"wybra_it_{suffix}",
            runtime_user=f"wybra_user_{suffix}",
            runtime_password=_random_secret(),
            service_user=self.user,
            service_password=self.password,
            service_database=self.database,
        )


@dataclass(frozen=True, slots=True)
class MySQLContainerService:
    backend: str
    host: str
    port: int
    user: str
    password: str = field(repr=False)

    def runtime_config(self) -> ContainerDatabaseConfig:
        suffix = _random_identifier(12)
        return ContainerDatabaseConfig(
            backend=self.backend,
            host=self.host,
            port=self.port,
            database=f"wybra_it_{suffix}",
            runtime_user=f"wybra_user_{suffix}",
            runtime_password=_random_secret(),
            service_user=self.user,
            service_password=self.password,
        )


@dataclass(frozen=True, slots=True)
class SQLServerContainerService:
    host: str
    port: int
    user: str
    password: str = field(repr=False)
    database: str

    def runtime_config(self) -> ContainerDatabaseConfig:
        suffix = _random_identifier(12)
        return ContainerDatabaseConfig(
            backend="mssql",
            host=self.host,
            port=self.port,
            database=f"wybra_it_{suffix}",
            runtime_user=f"wybra_user_{suffix}",
            runtime_password=_random_sqlserver_secret(),
            service_user=self.user,
            service_password=self.password,
            service_database=self.database,
            options={
                "Encrypt": "yes",
                "TrustServerCertificate": "yes",
            },
        )


def skip_if_docker_unavailable() -> None:
    available, reason = docker_availability()
    if not available:
        pytest.skip(reason)


@lru_cache
def docker_availability() -> tuple[bool, str]:
    try:
        import docker
    except ImportError:
        return False, "Docker Python client is not installed."

    try:
        client = docker.from_env()
        try:
            client.ping()
        finally:
            client.close()
    except Exception as exc:
        return False, f"Docker is unavailable for testcontainers: {exc}"
    return True, "Docker is available."


def start_postgresql_container(
    *,
    root: Path | None = None,
) -> tuple[PostgresContainer, PostgreSQLContainerService]:
    skip_if_docker_unavailable()
    image = ContainerImageConfig.from_environment(root).postgres
    password = _random_secret()
    container = PostgresContainer(
        image=image,
        username="postgres",
        password=password,
        dbname="postgres",
        driver=None,
    )
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"PostgreSQL testcontainer could not start: {exc}")
    service = PostgreSQLContainerService(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(5432)),
        user="postgres",
        password=password,
        database="postgres",
    )
    return container, service


def start_mysql_container(
    *,
    backend: str,
    root: Path | None = None,
) -> tuple[MySqlContainer, MySQLContainerService]:
    skip_if_docker_unavailable()
    image_config = ContainerImageConfig.from_environment(root)
    image = image_config.mariadb if backend == "mariadb" else image_config.mysql
    password = _random_secret()
    container = MySqlContainer(
        image=image,
        username="test",
        root_password=password,
        password=_random_secret(),
        dbname="test",
        dialect="pymysql",
    )
    try:
        container.start()
    except Exception as exc:
        label = "MariaDB" if backend == "mariadb" else "MySQL"
        pytest.skip(f"{label} testcontainer could not start: {exc}")
    service = MySQLContainerService(
        backend=backend,
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(3306)),
        user="root",
        password=password,
    )
    return container, service


def start_mssql_container(
    *,
    root: Path | None = None,
) -> tuple[DockerContainer, SQLServerContainerService]:
    skip_if_mssql_driver_unavailable()
    skip_if_docker_unavailable()

    image = ContainerImageConfig.from_environment(root).mssql
    password = _random_sqlserver_secret()
    container = (
        DockerContainer(image)
        .with_exposed_ports(1433)
        .with_env("ACCEPT_EULA", "Y")
        .with_env("SA_PASSWORD", password)
        .waiting_for(
            ExecWaitStrategy(
                [
                    "bash",
                    "-c",
                    "/opt/mssql-tools*/bin/sqlcmd "
                    "-U SA -P \"$SA_PASSWORD\" -Q 'SELECT 1' -C",
                ]
            ).with_startup_timeout(120)
        )
    )
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"SQL Server testcontainer could not start: {exc}")
    service = SQLServerContainerService(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(1433)),
        user="SA",
        password=password,
        database="master",
    )
    return container, service


async def cleanup_postgresql_target(config: ContainerDatabaseConfig) -> None:
    await _cleanup_postgresql_target(config)


async def cleanup_mysql_target(config: ContainerDatabaseConfig) -> None:
    await _cleanup_mysql_target(config)


async def cleanup_mssql_target(config: ContainerDatabaseConfig) -> None:
    await _cleanup_mssql_target(config)


def assert_database_secrets_absent(
    text: str,
    config: ContainerDatabaseConfig,
) -> None:
    leaked_labels = [
        label
        for label, value in (
            ("runtime_password", config.runtime_password),
            ("service_password", config.service_password),
        )
        if value and value in text
    ]
    assert not leaked_labels, f"database secrets leaked: {', '.join(leaked_labels)}"


async def _cleanup_postgresql_target(config: ContainerDatabaseConfig) -> None:
    asyncpg = pytest.importorskip("asyncpg")
    maintenance = await asyncpg.connect(
        host=config.host,
        port=config.port,
        user=config.service_user,
        password=config.service_password,
        database=config.service_database or "postgres",
    )
    try:
        await maintenance.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = $1
              AND pid <> pg_backend_pid()
            """,
            config.database,
        )
        drop_database = render_sql(
            t"DROP DATABASE IF EXISTS {ident(config.database)}",
            dialect="postgresql",
        )
        await maintenance.execute(
            drop_database.statement,
            *drop_database.parameters,
        )
        drop_role = render_sql(
            t"DROP ROLE IF EXISTS {ident(config.runtime_user)}",
            dialect="postgresql",
        )
        await maintenance.execute(drop_role.statement, *drop_role.parameters)
    finally:
        await maintenance.close()


async def _cleanup_mysql_target(config: ContainerDatabaseConfig) -> None:
    aiomysql = pytest.importorskip("aiomysql")
    connection = await aiomysql.connect(
        host=config.host,
        port=config.port,
        user=config.service_user,
        password=config.service_password,
        autocommit=True,
    )
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = %s",
                (config.database,),
            )
            database_row = await cursor.fetchone()
            if database_row is not None and database_row[0]:
                drop_database = render_sql(
                    t"DROP DATABASE {ident(config.database)}",
                    dialect="mysql",
                    quote_identifier=quote_mysql_identifier,
                )
                await cursor.execute(
                    drop_database.statement,
                    drop_database.parameters or None,
                )

            await cursor.execute(
                "SELECT COUNT(*) FROM mysql.user WHERE User = %s AND Host = %s",
                (config.runtime_user, "%"),
            )
            user_row = await cursor.fetchone()
            if user_row is not None and user_row[0]:
                account = trusted_sql(_mysql_account_literal(config.runtime_user))
                drop_user = render_sql(
                    t"DROP USER {account}",
                    dialect="mysql",
                    quote_identifier=quote_mysql_identifier,
                )
                await cursor.execute(drop_user.statement, drop_user.parameters or None)
    finally:
        connection.close()
        wait_closed = getattr(connection, "wait_closed", None)
        if callable(wait_closed):
            await wait_closed()


async def _cleanup_mssql_target(config: ContainerDatabaseConfig) -> None:
    asyncodbc, _pyodbc = _mssql_driver_modules()
    connection = await asyncodbc.connect(
        dsn=_mssql_dsn(config, database=config.service_database or "master"),
        autocommit=True,
    )
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                IF DB_ID(?) IS NOT NULL
                BEGIN
                    DECLARE @sql nvarchar(max) =
                        N'ALTER DATABASE ' + QUOTENAME(?) +
                        N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE'
                    EXEC(@sql)
                    SET @sql = N'DROP DATABASE ' + QUOTENAME(?)
                    EXEC(@sql)
                END
                """,
                config.database,
                config.database,
                config.database,
            )
            await cursor.execute(
                """
                IF SUSER_ID(?) IS NOT NULL
                BEGIN
                    DECLARE @sql nvarchar(max) = N'DROP LOGIN ' + QUOTENAME(?)
                    EXEC(@sql)
                END
                """,
                config.runtime_user,
                config.runtime_user,
            )
    finally:
        await connection.close()


async def postgresql_fetch_value(
    config: ContainerDatabaseConfig,
    query: str,
    *args: object,
    database: str | None = None,
) -> object:
    asyncpg = pytest.importorskip("asyncpg")
    connection = await asyncpg.connect(
        host=config.host,
        port=config.port,
        user=config.service_user,
        password=config.service_password,
        database=database or config.database,
    )
    try:
        return await connection.fetchval(query, *args)
    finally:
        await connection.close()


async def mssql_fetch_value(
    config: ContainerDatabaseConfig,
    query: str,
    *args: object,
    database: str | None = None,
) -> object:
    asyncodbc, _pyodbc = _mssql_driver_modules()
    connection = await asyncodbc.connect(
        dsn=_mssql_dsn(config, database=database or config.database),
        autocommit=True,
    )
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(query, *args)
            row = await cursor.fetchone()
            return None if row is None else row[0]
    finally:
        await connection.close()


async def mysql_fetch_value(
    config: ContainerDatabaseConfig,
    query: str,
    *args: object,
    database: str | None = None,
) -> object:
    aiomysql = pytest.importorskip("aiomysql")
    connection_kwargs = {
        "host": config.host,
        "port": config.port,
        "user": config.service_user,
        "password": config.service_password,
        "autocommit": True,
    }
    if database is not None:
        connection_kwargs["db"] = database
    connection = await aiomysql.connect(
        **connection_kwargs,
    )
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(query, args)
            row = await cursor.fetchone()
            return None if row is None else row[0]
    finally:
        connection.close()
        wait_closed = getattr(connection, "wait_closed", None)
        if callable(wait_closed):
            await wait_closed()


def _image_value(name: str, *, root: Path | None) -> str:
    value = os.environ.get(name)
    if value is not None and value.strip():
        return value.strip()
    dotenv_value = _dotenv_values(root).get(name)
    if dotenv_value is not None and dotenv_value.strip():
        return dotenv_value.strip()
    return _DEFAULT_IMAGES[name]


@lru_cache
def _dotenv_values(root: Path | None) -> dict[str, str]:
    dotenv_path = (root or Path.cwd()) / ".env"
    if not dotenv_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _random_identifier(length: int) -> str:
    return "".join(secrets.choice(_APP_USER_ALPHABET) for _ in range(length))


def _random_secret() -> str:
    return secrets.token_urlsafe(24)


def _random_sqlserver_secret() -> str:
    return f"Wybra1!{secrets.token_urlsafe(18)}"


def _redacted(value: str) -> str:
    if len(value) <= 4:
        return "<redacted>"
    return f"{value[:2]}...{value[-2:]}"


def _toml_string(value: str) -> str:
    return json.dumps(value)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _mysql_account_literal(user: str) -> str:
    return _sql_string(user) + "@'%'"


def skip_if_mssql_driver_unavailable() -> None:
    try:
        _asyncodbc, pyodbc = _mssql_driver_modules()
    except Exception as exc:
        pytest.skip(f"SQL Server ODBC driver is unavailable: {type(exc).__name__}")
    drivers = set(pyodbc.drivers())
    if "ODBC Driver 18 for SQL Server" not in drivers:
        pytest.skip("SQL Server ODBC driver is unavailable: ODBC Driver 18 missing.")


def _mssql_driver_modules() -> tuple[object, object]:
    asyncodbc = importlib.import_module("asyncodbc")
    pyodbc = importlib.import_module("pyodbc")
    return asyncodbc, pyodbc


def _mssql_dsn(config: ContainerDatabaseConfig, *, database: str) -> str:
    server = f"{config.host},{config.port}"
    return ";".join(
        (
            "DRIVER={ODBC Driver 18 for SQL Server}",
            f"SERVER={{{server}}}",
            f"DATABASE={{{database}}}",
            f"UID={{{config.service_user}}}",
            f"PWD={{{config.service_password}}}",
            "Encrypt={yes}",
            "TrustServerCertificate={yes}",
        )
    )
