from __future__ import annotations

import ast
import inspect
import io
import logging
import sys
import types
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager, redirect_stdout
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from functools import partial
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol, cast

import anyio
import click
from tortoise import fields
from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.cli import cli as tortoise_cli
from tortoise.cli import utils as tortoise_cli_utils
from tortoise.context import TortoiseContext
from tortoise.exceptions import ConfigurationError as TortoiseConfigurationError
from tortoise.fields.relational import (
    ForeignKeyFieldInstance,
    ManyToManyFieldInstance,
    OneToOneFieldInstance,
)
from tortoise.migrations.autodetector import RELATION_FIELDS, MigrationAutodetector
from tortoise.migrations.constraints import CheckConstraint
from tortoise.migrations.executor import MigrationExecutor
from tortoise.migrations.graph import MigrationKey
from tortoise.migrations.operations import AddConstraint, CreateModel
from tortoise.migrations.recorder import MigrationRecorder
from tortoise.models import Model

from wybra.core.composition import AppConfig
from wybra.core.conventions import MODEL_SURFACE_MODULE
from wybra.core.logging import LoggingConfigurationError
from wybra.db.provisioning import (
    DatabaseMaintenanceRequest,
    DatabaseMaintenanceTask,
    DatabaseProvisioningConfigurationError,
    DatabaseProvisioningOperationError,
    DestroyDatabaseRequest,
    ProvisioningContext,
    ProvisioningPhaseResult,
    database_maintenance_tasks,
    destroy_database,
    initialise_database,
    provisioning_context,
    run_database_maintenance,
)
from wybra.db.provisioning.mysql import quote_mysql_identifier
from wybra.db.settings import CredentialPurpose, ResolvedDatabaseConnection
from wybra.db.sql import ident, param, render_sql
from wybra.db.surfaces import (
    DataCompositionError,
    migration_version_locations_from_modules,
)
from wybra.db.tortoise import (
    build_tortoise_config as build_config,
)
from wybra.db.tortoise import (
    tortoise_migrations_package,
)
from wybra.db.urls import (
    database_backend_for_url,
    database_url_support_error,
    is_supported_database_url,
    safe_database_error_message,
)
from wybra.db.versioning import (
    VersionField,
    version_column_check_constraint,
    version_field_check_constraint,
)
from wybra.tools.app_startup import (
    CONFIG_SOURCE_CONTEXT_KEY,
    CONFIG_SOURCE_HELP,
    CONFIG_SOURCE_OPTION,
    config_source_from_click_context,
)
from wybra.tools.cli_logging import configure_cli_logging

DATABASE_URL_HELP = "Override the configured database URL for this migration command."

logger = logging.getLogger(__name__)
TORTOISE_CONFIG_VARIABLE = "TORTOISE_ORM"


class MigrationSettings(Protocol):
    """Settings shape required by Tortoise migration command construction.

    Host adapters provide this shape from their concrete settings object.
    Migration commands need the effective database URL, configured modules,
    and optional composition metadata.
    """

    database_url: str | None
    database_connection: ResolvedDatabaseConnection | None
    provisioning_connection: ResolvedDatabaseConnection | None
    project_root: Path
    migrations_root: Path | None
    app_config: AppConfig | None

    @property
    def modules(self) -> tuple[str, ...]: ...


class MigrationSettingsLoader(Protocol):
    """Callable shape used to build migration settings.

    Loaders must accept the database URL as their first argument. Newer loaders
    may accept ``config_source`` as a keyword-only argument; extra keywords are
    permitted so wrappers can forward command context without losing type
    information for the required database URL.
    """

    def __call__(
        self,
        database_url: str | None,
        *,
        config_source: str | None = ...,
        database_credential_purpose: CredentialPurpose = ...,
        fallback_to_runtime_credentials: bool = ...,
        include_provisioning_connection: bool = ...,
        resolve_database_credentials: bool = ...,
        **_: object,
    ) -> MigrationSettings: ...


class MigrationConfigurationError(ValueError):
    """Raised when host application migration settings cannot be built."""


class MigrationStateError(RuntimeError):
    """Raised when a migration operation is invalid for the database state."""


def special_migration_description(migration_path: Path) -> str | None:
    """Return the declared rationale for an explicit special migration."""
    try:
        module = ast.parse(migration_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise MigrationStateError(
            f"Cannot read migration source: {migration_path}."
        ) from exc
    except SyntaxError as exc:
        raise MigrationStateError(
            f"Cannot parse migration source: {migration_path}."
        ) from exc

    declarations = _migration_class_declarations(module)
    marker = declarations.get("not_generated")
    if not isinstance(marker, ast.Constant) or marker.value is not True:
        return None
    description = declarations.get("not_generated_description")
    if not isinstance(description, ast.Constant) or not isinstance(
        description.value, str
    ):
        raise MigrationStateError(
            "Special migration requires a non-empty not_generated_description: "
            f"{migration_path}."
        )
    if not (value := description.value.strip()):
        raise MigrationStateError(
            "Special migration requires a non-empty not_generated_description: "
            f"{migration_path}."
        )
    return value


def migration_dependencies(migration_path: Path) -> tuple[tuple[str, str], ...]:
    """Read literal Tortoise migration dependencies without importing source."""
    try:
        module = ast.parse(migration_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise MigrationStateError(
            f"Cannot read migration source: {migration_path}."
        ) from exc
    except SyntaxError as exc:
        raise MigrationStateError(
            f"Cannot parse migration source: {migration_path}."
        ) from exc

    declaration = _migration_class_declarations(module).get("dependencies")
    if declaration is None:
        return ()
    try:
        value = ast.literal_eval(declaration)
    except ValueError as exc:
        raise MigrationStateError(
            f"Migration dependencies must be literal values: {migration_path}."
        ) from exc
    if not isinstance(value, list | tuple) or not all(
        isinstance(dependency, tuple)
        and len(dependency) == 2
        and all(isinstance(item, str) for item in dependency)
        for dependency in value
    ):
        raise MigrationStateError(
            f"Migration dependencies are invalid: {migration_path}."
        )
    return tuple(value)


def _migration_class_declarations(module: ast.Module) -> dict[str, ast.expr]:
    migration_class = next(
        (
            node
            for node in module.body
            if isinstance(node, ast.ClassDef) and node.name == "Migration"
        ),
        None,
    )
    if migration_class is None:
        return {}
    declarations: dict[str, ast.expr] = {}
    for statement in migration_class.body:
        if isinstance(statement, ast.Assign):
            for target in statement.targets:
                if isinstance(target, ast.Name):
                    declarations[target.id] = statement.value
        elif (
            isinstance(statement, ast.AnnAssign)
            and isinstance(statement.target, ast.Name)
            and statement.value is not None
        ):
            declarations[statement.target.id] = statement.value
    return declarations


@dataclass(frozen=True, slots=True)
class MigrationContext:
    """Resolved migration command context passed to migration backends."""

    settings: MigrationSettings
    config: dict[str, Any]
    database_connection: ResolvedDatabaseConnection
    provisioning_connection: ResolvedDatabaseConnection | None = None
    migrations_root: Path | None = None


@dataclass(frozen=True, slots=True)
class GeneratedTemporaryMigrations:
    """Generated migrations and application configuration for one test lifecycle."""

    config: dict[str, Any]
    root: Path
    paths: tuple[Path, ...]


@dataclass(frozen=True, slots=True)
class MigrationResetPlan:
    """Preflight result for replacing model-derived migration history."""

    generated_baseline: tuple[Path, ...]
    migration_locations: tuple[Path, ...]


@dataclass(frozen=True, slots=True)
class ModelMigrationPlan:
    """Dependency-ordered migration generation derived from current models."""

    app_labels: tuple[str, ...]
    dependencies: tuple[tuple[str, tuple[str, ...]], ...]

    def dependencies_for(self, app_label: str) -> tuple[str, ...]:
        return dict(self.dependencies).get(app_label, ())


class MigrationBackend(Protocol):
    """Backend boundary for migration lifecycle operations."""

    async def initialise(
        self,
        context: MigrationContext,
        *,
        app_labels: tuple[str, ...],
    ) -> None: ...

    async def makemigrations(
        self,
        context: MigrationContext,
        request: MakeMigrationsRequest,
    ) -> None: ...

    async def migrate(
        self,
        context: MigrationContext,
        request: MigrationTargetRequest,
    ) -> None: ...

    async def downgrade(
        self,
        context: MigrationContext,
        request: MigrationTargetRequest,
    ) -> None: ...

    async def history(
        self,
        context: MigrationContext,
        app_labels: tuple[str, ...],
    ) -> None: ...

    async def heads(
        self,
        context: MigrationContext,
        app_labels: tuple[str, ...],
    ) -> None: ...

    async def sqlmigrate(
        self,
        context: MigrationContext,
        request: SqlMigrateRequest,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class MakeMigrationsRequest:
    app_labels: tuple[str, ...]
    empty: bool
    name: str | None


@dataclass(frozen=True, slots=True)
class MigrationTargetRequest:
    app_label: str | None
    migration: str | None
    fake: bool
    dry_run: bool


@dataclass(frozen=True, slots=True)
class SqlMigrateRequest:
    app_label: str | None
    migration_name: str | None
    backward: bool


class TortoiseMigrationBackend:
    """Tortoise-backed migration backend."""

    async def initialise(
        self,
        context: MigrationContext,
        *,
        app_labels: tuple[str, ...],
    ) -> None:
        await _run_tortoise_cli(context, ["init", *app_labels])

    async def makemigrations(
        self,
        context: MigrationContext,
        request: MakeMigrationsRequest,
    ) -> None:
        if request.app_labels or request.empty:
            await _run_tortoise_cli(context, _makemigrations_arguments(request))
            return
        await _generate_model_migrations(
            context.config,
            context.migrations_root,
            request,
        )

    async def migrate(
        self,
        context: MigrationContext,
        request: MigrationTargetRequest,
    ) -> None:
        await _run_tortoise_cli(context, _target_args("migrate", request))

    async def downgrade(
        self,
        context: MigrationContext,
        request: MigrationTargetRequest,
    ) -> None:
        await _run_tortoise_cli(context, _target_args("downgrade", request))

    async def history(
        self,
        context: MigrationContext,
        app_labels: tuple[str, ...],
    ) -> None:
        await _run_tortoise_cli(context, ["history", *app_labels])

    async def heads(
        self,
        context: MigrationContext,
        app_labels: tuple[str, ...],
    ) -> None:
        await _run_tortoise_cli(context, ["heads", *app_labels])

    async def sqlmigrate(
        self,
        context: MigrationContext,
        request: SqlMigrateRequest,
    ) -> None:
        args = ["sqlmigrate"]
        if request.app_label is not None:
            args.append(request.app_label)
        if request.migration_name is not None:
            args.append(request.migration_name)
        if request.backward:
            args.append("--backward")
        await _run_tortoise_cli(context, args)


def _target_args(command_name: str, request: MigrationTargetRequest) -> list[str]:
    args = [command_name]
    if request.app_label is not None:
        args.append(request.app_label)
    if request.migration is not None:
        args.append(request.migration)
    if request.fake:
        args.append("--fake")
    if request.dry_run:
        args.append("--dry-run")
    return args


def _database_url_option[F: Callable[..., Any]](function: F) -> F:
    """Add the optional ``database_url: str | None`` Click option."""

    return click.option("--database-url", help=DATABASE_URL_HELP)(function)


def create_migrate_command(
    settings_loader: MigrationSettingsLoader,
    *,
    migration_runner: Callable[..., int] | None = None,
) -> click.Group:
    if migration_runner is None:

        def migration_runner(
            database_url: str | None,
            config_source: str | None,
            operation: Callable[[MigrationBackend, MigrationContext], Awaitable[None]],
            *,
            database_credential_purpose: CredentialPurpose = "runtime",
            fallback_to_runtime_credentials: bool = False,
            include_provisioning_connection: bool = False,
            resolve_database_credentials: bool = True,
        ) -> int:
            return _run_migration(
                settings_loader,
                database_url,
                config_source,
                operation,
                database_credential_purpose=database_credential_purpose,
                fallback_to_runtime_credentials=fallback_to_runtime_credentials,
                include_provisioning_connection=include_provisioning_connection,
                resolve_database_credentials=resolve_database_credentials,
            )

    @click.group(
        name="wybra-migrate",
        context_settings={
            "help_option_names": ["-h", "--help"],
            "max_content_width": 120,
        },
        help="Run application schema migrations through Tortoise.",
    )
    @_database_url_option
    @click.option(
        CONFIG_SOURCE_OPTION, CONFIG_SOURCE_CONTEXT_KEY, help=CONFIG_SOURCE_HELP
    )
    @click.pass_context
    def migrate_command(
        ctx: click.Context,
        database_url: str | None,
        config_source: str | None,
    ) -> None:
        ctx.ensure_object(dict)
        ctx.obj["database_url"] = database_url
        ctx.obj[CONFIG_SOURCE_CONTEXT_KEY] = config_source

    @migrate_command.command(
        "init",
        help="Initialise migration packages.",
    )
    @_database_url_option
    @click.argument("app_labels", nargs=-1)
    @click.pass_context
    def init_command(
        ctx: click.Context,
        database_url: str | None,
        app_labels: tuple[str, ...],
    ) -> int:
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            operation=lambda backend, context: initialise_migration_lifecycle(
                backend,
                context,
                app_labels=app_labels,
            ),
            include_provisioning_connection=True,
        )

    @migrate_command.command(
        "destroy",
        help="Destroy database objects owned by lifecycle setup.",
    )
    @_database_url_option
    @click.option(
        "--confirm",
        required=True,
        help="Confirm the target database/schema name before destructive work runs.",
    )
    @click.option(
        "--reset-migrations",
        is_flag=True,
        help=(
            "Remove replaceable migration history after the confirmed database destroy."
        ),
    )
    @click.pass_context
    def destroy_command(
        ctx: click.Context,
        database_url: str | None,
        confirm: str,
        reset_migrations: bool,
    ) -> int:
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            include_provisioning_connection=True,
            operation=lambda _backend, context: _destroy_command_lifecycle(
                context,
                confirm=confirm,
                reset_migrations=reset_migrations,
            ),
        )

    @migrate_command.command(
        "tasks",
        help="List database maintenance tasks.",
    )
    @_database_url_option
    @click.pass_context
    def tasks_command(
        ctx: click.Context,
        database_url: str | None,
    ) -> int:
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            operation=list_database_maintenance_tasks_lifecycle,
            resolve_database_credentials=False,
        )

    @migrate_command.command(
        "run",
        help="Run a database maintenance task.",
    )
    @_database_url_option
    @click.option(
        "--confirm",
        help="Confirm the maintenance task name before protected work runs.",
    )
    @click.argument("task")
    @click.pass_context
    def run_command(
        ctx: click.Context,
        database_url: str | None,
        confirm: str | None,
        task: str,
    ) -> int:
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            include_provisioning_connection=True,
            operation=lambda _backend, context: run_database_maintenance_lifecycle(
                context,
                DatabaseMaintenanceRequest(
                    task=_maintenance_task_from_argument(task),
                    confirm=confirm,
                ),
            ),
        )

    @migrate_command.command(
        "makemigrations",
        help="Create migrations from model changes.",
    )
    @_database_url_option
    @click.argument("app_labels", nargs=-1)
    @click.option("--empty", is_flag=True, help="Create an empty migration.")
    @click.option("-n", "--name", help="Use this name for the migration file.")
    @click.option(
        "--module",
        "model_modules",
        multiple=True,
        help="Generate from a model-owning module or its .models surface.",
    )
    @click.option(
        "--migrations-root",
        type=click.Path(path_type=Path),
        help="Write generated migrations under an isolated importable root.",
    )
    @click.pass_context
    def makemigrations_command(
        ctx: click.Context,
        database_url: str | None,
        app_labels: tuple[str, ...],
        empty: bool,
        name: str | None,
        model_modules: tuple[str, ...],
        migrations_root: Path | None,
    ) -> int:
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            lambda backend, context: backend.makemigrations(
                _migration_context_with_overrides(
                    context,
                    model_modules=model_modules,
                    migrations_root=migrations_root,
                ),
                MakeMigrationsRequest(app_labels=app_labels, empty=empty, name=name),
            ),
            database_credential_purpose="service_account",
        )

    @migrate_command.command(
        "migrate",
        help="Apply migrations.",
    )
    @_database_url_option
    @click.argument("args", nargs=-1)
    @click.option(
        "--fake",
        is_flag=True,
        help="Record migrations without executing SQL.",
    )
    @click.option(
        "--dry-run",
        is_flag=True,
        help="Show what would run without changing database state.",
    )
    @click.pass_context
    def migrate_command_command(
        ctx: click.Context,
        database_url: str | None,
        args: tuple[str, ...],
        fake: bool,
        dry_run: bool,
    ) -> int:
        app_label, migration = _migration_target_from_args(args)
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            lambda backend, context: backend.migrate(
                context,
                MigrationTargetRequest(
                    app_label=app_label,
                    migration=migration,
                    fake=fake,
                    dry_run=dry_run,
                ),
            ),
            database_credential_purpose="service_account",
        )

    @migrate_command.command("downgrade", help="Unapply migrations.")
    @_database_url_option
    @click.argument("app_label", required=False)
    @click.argument("migration", required=False)
    @click.option(
        "--fake",
        is_flag=True,
        help="Record migrations without executing SQL.",
    )
    @click.option(
        "--dry-run",
        is_flag=True,
        help="Show what would run without changing database state.",
    )
    @click.pass_context
    def downgrade_command(
        ctx: click.Context,
        database_url: str | None,
        app_label: str | None,
        migration: str | None,
        fake: bool,
        dry_run: bool,
    ) -> int:
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            lambda backend, context: backend.downgrade(
                context,
                MigrationTargetRequest(
                    app_label=app_label,
                    migration=migration,
                    fake=fake,
                    dry_run=dry_run,
                ),
            ),
            database_credential_purpose="service_account",
        )

    @migrate_command.command("history", help="Show migration history.")
    @_database_url_option
    @click.argument("app_labels", nargs=-1)
    @click.pass_context
    def history_command(
        ctx: click.Context,
        database_url: str | None,
        app_labels: tuple[str, ...],
    ) -> int:
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            lambda backend, context: backend.history(context, app_labels),
        )

    @migrate_command.command("heads", help="Show migration heads on disk.")
    @_database_url_option
    @click.argument("app_labels", nargs=-1)
    @click.pass_context
    def heads_command(
        ctx: click.Context,
        database_url: str | None,
        app_labels: tuple[str, ...],
    ) -> int:
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            lambda backend, context: backend.heads(context, app_labels),
        )

    @migrate_command.command("sqlmigrate", help="Print SQL for a migration.")
    @_database_url_option
    @click.argument("app_label", required=False)
    @click.argument("migration_name", required=False)
    @click.option(
        "--backward",
        is_flag=True,
        help="Generate SQL to unapply the migration.",
    )
    @click.pass_context
    def sqlmigrate_command(
        ctx: click.Context,
        database_url: str | None,
        app_label: str | None,
        migration_name: str | None,
        backward: bool,
    ) -> int:
        return migration_runner(
            _database_url_for_command(ctx, database_url),
            _config_source_for_command(ctx),
            lambda backend, context: backend.sqlmigrate(
                context,
                SqlMigrateRequest(
                    app_label=app_label,
                    migration_name=migration_name,
                    backward=backward,
                ),
            ),
        )

    return migrate_command


def _database_url_for_command(
    ctx: click.Context, command_database_url: str | None
) -> str | None:
    if command_database_url is not None:
        return command_database_url

    if ctx.obj is None:
        return None

    if not isinstance(ctx.obj, dict):
        raise click.UsageError(
            "Invalid Click context object for wybra-migrate; expected a dictionary."
        )

    root_database_url = ctx.obj.get("database_url")
    if root_database_url is None:
        return None
    if not isinstance(root_database_url, str):
        raise click.UsageError(
            "Invalid root database_url type "
            f"{type(root_database_url)!r}; expected a string."
        )
    return root_database_url


def _migration_target_from_args(args: Sequence[str]) -> tuple[str | None, str | None]:
    if len(args) > 2:
        raise click.UsageError(
            "migrate accepts at most APP_LABEL and MIGRATION arguments."
        )
    app_label = args[0] if len(args) >= 1 else None
    migration = args[1] if len(args) == 2 else None
    return app_label, migration


def _maintenance_task_from_argument(task_argument: str) -> str:
    task = task_argument.strip()
    if not task:
        raise click.UsageError("run TASK must not be blank.")
    return task


def _config_source_for_command(ctx: click.Context) -> str | None:
    return config_source_from_click_context(
        ctx,
        error_factory=click.UsageError,
        invalid_context_message=(
            "Invalid Click context object for wybra-migrate; expected a dictionary."
        ),
        invalid_type_message=lambda value_type: (
            f"Invalid root config_source type {value_type!r}; expected a string."
        ),
    )


def _run_migration(
    settings_loader: MigrationSettingsLoader,
    database_url: str | None,
    config_source: str | None,
    operation: Callable[[MigrationBackend, MigrationContext], Awaitable[None]],
    *,
    database_credential_purpose: CredentialPurpose = "runtime",
    fallback_to_runtime_credentials: bool = False,
    include_provisioning_connection: bool = False,
    resolve_database_credentials: bool = True,
) -> int:
    return anyio.run(
        partial(
            run_migration,
            settings_loader,
            database_url,
            config_source,
            operation,
            database_credential_purpose=database_credential_purpose,
            fallback_to_runtime_credentials=fallback_to_runtime_credentials,
            include_provisioning_connection=include_provisioning_connection,
            resolve_database_credentials=resolve_database_credentials,
        )
    )


async def run_migration(
    settings_loader: MigrationSettingsLoader,
    database_url: str | None,
    config_source: str | None,
    operation: Callable[[MigrationBackend, MigrationContext], Awaitable[None]],
    *,
    database_credential_purpose: CredentialPurpose = "runtime",
    fallback_to_runtime_credentials: bool = False,
    include_provisioning_connection: bool = False,
    resolve_database_credentials: bool = True,
) -> int:
    try:
        configure_cli_logging()
        settings = _load_migration_settings(
            settings_loader,
            database_url,
            config_source,
            database_credential_purpose=database_credential_purpose,
            fallback_to_runtime_credentials=fallback_to_runtime_credentials,
            include_provisioning_connection=include_provisioning_connection,
            resolve_database_credentials=resolve_database_credentials,
        )
        configure_cli_logging(settings.app_config)
        context = build_migration_context(settings)
        backend: MigrationBackend = TortoiseMigrationBackend()
    except (
        LoggingConfigurationError,
        MigrationConfigurationError,
        DataCompositionError,
    ) as exc:
        logger.error("configuration: failed: %s", exc)
        return 1

    try:
        await operation(backend, context)
    except MigrationConfigurationError as exc:
        logger.error("configuration: failed: %s", exc)
        return 1
    except MigrationStateError as exc:
        logger.error("migration: failed: %s", exc)
        return 1
    except DatabaseProvisioningConfigurationError as exc:
        logger.error("configuration: failed: %s", exc)
        return 1
    except DatabaseProvisioningOperationError as exc:
        logger.error("database lifecycle: failed: %s", safe_database_error_message(exc))
        return 1
    except (TortoiseConfigurationError, tortoise_cli_utils.CLIError) as exc:
        logger.error("migration: failed: %s", safe_database_error_message(exc))
        return 1
    return 0


def _load_migration_settings(
    settings_loader: MigrationSettingsLoader,
    database_url: str | None,
    config_source: str | None,
    *,
    database_credential_purpose: CredentialPurpose = "runtime",
    fallback_to_runtime_credentials: bool = False,
    include_provisioning_connection: bool = False,
    resolve_database_credentials: bool = True,
) -> MigrationSettings:
    optional_kwargs = {
        "database_credential_purpose": database_credential_purpose,
        "fallback_to_runtime_credentials": fallback_to_runtime_credentials,
        "include_provisioning_connection": include_provisioning_connection,
        "resolve_database_credentials": resolve_database_credentials,
    }
    if config_source is None:
        return _call_settings_loader(
            settings_loader,
            database_url,
            _supported_loader_kwargs(settings_loader, optional_kwargs),
        )

    if _loader_accepts_keyword_config_source(settings_loader):
        kwargs = _supported_loader_kwargs(settings_loader, optional_kwargs)
        kwargs["config_source"] = config_source
        return _call_settings_loader(
            settings_loader,
            database_url,
            kwargs,
        )
    raise MigrationConfigurationError(
        "Migration settings loader must accept config_source when --config is used."
    )


def _call_settings_loader(
    settings_loader: MigrationSettingsLoader,
    database_url: str | None,
    kwargs: dict[str, object],
) -> MigrationSettings:
    return cast(Any, settings_loader)(database_url, **kwargs)


def _loader_accepts_keyword_config_source(
    settings_loader: MigrationSettingsLoader,
) -> bool:
    """Return whether a migration settings loader can accept ``config_source``.

    Loader adapters may be simple functions, callable objects, or wrappers.
    Runtime ``--config`` support is enabled only when the callable can accept a
    keyword named ``config_source``, either explicitly or through ``**kwargs``.
    Callables whose signatures cannot be inspected are treated as unsupported
    so the command fails with a configuration error instead of guessing.
    """
    try:
        signature = inspect.signature(settings_loader)
    except TypeError, ValueError:
        return False

    has_explicit_config_source = any(
        parameter.name == "config_source"
        and parameter.kind
        in (
            inspect.Parameter.KEYWORD_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        for parameter in signature.parameters.values()
    )
    if has_explicit_config_source:
        return True

    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )


def _supported_loader_kwargs(
    settings_loader: MigrationSettingsLoader,
    values: dict[str, object],
) -> dict[str, object]:
    try:
        signature = inspect.signature(settings_loader)
    except (TypeError, ValueError) as exc:
        if any(values.values()):
            logger.warning(
                "migration settings loader signature could not be inspected; "
                "optional loader arguments ignored: %s",
                exc,
            )
        return {}
    if any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    ):
        return dict(values)
    return {
        name: value
        for name, value in values.items()
        if any(
            parameter.name == name
            and parameter.kind
            in (
                inspect.Parameter.KEYWORD_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
            for parameter in signature.parameters.values()
        )
    }


def run_migrate_command(
    migrate_command: click.Group,
    argv: Sequence[str] | None = None,
) -> int:
    try:
        result = migrate_command.main(
            args=None if argv is None else list(argv),
            prog_name="wybra-migrate",
            standalone_mode=False,
        )
    except click.exceptions.Exit as exc:
        return int(exc.exit_code or 0)
    except click.ClickException as exc:
        exc.show()
        return int(exc.exit_code or 1)
    return int(result or 0)


def main(argv: Sequence[str] | None = None) -> int:
    return run_migrate_command(migrate_command, argv)


def build_migration_context(settings: MigrationSettings) -> MigrationContext:
    database_connection = _database_connection_for_settings(settings)

    return MigrationContext(
        settings=settings,
        config=build_tortoise_config(settings),
        database_connection=database_connection,
        provisioning_connection=getattr(settings, "provisioning_connection", None),
        migrations_root=settings.migrations_root,
    )


def build_tortoise_config(settings: MigrationSettings) -> dict[str, Any]:
    database_connection = getattr(settings, "database_connection", None)
    if database_connection is not None:
        return build_config(
            database_connection=database_connection,
            modules=settings.modules,
            migrations_root=settings.migrations_root,
        )
    if settings.database_url is None:
        raise MigrationConfigurationError("Database URL is required.")
    return build_config(
        database_url=settings.database_url,
        modules=settings.modules,
        migrations_root=settings.migrations_root,
    )


def _migration_context_with_overrides(
    context: MigrationContext,
    *,
    model_modules: tuple[str, ...],
    migrations_root: Path | None,
) -> MigrationContext:
    modules = (
        tuple(_model_owner_module(module_name) for module_name in model_modules)
        if model_modules
        else context.settings.modules
    )
    root = migrations_root or context.settings.migrations_root
    database_connection = getattr(context.settings, "database_connection", None)
    if database_connection is not None:
        config = build_config(
            database_connection=database_connection,
            modules=modules,
            migrations_root=root,
        )
    else:
        if context.settings.database_url is None:
            raise MigrationConfigurationError("Database URL is required.")
        config = build_config(
            database_url=context.settings.database_url,
            modules=modules,
            migrations_root=root,
        )
    return replace(context, config=config, migrations_root=root)


def _model_owner_module(module_name: str) -> str:
    model_surface_suffix = f".{MODEL_SURFACE_MODULE}"
    if module_name.endswith(model_surface_suffix):
        return module_name.removesuffix(model_surface_suffix)
    return module_name


async def initialise_migration_lifecycle(
    backend: MigrationBackend,
    context: MigrationContext,
    *,
    app_labels: tuple[str, ...],
) -> None:
    await initialise_database_lifecycle(context)
    await backend.initialise(context, app_labels=app_labels)


async def initialise_database_lifecycle(context: MigrationContext) -> None:
    _report_provisioning_results(
        await initialise_database(_provisioning_context(context))
    )


async def destroy_database_lifecycle(
    context: MigrationContext,
    request: DestroyDatabaseRequest,
) -> None:
    _report_provisioning_results(
        await destroy_database(_provisioning_context(context), request)
    )


async def list_database_maintenance_tasks_lifecycle(
    _backend: MigrationBackend,
    context: MigrationContext,
) -> None:
    provisioning_context = _provisioning_context(context)
    _report_database_maintenance_tasks(
        provisioning_context.family,
        database_maintenance_tasks(provisioning_context),
    )


async def run_database_maintenance_lifecycle(
    context: MigrationContext,
    request: DatabaseMaintenanceRequest,
) -> None:
    _report_provisioning_results(
        await run_database_maintenance(_provisioning_context(context), request)
    )


def _report_database_maintenance_tasks(
    family: str,
    tasks: Sequence[DatabaseMaintenanceTask],
) -> None:
    if not tasks:
        click.echo(f"No database maintenance tasks are available for {family}.")
        return

    for task in tasks:
        click.echo(f"{task.name}: {task.description}")
        click.echo(f"  credentials: {task.credential_scope}")
        if task.recommended_frequency is not None:
            click.echo(f"  recommended: {task.recommended_frequency}")
        if task.requires_confirmation:
            click.echo("  confirmation: required")


def _report_provisioning_results(
    results: Sequence[ProvisioningPhaseResult],
) -> None:
    for result in results:
        logger.info(
            "database lifecycle: %s %s %s: %s",
            result.family,
            result.phase,
            result.status,
            result.message,
        )


def _provisioning_context(context: MigrationContext) -> ProvisioningContext:
    return provisioning_context(
        runtime_connection=context.database_connection,
        provisioning_connection=context.provisioning_connection,
        project_root=context.settings.project_root,
        modules=context.settings.modules,
    )


def _database_connection_for_settings(
    settings: MigrationSettings,
) -> ResolvedDatabaseConnection:
    database_connection = getattr(settings, "database_connection", None)
    if database_connection is not None:
        return database_connection

    if settings.database_url is None or not settings.database_url.strip():
        raise MigrationConfigurationError("Database URL is required.")
    if not is_supported_database_url(settings.database_url):
        raise MigrationConfigurationError(
            database_url_support_error(settings.database_url)
        )
    backend = database_backend_for_url(settings.database_url)
    if backend is None:  # pragma: no cover - is_supported_database_url checked above
        raise MigrationConfigurationError(
            database_url_support_error(settings.database_url)
        )
    return ResolvedDatabaseConnection.from_url(settings.database_url, backend=backend)


class _VersionFieldMigrationAutodetector(MigrationAutodetector):
    """Teach Tortoise's normal migration generation about ``VersionField``."""

    def _relation_dependencies(self, app_label, new_state):  # type: ignore[no-untyped-def]
        """Include new cross-app initial migrations absent from Tortoise's graph.

        Tortoise only discovers related-app leaf migrations that already exist in
        its loader graph. During a fresh multi-app baseline, every initial
        migration is new, so model-derived cross-app edges would otherwise be
        lost. The current model state is authoritative for dependency edges.
        Where an app retains committed migrations during application, its
        recorded migration leaves provide the dependency target; otherwise its
        generated initial migration is the target.
        """
        dependencies = super()._relation_dependencies(app_label, new_state)
        for (model_app, _model_name), model_state in new_state.models.items():
            if model_app != app_label:
                continue
            for field in model_state.fields.values():
                if not isinstance(field, RELATION_FIELDS):
                    continue
                model_name = field.model_name
                if isinstance(model_name, str):
                    related_app, _related_model = model_name.split(".", 1)
                else:
                    related_app = getattr(
                        getattr(model_name, "_meta", None), "app", None
                    )
                    if not isinstance(related_app, str):
                        continue
                if related_app == app_label or related_app not in self.apps_config:
                    continue
                if self._leaf_nodes(related_app):
                    continue
                if any(
                    related_model_app == related_app
                    for related_model_app, _ in new_state.models
                ):
                    committed_leaves = (_relation_dependency_leaves.get() or {}).get(
                        related_app,
                        (),
                    )
                    if committed_leaves:
                        dependencies.update(committed_leaves)
                    else:
                        dependencies.add(
                            MigrationKey(app_label=related_app, name="0001_initial")
                        )
        return dependencies

    def _current_state(self):  # type: ignore[no-untyped-def]
        state = super()._current_state()
        for app_label, models in self.apps.items():
            for model in models.values():
                model_state = state.models[(app_label, model.__name__)]
                constraints = list(model_state.options.get("constraints", ()))
                for field_name, field in model._meta.fields_map.items():
                    if not isinstance(field, VersionField):
                        continue
                    constraint = version_field_check_constraint(model, field_name)
                    if any(
                        isinstance(existing, CheckConstraint)
                        and existing.check == constraint.check
                        for existing in constraints
                    ):
                        continue
                    constraints.append(constraint)
                if constraints:
                    model_state.options["constraints"] = tuple(constraints)
        return state

    async def changes(self):  # type: ignore[no-untyped-def]
        writers = await super().changes()
        for writer in writers:
            operations = []
            for operation in writer.operations:
                operations.append(operation)
                if not isinstance(operation, CreateModel):
                    continue
                constraints = list((operation.options or {}).get("constraints", ()))
                version_constraints = _version_constraints_for_create_model(operation)
                if not version_constraints:
                    continue
                operation.options = {
                    **(operation.options or {}),
                    "constraints": tuple(
                        constraint
                        for constraint in constraints
                        if constraint not in version_constraints
                    ),
                }
                operations.extend(
                    AddConstraint(model_name=operation.name, constraint=constraint)
                    for constraint in version_constraints
                )
            writer.operations = operations
        return writers


def _version_constraints_for_create_model(
    operation: CreateModel,
) -> tuple[CheckConstraint, ...]:
    """Extract generated version constraints so Tortoise emits ``AddConstraint``."""
    table_name = str((operation.options or {}).get("table", operation.name.lower()))
    constraints = tuple((operation.options or {}).get("constraints", ()))
    version_constraints = []
    for field_name, field in operation.fields:
        if not isinstance(field, VersionField):
            continue
        constraint = _version_check_constraint_for_create_field(
            table_name,
            field_name,
            field,
        )
        if constraint in constraints:
            version_constraints.append(constraint)
    return tuple(version_constraints)


def _version_check_constraint_for_create_field(
    table_name: str,
    field_name: str,
    field: VersionField,
) -> CheckConstraint:
    column_name = field.source_field or field_name
    return version_column_check_constraint(table_name, column_name)


_relation_dependency_leaves: ContextVar[dict[str, tuple[MigrationKey, ...]] | None] = (
    ContextVar("relation_dependency_leaves", default=None)
)


@contextmanager
def _version_field_migration_autodetector(
    *,
    relation_dependency_leaves: dict[str, tuple[MigrationKey, ...]] | None = None,
) -> Iterator[None]:
    """Install the generated-constraint extension for one CLI invocation."""
    cli_module = cast(Any, tortoise_cli)
    original = cli_module.MigrationAutodetector
    leaves_token = _relation_dependency_leaves.set(relation_dependency_leaves or {})
    cli_module.MigrationAutodetector = _VersionFieldMigrationAutodetector
    try:
        yield
    finally:
        cli_module.MigrationAutodetector = original
        _relation_dependency_leaves.reset(leaves_token)


async def _run_tortoise_cli(context: MigrationContext, args: Sequence[str]) -> None:
    await _run_tortoise_config(context.config, context.migrations_root, args)


async def _generate_model_migrations(
    config: dict[str, Any],
    migrations_root: Path | None,
    request: MakeMigrationsRequest,
    *,
    quiet: bool = False,
    relation_dependency_leaves: dict[str, tuple[MigrationKey, ...]] | None = None,
) -> ModelMigrationPlan | None:
    """Generate a fresh model baseline from current-model dependencies.

    Full generation validates the finalised model graph before invoking
    Tortoise. The Wybra autodetector extension adds missing dependencies between
    simultaneously-created initial migrations; Tortoise cannot target one app
    at a time because it omits the other apps needed to resolve its relations.
    """
    if request.app_labels or request.empty:
        await _run_tortoise_config(
            config,
            migrations_root,
            _makemigrations_arguments(request),
            quiet=quiet,
            relation_dependency_leaves=relation_dependency_leaves,
        )
        return None

    plan = await model_migration_plan(config)
    locations = _migration_locations_by_app(config, migrations_root)
    before = {
        app_label: _migration_source_paths(location)
        for app_label, location in locations.items()
    }
    await _run_tortoise_config(
        config,
        migrations_root,
        _makemigrations_arguments(request),
        quiet=quiet,
        relation_dependency_leaves=relation_dependency_leaves,
    )
    for app_label in plan.app_labels:
        _verify_generated_initial_dependencies(
            app_label,
            plan.dependencies_for(app_label),
            _migration_source_paths(locations[app_label]) - before[app_label],
        )
    return plan


def _makemigrations_arguments(request: MakeMigrationsRequest) -> list[str]:
    args = ["makemigrations", *request.app_labels]
    if request.empty:
        args.append("--empty")
    if request.name is not None:
        args.extend(("-n", request.name))
    return args


async def model_migration_plan(config: dict[str, Any]) -> ModelMigrationPlan:
    """Return a stable app plan derived solely from finalised Tortoise models."""
    apps_config = config.get("apps")
    if not isinstance(apps_config, dict):
        raise MigrationConfigurationError("Tortoise configuration has no apps mapping.")
    app_labels = tuple(apps_config)
    dependencies: dict[str, set[str]] = {app_label: set() for app_label in app_labels}

    async with TortoiseContext() as tortoise_context:
        await tortoise_context.init(config=config, init_connections=False)
        apps = tortoise_context.apps
        if apps is None:  # pragma: no cover - TortoiseContext.init guarantees apps
            raise MigrationStateError("Tortoise did not initialise model apps.")
        relation_fields = (
            ForeignKeyFieldInstance,
            ManyToManyFieldInstance,
            OneToOneFieldInstance,
        )
        for app_label, models in apps.items():
            if app_label not in dependencies:
                continue
            for model in models.values():
                for field in model._meta.fields_map.values():
                    if not isinstance(field, relation_fields):
                        continue
                    related_model = field.related_model
                    related_app = getattr(
                        getattr(related_model, "_meta", None), "app", None
                    )
                    if (
                        isinstance(related_app, str)
                        and related_app in dependencies
                        and related_app != app_label
                    ):
                        dependencies[app_label].add(related_app)

    ordered_apps = _topological_migration_app_order(app_labels, dependencies)
    return ModelMigrationPlan(
        app_labels=ordered_apps,
        dependencies=tuple(
            (app_label, tuple(sorted(dependencies[app_label])))
            for app_label in app_labels
        ),
    )


def _topological_migration_app_order(
    app_labels: tuple[str, ...],
    dependencies: dict[str, set[str]],
) -> tuple[str, ...]:
    """Topologically order apps, preserving configured order when independent."""
    remaining = {app_label: set(dependencies[app_label]) for app_label in app_labels}
    ordered: list[str] = []
    while ready := [
        app_label
        for app_label in app_labels
        if app_label in remaining and not remaining[app_label]
    ]:
        app_label = ready[0]
        ordered.append(app_label)
        remaining.pop(app_label)
        for app_dependencies in remaining.values():
            app_dependencies.discard(app_label)
    if remaining:
        participants = ", ".join(sorted(remaining))
        raise MigrationStateError(
            "Cross-app model relations form a migration dependency cycle: "
            f"{participants}. Use deliberate staged schema work for the cycle."
        )
    return tuple(ordered)


def _migration_locations_by_app(
    config: dict[str, Any],
    migrations_root: Path | None,
) -> dict[str, Path]:
    apps = config.get("apps")
    if not isinstance(apps, dict):
        raise MigrationConfigurationError("Tortoise configuration has no apps mapping.")
    if migrations_root is not None:
        return {app_label: migrations_root / app_label for app_label in apps}
    locations: dict[str, Path] = {}
    for app_label, app_config in apps.items():
        locations[app_label] = _migration_location(app_label, app_config)
    return locations


def _migration_location(app_label: str, app_config: object) -> Path:
    if not isinstance(app_config, dict):
        raise MigrationConfigurationError(
            f"Tortoise app configuration is invalid: {app_label}."
        )
    migrations_module = app_config.get("migrations")
    if not isinstance(migrations_module, str):
        raise MigrationConfigurationError(
            f"Tortoise app has no migrations module: {app_label}."
        )
    module = __import__(migrations_module, fromlist=["__path__"])
    paths = getattr(module, "__path__", ())
    if not paths:
        raise MigrationConfigurationError(
            f"Tortoise migrations module is not a package: {migrations_module}."
        )
    return Path(next(iter(paths)))


def _migration_source_paths(location: Path) -> set[Path]:
    return {
        path for path in location.glob("[0-9][0-9][0-9][0-9]_*.py") if path.is_file()
    }


def _verify_generated_initial_dependencies(
    app_label: str,
    required_apps: tuple[str, ...],
    generated_paths: set[Path],
) -> None:
    for migration_path in generated_paths:
        if migration_path.stem != "0001_initial":
            continue
        actual_apps = {
            dependency_app
            for dependency_app, _name in migration_dependencies(migration_path)
        }
        missing_apps = sorted(set(required_apps) - actual_apps)
        if missing_apps:
            raise MigrationStateError(
                "Generated initial migration is missing model-derived dependencies: "
                f"app={app_label}, missing={', '.join(missing_apps)}."
            )


async def _run_tortoise_config(
    config: dict[str, Any],
    migrations_root: Path | None,
    args: Sequence[str],
    *,
    quiet: bool = False,
    relation_dependency_leaves: dict[str, tuple[MigrationKey, ...]] | None = None,
) -> None:
    with _temporary_migrations_package(migrations_root):
        config_module = _register_tortoise_config_module(config)
        try:
            with (
                _tortoise_migration_recorder_compatibility(),
                _version_field_migration_autodetector(
                    relation_dependency_leaves=relation_dependency_leaves,
                ),
            ):
                arguments = [
                    "--config",
                    f"{config_module}.{TORTOISE_CONFIG_VARIABLE}",
                    *args,
                ]
                if quiet:
                    with redirect_stdout(io.StringIO()):
                        exit_code = await tortoise_cli.run_cli_async(arguments)
                else:
                    exit_code = await tortoise_cli.run_cli_async(arguments)
        finally:
            sys.modules.pop(config_module, None)

    if exit_code:
        command_name = args[0] if args else "<unknown>"
        raise MigrationStateError(
            "Tortoise migration command failed: "
            f"command={command_name}, exit_code={exit_code}."
        )


@asynccontextmanager
async def generated_temporary_migrations(
    config: dict[str, Any],
    *,
    app_labels: tuple[str, ...] | None = None,
) -> AsyncIterator[GeneratedTemporaryMigrations]:
    """Generate test migrations safely in an isolated root and remove them.

    Tortoise's autodetector examines every configured app, regardless of which
    app migration paths the test lifecycle will ultimately apply.  Therefore
    every app is redirected to the disposable root while generating.  The
    yielded configuration restores committed migration paths for apps that do
    not require generated test migrations.
    """
    with TemporaryDirectory(prefix="wybra-migrations-") as directory:
        root = Path(directory)
        generation_config = _config_with_migrations_root(config, root)
        application_config = _config_with_migrations_root(
            config,
            root,
            app_labels=app_labels,
        )
        await _generate_model_migrations(
            generation_config,
            root,
            MakeMigrationsRequest(app_labels=(), empty=False, name=None),
            quiet=True,
            relation_dependency_leaves=_committed_migration_leaves(
                config,
                retained_app_labels=_retained_migration_app_labels(
                    config,
                    app_labels=app_labels,
                ),
            ),
        )
        paths = tuple(sorted(root.glob("*/[0-9][0-9][0-9][0-9]_*.py")))
        yield GeneratedTemporaryMigrations(
            config=application_config,
            root=root,
            paths=paths,
        )


async def plan_migration_reset(context: MigrationContext) -> MigrationResetPlan:
    """Generate a disposable model baseline and reject unplanned exceptions."""
    migration_locations = migration_version_locations_from_modules(
        context.settings.modules
    )
    special_migrations = tuple(
        migration_path
        for location in migration_locations
        for migration_path in location.glob("*.py")
        if migration_path.name != "__init__.py"
        and special_migration_description(migration_path) is not None
    )
    if special_migrations:
        paths = ", ".join(str(path) for path in special_migrations)
        raise MigrationStateError(
            "Migration reset requires a reviewed baseline-compatible provenance "
            f"plan for special migrations: {paths}."
        )

    async with generated_temporary_migrations(context.config) as baseline:
        return MigrationResetPlan(
            generated_baseline=baseline.paths,
            migration_locations=migration_locations,
        )


async def reset_migrations_lifecycle(
    context: MigrationContext,
    *,
    confirm: str,
) -> MigrationResetPlan:
    """Destroy a confirmed database then remove replaceable migration files."""
    plan = await plan_migration_reset(context)
    if not plan.generated_baseline:
        raise MigrationStateError(
            "Migration reset generated no baseline migrations; refusing to remove "
            "committed migration history."
        )
    await destroy_database_lifecycle(context, DestroyDatabaseRequest(confirm=confirm))
    for location in plan.migration_locations:
        for migration_path in location.glob("*.py"):
            if migration_path.name != "__init__.py":
                migration_path.unlink()
    return plan


async def _destroy_command_lifecycle(
    context: MigrationContext,
    *,
    confirm: str,
    reset_migrations: bool,
) -> None:
    if reset_migrations:
        await reset_migrations_lifecycle(context, confirm=confirm)
        return
    await destroy_database_lifecycle(
        context,
        DestroyDatabaseRequest(confirm=confirm),
    )


def _config_with_migrations_root(
    config: dict[str, Any],
    migrations_root: Path,
    *,
    app_labels: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    apps = config.get("apps")
    if not isinstance(apps, dict):
        raise MigrationConfigurationError("Tortoise configuration has no apps.")
    if not all(
        isinstance(app_label, str) and isinstance(app_config, dict)
        for app_label, app_config in apps.items()
    ):
        raise MigrationConfigurationError("Tortoise configuration has invalid apps.")
    typed_apps = cast(dict[str, dict[str, Any]], apps)
    migrations_package = tortoise_migrations_package(migrations_root)
    temporary_labels = set(typed_apps) if app_labels is None else set(app_labels)
    unknown_labels = temporary_labels - set(typed_apps)
    if unknown_labels:
        raise MigrationConfigurationError(
            "Temporary migration apps are not configured: "
            f"{', '.join(sorted(unknown_labels))}."
        )
    return {
        **config,
        "apps": {
            app_label: {
                **app_config,
                "migrations": (
                    f"{migrations_package}.{app_label}"
                    if app_label in temporary_labels
                    else app_config["migrations"]
                ),
            }
            for app_label, app_config in typed_apps.items()
        },
    }


def _retained_migration_app_labels(
    config: dict[str, Any],
    *,
    app_labels: tuple[str, ...] | None,
) -> tuple[str, ...]:
    """Return apps whose committed migrations will be applied after generation."""
    apps = config.get("apps")
    if not isinstance(apps, dict):
        raise MigrationConfigurationError("Tortoise configuration has no apps.")
    temporary_labels = set(apps) if app_labels is None else set(app_labels)
    return tuple(app_label for app_label in apps if app_label not in temporary_labels)


def _committed_migration_leaves(
    config: dict[str, Any],
    *,
    retained_app_labels: tuple[str, ...],
) -> dict[str, tuple[MigrationKey, ...]]:
    """Read migration leaves for apps that retain their committed history."""
    apps = config.get("apps")
    if not isinstance(apps, dict):
        raise MigrationConfigurationError("Tortoise configuration has no apps.")
    leaves_by_app: dict[str, tuple[MigrationKey, ...]] = {}
    for app_label in retained_app_labels:
        if app_label not in apps:
            raise MigrationConfigurationError(
                f"Retained migration app is not configured: {app_label}."
            )
        paths = _migration_source_paths(_migration_location(app_label, apps[app_label]))
        names = {path.stem for path in paths}
        parent_names = {
            dependency_name
            for path in paths
            for dependency_app, dependency_name in migration_dependencies(path)
            if dependency_app == app_label and dependency_name in names
        }
        leaf_names = tuple(sorted(names - parent_names))
        if leaf_names:
            leaves_by_app[app_label] = tuple(
                MigrationKey(app_label=app_label, name=name) for name in leaf_names
            )
    return leaves_by_app


def apps_requiring_temporary_migrations(config: dict[str, Any]) -> tuple[str, ...]:
    """Return configured apps whose migration package is absent from source."""
    apps = config.get("apps")
    if not isinstance(apps, dict):
        raise MigrationConfigurationError("Tortoise configuration has no apps.")
    labels = []
    for app_label, app_config in apps.items():
        if not isinstance(app_config, dict):
            raise MigrationConfigurationError(
                "Tortoise configuration has invalid apps."
            )
        migrations_module = app_config.get("migrations")
        if not isinstance(migrations_module, str):
            raise MigrationConfigurationError(
                f"Tortoise app has no migrations module: {app_label}."
            )
        spec = find_spec(migrations_module)
        locations = () if spec is None else spec.submodule_search_locations
        has_migration_source = bool(locations) and any(
            any(path.glob("[0-9][0-9][0-9][0-9]_*.py"))
            for path in (Path(location) for location in locations)
        )
        if not has_migration_source:
            labels.append(app_label)
    return tuple(labels)


def _register_tortoise_config_module(config: dict[str, Any]) -> str:
    module_name = f"_wybra_tortoise_config_{uuid.uuid4().hex}"
    module = types.ModuleType(module_name)
    setattr(module, TORTOISE_CONFIG_VARIABLE, config)
    sys.modules[module_name] = module
    return module_name


@contextmanager
def _temporary_migrations_package(migrations_root: Path | None) -> Iterator[None]:
    """Make an isolated migration root importable for one Tortoise command."""
    if migrations_root is None:
        yield
        return

    migrations_root.mkdir(parents=True, exist_ok=True)
    package_name = tortoise_migrations_package(migrations_root)
    if package_name in sys.modules:
        raise MigrationStateError(
            f"Temporary migrations package is already active: {package_name}."
        )

    package = types.ModuleType(package_name)
    package.__dict__.update(
        __package__=package_name,
        __path__=[str(migrations_root)],
    )
    sys.modules[package_name] = package
    try:
        yield
    finally:
        for module_name in tuple(sys.modules):
            if module_name == package_name or module_name.startswith(
                f"{package_name}."
            ):
                sys.modules.pop(module_name, None)


async def apply_tortoise_migrations(
    connection: BaseDBAsyncClient,
    apps: dict[str, dict[str, object]],
    *,
    migrations_root: Path | None = None,
) -> None:
    """Apply native Tortoise migrations using an existing database connection."""
    with _temporary_migrations_package(migrations_root):
        with _tortoise_migration_recorder_compatibility():
            await MigrationExecutor(connection, apps).migrate()


@contextmanager
def _tortoise_migration_recorder_compatibility() -> Iterator[None]:
    original_record_applied = MigrationRecorder.record_applied
    original_make_model = MigrationRecorder._make_model

    def recorder_dialect(self: Any) -> str:
        dialect = getattr(self, "_dialect", "")
        if isinstance(dialect, str) and dialect:
            return dialect
        connection = getattr(self, "connection", None)
        capabilities = getattr(connection, "capabilities", None)
        return str(getattr(capabilities, "dialect", ""))

    async def record_applied(self: Any, app: str, name: str) -> None:
        if recorder_dialect(self) != "mysql":
            await original_record_applied(self, app, name)
            return

        # Tortoise currently writes timezone-aware ISO strings for migration
        # recorder timestamps. MariaDB rejects those for DATETIME in strict
        # mode, so use a plain UTC DATETIME literal for MySQL-family backends
        # until the upstream recorder handles this natively.
        applied_at = datetime.now(UTC).replace(tzinfo=None).isoformat(" ")
        query = render_sql(
            t"INSERT INTO {ident(self.table_name)} "
            t"({ident('app')}, {ident('name')}, {ident('applied_at')}) "
            t"VALUES ({param(app)}, {param(name)}, {param(applied_at)})",
            dialect="mysql",
            quote_identifier=quote_mysql_identifier,
        )
        await self.connection.execute_query(query.statement, list(query.parameters))

    def make_model(self: Any, table_name: str) -> type[Model]:
        class MigrationRecord(Model):
            # Tortoise 1.1 still calls this deprecated field alias with
            # ``pk=True``. Use the current spelling until upstream updates its
            # migration recorder implementation.
            id = fields.IntField(primary_key=True)
            app = fields.CharField(max_length=255)
            name = fields.CharField(max_length=255)
            applied_at = fields.DatetimeField()

            class Meta:
                table = table_name
                app = "_migrations"
                unique_together = (("app", "name"),)

        return MigrationRecord

    MigrationRecorder._make_model = make_model
    MigrationRecorder.record_applied = record_applied
    try:
        yield
    finally:
        MigrationRecorder._make_model = original_make_model
        MigrationRecorder.record_applied = original_record_applied


def _missing_settings_loader(
    database_url: str | None,
    *,
    config_source: str | None = None,
    **_extra: object,
) -> MigrationSettings:
    del database_url, config_source, _extra
    raise MigrationConfigurationError("Migration settings loader is not configured.")


migrate_command = create_migrate_command(_missing_settings_loader)
