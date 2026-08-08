from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Protocol

import pytest
from fastapi import FastAPI
from starlette.exceptions import HTTPException as StarletteHTTPException

from wybra import Site, SiteCapabilityError, SiteCapabilityProxy, get_site, start_site
from wybra.config import (
    ConfigService,
    ConfigSourceError,
    ConfigSourceResult,
    MappingConfigSource,
)
from wybra.core.composition import (
    APP_CONFIG_ENV,
    APP_ROOT_ENV,
    AppConfig,
    AssetOptions,
    CompositionError,
    RouteOptions,
    TemplateOptions,
    load_app_config,
)
from wybra.core.config import ENV_APP_DEBUG, ENV_APP_ENV
from wybra.core.exceptions import ConfigurationError, Http404
from wybra.core.routes import inspect_route_tree
from wybra.db.config import ENV_DATABASE_URL
from wybra.errors.capabilities import ErrorHandlingCapability
from wybra.errors.handlers import EmptyBodyResponseException
from wybra.site import _local_file_uri_path, start
from wybra.site_config import app_config_from_site
from wybra.tasks import TaskFeatures, TasksCapability
from wybra.testing import WybraTestClient


class ExampleCapability:
    def label(self) -> str:
        return "example"


class OtherCapability:
    pass


class UnsupportedCapability(Protocol):
    pass


class ExampleImplementation(ExampleCapability):
    def __init__(self) -> None:
        self.calls = 0

    def label(self) -> str:
        self.calls += 1
        return "implementation"


class LateCapability:
    def label(self) -> str:
        return "late"


class ClosingCapability:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class SyncClosingCapability:
    def close(self) -> None:
        pass


class OrderedClosingCapability:
    def __init__(self, name: str, closed: list[str]) -> None:
        self.name = name
        self.closed = closed

    async def close(self) -> None:
        self.closed.append(self.name)


class DependentClosingCapability(OrderedClosingCapability):
    pass


class ClosingTasksCapability:
    features = TaskFeatures()

    def __init__(self, closed: list[str]) -> None:
        self.closed = closed

    async def submit(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("Task submission is not exercised by this test.")

    async def status(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("Task status is not exercised by this test.")

    async def lifecycle(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("Task lifecycle is not exercised by this test.")

    async def close(self) -> None:
        self.closed.append("tasks")


def _write_app_config(
    path: Path,
    *,
    modules: tuple[str, ...],
    asset_root: str | None = None,
    deployment_environment: str | None = None,
    debug: bool | None = None,
    log_format: str | None = None,
) -> Path:
    asset_root_config = f'        root = "{asset_root}"\n' if asset_root else ""
    deployment_config = (
        f'        deployment_environment = "{deployment_environment}"\n'
        if deployment_environment is not None
        else ""
    )
    debug_config = (
        f"        debug = {str(debug).lower()}\n" if debug is not None else ""
    )
    log_config = (
        f"""
        [log]
        version = 1
        disable_existing_loggers = false

        [log.formatters.simple]
        format = "{log_format}"

        [log.handlers.console]
        class = "logging.StreamHandler"
        level = "INFO"
        formatter = "simple"
        stream = "ext://sys.stderr"

        [log.root]
        level = "INFO"
        handlers = ["console"]
        """
        if log_format is not None
        else ""
    )
    path.write_text(
        f"""
        [app]
        modules = {json.dumps(list(modules))}
        database_url = "sqlite:///app.sqlite3"
{deployment_config.rstrip()}
{debug_config.rstrip()}

        [app.routes]

        [app.templates]
        auto_reload = true
        cache_size = 0

        [app.assets]
        url_path = "/static/"
{asset_root_config.rstrip()}
{log_config.rstrip()}
        """,
        encoding="utf-8",
    )
    return path


def _append_explicit_file_sessions_config(path: Path, directory: Path) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            f"""

            [wybra.sessions]
            storage_backend = "file"
            file_directory = "{directory.as_posix()}"
            """
        )


def _site_from_mapping(values: dict[str, dict[str, object]]) -> Site:
    return Site(
        app=FastAPI(),
        config=ConfigService(
            [MappingConfigSource(values)],
            discover_module_config=False,
        ),
    )


@pytest.fixture(autouse=True)
def restore_root_logging():
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    original_disabled = logging.root.manager.disable
    yield
    for handler in list(root.handlers):
        root.removeHandler(handler)
        if handler not in original_handlers:
            handler.close()
    root.handlers[:] = original_handlers
    root.setLevel(original_level)
    logging.disable(original_disabled)


class TestSiteComposition:
    @pytest.mark.anyio
    async def test_start_applies_logging_before_module_hooks(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        package_root = tmp_path / "logging_hook_app"
        package_root.mkdir()
        (package_root / "__init__.py").write_text(
            "import logging\n"
            "async def setup_site(site):\n"
            "    logging.getLogger('logging_hook_app').warning('setup hook')\n"
            "async def post_setup_site(site):\n"
            "    logging.getLogger('logging_hook_app').warning('post setup hook')\n",
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        config_path = _write_app_config(
            tmp_path / "app.toml",
            modules=("logging_hook_app",),
            log_format="CONFIGURED %(levelname)s %(name)s %(message)s",
        )
        app_config = load_app_config(project_root=tmp_path, config_path=config_path)

        await start(FastAPI(), config_source=app_config)

        output = capsys.readouterr().err
        assert "CONFIGURED WARNING logging_hook_app setup hook" in output
        assert "CONFIGURED WARNING logging_hook_app post setup hook" in output
        assert "WARNING:logging_hook_app:" not in output

    @pytest.mark.anyio
    async def test_start_applies_default_logging_before_module_hooks(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        package_root = tmp_path / "default_logging_hook_app"
        package_root.mkdir()
        (package_root / "__init__.py").write_text(
            "import logging\n"
            "async def setup_site(site):\n"
            "    logging.getLogger('default_logging_hook_app').warning('setup hook')\n",
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        config_path = _write_app_config(
            tmp_path / "app.toml",
            modules=("default_logging_hook_app",),
        )
        app_config = load_app_config(project_root=tmp_path, config_path=config_path)

        await start(FastAPI(), config_source=app_config)

        output = capsys.readouterr().err.strip()
        assert re.match(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4} "
            r"WARNING default_logging_hook_app setup hook",
            output,
        )

    @pytest.mark.anyio
    async def test_start_registers_configured_routes_without_web_module(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        package_root = tmp_path / "core_route_app"
        package_root.mkdir()
        (package_root / "__init__.py").write_text("", encoding="utf-8")
        (package_root / "routes.py").write_text(
            "from fastapi import APIRouter\n"
            "router = APIRouter()\n"
            "@router.get('/status', name='status')\n"
            "async def status():\n"
            "    return {'ok': True}\n"
            "module_routers = {'default': router}\n",
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))

        app = FastAPI()
        await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {
                        "config_path": tmp_path / "app.toml",
                        "project_root": tmp_path,
                        "modules": ("core_route_app",),
                    },
                    "app.routes": {"prefixes": {"core_route_app": {"default": "/api"}}},
                }
            ),
        )

        paths = {route.path for route in inspect_route_tree(app).routes}

        assert "/api/status" in paths

    @pytest.mark.anyio
    async def test_start_requires_api_capability_for_routes_declaring_api_semantics(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        package_root = tmp_path / "api_route_app"
        package_root.mkdir()
        (package_root / "__init__.py").write_text("", encoding="utf-8")
        (package_root / "routes.py").write_text(
            "from fastapi import APIRouter\n"
            "from wybra.core.routes import RouteType, route\n"
            "router = APIRouter()\n"
            "@router.get('/status', name='status')\n"
            "async def status():\n"
            "    return {'ok': True}\n"
            "status = route('/status', RouteType.API)(status)\n"
            "module_routers = {'default': router}\n",
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))

        with pytest.raises(SiteCapabilityError, match="ApiCapability"):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {
                        "app": {
                            "config_path": tmp_path / "app.toml",
                            "project_root": tmp_path,
                            "modules": ("api_route_app",),
                        },
                        "app.routes": {
                            "prefixes": {"api_route_app": {"default": "/api"}}
                        },
                    }
                ),
            )

    @pytest.mark.anyio
    async def test_start_accepts_api_semantics_when_api_module_is_configured(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        package_root = tmp_path / "configured_api_route_app"
        package_root.mkdir()
        (package_root / "__init__.py").write_text("", encoding="utf-8")
        (package_root / "routes.py").write_text(
            "from fastapi import APIRouter\n"
            "from wybra.core.routes import RouteType, route\n"
            "router = APIRouter()\n"
            "@router.get('/status', name='status')\n"
            "async def status():\n"
            "    return {'ok': True}\n"
            "status = route('/status', RouteType.API)(status)\n"
            "module_routers = {'default': router}\n",
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))

        app = FastAPI()
        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {
                        "config_path": tmp_path / "app.toml",
                        "project_root": tmp_path,
                        "modules": ("wybra.api", "configured_api_route_app"),
                    },
                    "app.routes": {
                        "prefixes": {"configured_api_route_app": {"default": "/api"}}
                    },
                }
            ),
        )

        assert site.has_module("wybra.api")
        assert "/api/status" in {route.path for route in inspect_route_tree(app).routes}

    def test_app_config_from_site_rejects_non_mapping_route_prefixes(self) -> None:
        site = _site_from_mapping(
            {
                "app": {"modules": ("host_app",)},
                "app.routes": {"prefixes": []},
            }
        )

        with pytest.raises(
            CompositionError,
            match=r"'app\.routes' prefixes must be a mapping",
        ):
            app_config_from_site(site)

    def test_app_config_from_site_rejects_non_mapping_module_route_prefixes(
        self,
    ) -> None:
        site = _site_from_mapping(
            {
                "app": {"modules": ("host_app",)},
                "app.routes": {"prefixes": {"host_app": ""}},
            }
        )

        with pytest.raises(
            CompositionError,
            match=r"prefixes for 'host_app' must be a mapping",
        ):
            app_config_from_site(site)

    def test_app_config_from_site_rejects_non_string_route_prefix(self) -> None:
        site = _site_from_mapping(
            {
                "app": {"modules": ("host_app",)},
                "app.routes": {"prefixes": {"host_app": {"admin": 123}}},
            }
        )

        with pytest.raises(
            CompositionError,
            match=r"prefix for 'host_app' router 'admin' must be a string",
        ):
            app_config_from_site(site)

    def test_app_config_from_site_rejects_malformed_scalar_options(self) -> None:
        site = _site_from_mapping(
            {
                "app": {"modules": ("host_app",)},
                "app.templates": {"cache_size": -1},
            }
        )

        with pytest.raises(
            CompositionError,
            match=r"cache_size' must be a non-negative integer",
        ):
            app_config_from_site(site)

    @pytest.mark.anyio
    async def test_start_composes_existing_fastapi_app_from_file_source(
        self,
        tmp_path: Path,
    ) -> None:
        app = FastAPI(title="Host app")
        config_path = _write_app_config(
            tmp_path / "app.toml",
            modules=("wybra.assets",),
        )

        site = await start(app, config_source=str(config_path))

        assert isinstance(site, Site)
        assert site.app is app
        assert site.modules == ("wybra.assets",)
        assert site.has_module("wybra.assets") is True
        assert site.has_module("wybra.auth") is False

    @pytest.mark.anyio
    async def test_start_accepts_relative_file_source_string(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_app_config(
            tmp_path / "app.toml",
            modules=("wybra.assets",),
        )
        monkeypatch.chdir(tmp_path)

        site = await start(FastAPI(), config_source="app.toml")

        assert site.modules == ("wybra.assets",)

    @pytest.mark.anyio
    async def test_start_uses_app_root_environment_for_app_config(
        self,
        tmp_path: Path,
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        _write_app_config(
            project_root / "app.toml",
            modules=("wybra.assets",),
        )

        site = await start(
            FastAPI(),
            environ={
                APP_ROOT_ENV: project_root.as_posix(),
                APP_CONFIG_ENV: "app.toml",
            },
        )

        app_config = app_config_from_site(site)
        assert app_config.project_root == project_root.resolve()
        assert app_config.config_path == (project_root / "app.toml").resolve()

    @pytest.mark.anyio
    async def test_start_app_config_does_not_override_project_root(
        self,
        tmp_path: Path,
    ) -> None:
        project_root = tmp_path / "project"
        config_root = project_root / "config"
        config_root.mkdir(parents=True)
        _write_app_config(
            config_root / "app.toml",
            modules=("wybra.assets",),
        )

        site = await start(
            FastAPI(),
            environ={
                APP_ROOT_ENV: project_root.as_posix(),
                APP_CONFIG_ENV: "config/app.toml",
            },
        )

        app_config = app_config_from_site(site)
        assert app_config.project_root == project_root.resolve()
        assert app_config.config_path == (config_root / "app.toml").resolve()

    @pytest.mark.anyio
    async def test_start_relative_config_source_uses_supplied_app_root(
        self,
        tmp_path: Path,
    ) -> None:
        project_root = tmp_path / "project"
        config_root = project_root / "config"
        config_root.mkdir(parents=True)
        _write_app_config(
            config_root / "app.toml",
            modules=("wybra.assets",),
        )

        site = await start(
            FastAPI(),
            config_source="config/app.toml",
            environ={APP_ROOT_ENV: project_root.as_posix()},
        )

        app_config = app_config_from_site(site)
        assert app_config.project_root == project_root.resolve()
        assert app_config.config_path == (config_root / "app.toml").resolve()

    @pytest.mark.anyio
    async def test_start_environment_overrides_database_url_and_deployment_fallback(
        self,
        tmp_path: Path,
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        _append_explicit_file_sessions_config(
            _write_app_config(project_root / "app.toml", modules=("wybra.db",)),
            tmp_path / "sessions",
        )

        site = await start(
            FastAPI(),
            environ={
                APP_ROOT_ENV: project_root.as_posix(),
                APP_CONFIG_ENV: "app.toml",
                ENV_DATABASE_URL: "sqlite:///override.sqlite3",
                ENV_APP_ENV: "staging",
            },
        )

        try:
            app_config = app_config_from_site(site)
            assert app_config.database_url == "sqlite:///override.sqlite3"
            assert site.deployment_environment == "staging"
            assert app_config.deployment_environment == "staging"
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_start_app_env_overrides_config_deployment_environment(
        self,
        tmp_path: Path,
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        _append_explicit_file_sessions_config(
            _write_app_config(
                project_root / "app.toml",
                modules=("wybra.assets",),
                deployment_environment="production",
            ),
            tmp_path / "sessions",
        )

        site = await start(
            FastAPI(),
            environ={
                APP_ROOT_ENV: project_root.as_posix(),
                APP_CONFIG_ENV: "app.toml",
                ENV_APP_ENV: "staging",
            },
        )

        try:
            app_config = app_config_from_site(site)
            assert site.deployment_environment == "staging"
            assert app_config.deployment_environment == "staging"
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_start_app_env_overrides_mapping_config_deployment_environment(
        self,
        tmp_path: Path,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": ("wybra.assets",),
                        "deployment_environment": "production",
                    },
                    "wybra.sessions": {
                        "storage_backend": "file",
                        "file_directory": (tmp_path / "sessions").as_posix(),
                    },
                }
            ),
            environ={ENV_APP_ENV: "staging"},
        )

        try:
            app_config = app_config_from_site(site)
            assert site.deployment_environment == "staging"
            assert app_config.deployment_environment == "staging"
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_start_defaults_deployment_environment_to_local(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ("wybra.assets",)}}),
            environ={},
        )

        try:
            assert site.deployment_environment == "local"
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_start_defaults_debug_to_disabled(self) -> None:
        app = FastAPI()
        site = await start(
            app,
            config_source=MappingConfigSource({"app": {"modules": ("wybra.assets",)}}),
            environ={},
        )

        try:
            assert app.debug is False
            assert site.config.get_config("app")["debug"] is False
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_start_applies_configured_local_debug(self, tmp_path: Path) -> None:
        app = FastAPI()
        app.middleware_stack = object()
        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": ("wybra.assets",),
                        "deployment_environment": "local",
                        "debug": True,
                    }
                }
            ),
        )

        try:
            assert app.debug is True
            assert app.middleware_stack is None
            assert site.config.get_config("app")["debug"] is True
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_start_app_debug_environment_overrides_config(
        self, tmp_path: Path
    ) -> None:
        app = FastAPI()
        site = await start(
            app,
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": ("wybra.assets",),
                        "deployment_environment": "local",
                        "debug": False,
                    }
                }
            ),
            environ={ENV_APP_DEBUG: "true"},
        )

        try:
            assert app.debug is True
            assert site.config.get_config("app")["debug"] is True
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_start_rejects_invalid_debug_environment(self) -> None:
        with pytest.raises(ConfigSourceError, match="app.debug"):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {"app": {"modules": ("wybra.assets",)}}
                ),
                environ={ENV_APP_DEBUG: "maybe"},
            )

    @pytest.mark.anyio
    async def test_start_rejects_debug_outside_local(self) -> None:
        with pytest.raises(ConfigurationError, match="app.debug is only allowed"):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {
                        "app": {
                            "modules": ("wybra.assets",),
                            "deployment_environment": "production",
                            "debug": True,
                        },
                        "wybra.sessions": {
                            "storage_backend": "file",
                            "file_directory": "/tmp/wybra-debug-test-sessions",
                        },
                    }
                ),
            )

    @pytest.mark.anyio
    async def test_start_rejects_invalid_effective_deployment_environment(
        self,
        tmp_path: Path,
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        _write_app_config(project_root / "app.toml", modules=("wybra.assets",))

        with pytest.raises(ConfigurationError, match="Deployment environment"):
            await start(
                FastAPI(),
                environ={
                    APP_ROOT_ENV: project_root.as_posix(),
                    APP_CONFIG_ENV: "app.toml",
                    ENV_APP_ENV: "dev",
                },
            )

    @pytest.mark.anyio
    async def test_start_accepts_file_uri_source_string(self, tmp_path: Path) -> None:
        config_path = _write_app_config(
            tmp_path / "app.toml",
            modules=("wybra.assets",),
        )

        site = await start(FastAPI(), config_source=config_path.as_uri())

        assert site.modules == ("wybra.assets",)

    def test_local_file_uri_path_normalises_windows_drive_paths(self) -> None:
        assert (
            _local_file_uri_path("/C:/Users/David%20N/app.toml", windows=True)
            == "C:/Users/David N/app.toml"
        )

    @pytest.mark.anyio
    async def test_start_rejects_blank_config_source_string(self) -> None:
        with pytest.raises(ConfigSourceError, match="must not be blank"):
            await start(FastAPI(), config_source="   ")

    @pytest.mark.anyio
    async def test_start_rejects_unsupported_config_source_uri_scheme(self) -> None:
        with pytest.raises(
            ConfigSourceError, match="Unsupported config source URI scheme"
        ):
            await start(FastAPI(), config_source="https://example.test/app.toml")

    @pytest.mark.anyio
    async def test_start_treats_windows_absolute_source_string_as_file_path(
        self,
    ) -> None:
        with pytest.raises(ConfigSourceError, match="file: App config file"):
            await start(FastAPI(), config_source=r"C:\config\app.toml")

    @pytest.mark.anyio
    async def test_start_rejects_invalid_config_source_object(self) -> None:
        with pytest.raises(
            ConfigSourceError, match="string, AppConfig, or ConfigSource"
        ):
            await start(FastAPI(), config_source=object())  # type: ignore[arg-type]

    @pytest.mark.anyio
    async def test_start_rejects_config_source_object_with_invalid_metadata(
        self,
    ) -> None:
        class InvalidConfigSource:
            metadata = object()

            def load(self) -> ConfigSourceResult:
                return ConfigSourceResult()

        with pytest.raises(
            ConfigSourceError, match="string, AppConfig, or ConfigSource"
        ):
            await start(
                FastAPI(),
                config_source=InvalidConfigSource(),  # type: ignore[arg-type]
            )

    @pytest.mark.anyio
    async def test_site_provides_and_requires_type_keyed_capability(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        capability = ExampleImplementation()

        site.provide_capability(ExampleCapability, capability)

        assert site.has_capability(ExampleCapability) is True
        assert site.require_capability(ExampleCapability) is capability

    @pytest.mark.anyio
    async def test_site_reports_missing_required_capability(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )

        with pytest.raises(SiteCapabilityError, match="Missing capability"):
            site.require_capability(ExampleCapability)

    @pytest.mark.anyio
    async def test_site_creates_capability_proxy_before_provider_exists(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )

        proxy = site.capability_proxy(ExampleCapability)

        assert isinstance(proxy, SiteCapabilityProxy)
        assert proxy.available() is False

    @pytest.mark.anyio
    async def test_site_capability_proxy_binds_on_first_required_use(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        proxy = site.capability_proxy(ExampleCapability)
        capability = ExampleImplementation()
        site.provide_capability(ExampleCapability, capability)

        assert proxy.available() is True
        assert (await proxy.require()).label() == "implementation"
        assert await proxy.require() is capability
        assert (await proxy.require()).label() == "implementation"
        assert capability.calls == 2

    @pytest.mark.anyio
    async def test_site_capability_proxy_is_immutable_after_binding(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        capability = ExampleImplementation()
        proxy = site.capability_proxy(ExampleCapability)
        site.provide_capability(ExampleCapability, capability)

        assert (await proxy.require()).label() == "implementation"

        del site._capabilities[ExampleCapability]

        assert (await proxy.require()).label() == "implementation"
        assert await proxy.require() is capability

    @pytest.mark.anyio
    async def test_site_capability_proxy_reports_missing_required_capability(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        proxy = site.capability_proxy(ExampleCapability)

        with pytest.raises(SiteCapabilityError, match="Missing capability"):
            await proxy.require()

    @pytest.mark.anyio
    async def test_site_capability_proxy_finalises_required_capability(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        proxy = site.capability_proxy(ExampleCapability)
        capability = ExampleImplementation()
        site.provide_capability(ExampleCapability, capability)

        assert await proxy.finalise_required() is capability
        assert await proxy.require() is capability
        assert proxy.finalised is True
        assert proxy.unavailable is False

    @pytest.mark.anyio
    async def test_site_capability_proxy_reuses_required_finalisation(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        proxy = site.capability_proxy(ExampleCapability)
        capability = ExampleImplementation()
        site.provide_capability(ExampleCapability, capability)

        assert await proxy.finalise_required() is capability
        del site._capabilities[ExampleCapability]

        assert await proxy.finalise_required() is capability
        assert await proxy.finalise_optional() is capability
        assert proxy.finalised is True
        assert proxy.unavailable is False

    @pytest.mark.anyio
    async def test_site_capability_proxy_reports_missing_required_finalisation(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        proxy = site.capability_proxy(ExampleCapability)

        with pytest.raises(SiteCapabilityError, match="Missing capability"):
            await proxy.finalise_required()

    @pytest.mark.anyio
    async def test_site_capability_proxy_finalises_optional_capability(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        proxy = site.capability_proxy(ExampleCapability)
        capability = ExampleImplementation()
        site.provide_capability(ExampleCapability, capability)

        assert await proxy.finalise_optional() is capability
        assert await proxy.optional() is capability
        assert proxy.finalised is True
        assert proxy.unavailable is False

    @pytest.mark.anyio
    async def test_site_capability_proxy_records_unavailable_optional_capability(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        proxy = site.capability_proxy(ExampleCapability)

        assert await proxy.finalise_optional() is None
        assert await proxy.optional() is None
        assert proxy.finalised is True
        assert proxy.unavailable is True

        with pytest.raises(SiteCapabilityError, match="Missing capability"):
            await proxy.require()

    @pytest.mark.anyio
    async def test_site_capability_proxy_reuses_unavailable_optional_finalisation(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        proxy = site.capability_proxy(ExampleCapability)

        assert await proxy.finalise_optional() is None
        site.provide_capability(ExampleCapability, ExampleImplementation())

        assert await proxy.finalise_optional() is None
        with pytest.raises(SiteCapabilityError, match="Missing capability"):
            await proxy.finalise_required()
        assert proxy.finalised is True
        assert proxy.unavailable is True

    @pytest.mark.anyio
    async def test_site_rejects_duplicate_capability_provider(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        site.provide_capability(ExampleCapability, ExampleImplementation())

        with pytest.raises(SiteCapabilityError, match="already provided"):
            site.provide_capability(ExampleCapability, ExampleImplementation())

    @pytest.mark.anyio
    async def test_site_rejects_capability_value_with_wrong_runtime_type(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )

        with pytest.raises(SiteCapabilityError, match="invalid type"):
            site.provide_capability(ExampleCapability, OtherCapability())

    @pytest.mark.anyio
    async def test_site_rejects_capability_type_without_runtime_validation(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )

        with pytest.raises(SiteCapabilityError, match="cannot be runtime-validated"):
            site.provide_capability(UnsupportedCapability, object())

    @pytest.mark.anyio
    async def test_site_close_closes_async_capabilities(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        capability = ClosingCapability()
        site.provide_capability(ClosingCapability, capability)

        await site.close()

        assert capability.closed is True
        assert site.has_capability(ClosingCapability) is False

        await site.close()

    @pytest.mark.anyio
    async def test_site_close_preserves_registration_order_for_capabilities(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        closed: list[str] = []
        dependant = DependentClosingCapability("dependant", closed)
        provider = OrderedClosingCapability("provider", closed)
        site.provide_capability(DependentClosingCapability, dependant)
        site.provide_capability(OrderedClosingCapability, provider)

        await site.close()

        assert closed == ["dependant", "provider"]

    @pytest.mark.anyio
    async def test_site_close_closes_tasks_before_their_cache_provider(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        closed: list[str] = []
        provider = OrderedClosingCapability("cache", closed)
        tasks = ClosingTasksCapability(closed)
        site.provide_capability(OrderedClosingCapability, provider)
        site.provide_capability(TasksCapability, tasks)

        await site.close()

        assert closed == ["tasks", "cache"]

    @pytest.mark.anyio
    async def test_site_close_reports_invalid_hooks_after_closing_capabilities(
        self,
    ) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )
        closing = ClosingCapability()
        site.provide_capability(ClosingCapability, closing)
        site.provide_capability(SyncClosingCapability, SyncClosingCapability())

        with pytest.raises(SiteCapabilityError, match="error_count=1"):
            await site.close()

        assert closing.closed is True
        assert site.has_capability(ClosingCapability) is False
        assert site.has_capability(SyncClosingCapability) is False

    @pytest.mark.anyio
    async def test_start_site_returns_fastapi_lifespan_and_stores_site(self) -> None:
        app = FastAPI()
        lifespan = start_site(
            config_source=MappingConfigSource({"app": {"modules": ()}}),
        )

        async with lifespan(app):
            assert isinstance(app.state.site, Site)
            assert app.state.site.app is app
            assert get_site(app) is app.state.site

    @pytest.mark.anyio
    async def test_get_site_rejects_missing_site(self) -> None:
        with pytest.raises(SiteCapabilityError, match="attribute=site"):
            get_site(FastAPI())

    @pytest.mark.anyio
    async def test_start_accepts_loaded_app_config(self, tmp_path: Path) -> None:
        app_config = AppConfig(
            config_path=tmp_path / "app.toml",
            project_root=tmp_path,
            modules=("wybra.assets",),
            routes=RouteOptions(prefixes={}),
            templates=TemplateOptions(auto_reload=True, cache_size=0),
            assets=AssetOptions(url_path="/static/"),
        )

        site = await start(FastAPI(), config_source=app_config)

        assert site.modules == ("wybra.assets",)
        assert site.has_module("wybra.assets") is True

    @pytest.mark.anyio
    async def test_start_accepts_config_source_object(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {"app": {"modules": ("wybra.assets",)}},
                source="test",
            ),
        )

        assert site.modules == ("wybra.assets",)

    @pytest.mark.anyio
    async def test_start_reports_missing_required_config_file(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(ConfigSourceError, match="file: App config file"):
            await start(FastAPI(), config_source=str(tmp_path / "missing.toml"))


def _write_module(root: Path, name: str, body: str) -> None:
    module_path = root / f"{name}.py"
    module_path.write_text(body, encoding="utf-8")


class TestModuleLifecycle:
    @pytest.mark.anyio
    async def test_start_invokes_setup_site_hooks_in_configured_module_order(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(tmp_path, "site_setup_recorder", "calls = []\n")
        _write_module(
            tmp_path,
            "first_module",
            "from site_setup_recorder import calls\n"
            'async def setup_site(site):\n    calls.append("first")\n',
        )
        _write_module(
            tmp_path,
            "second_module",
            "from site_setup_recorder import calls\n"
            'async def setup_site(site):\n    calls.append("second")\n',
        )

        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {"app": {"modules": ("first_module", "second_module")}}
            ),
        )

        from site_setup_recorder import calls

        assert isinstance(site, Site)
        assert calls == ["first", "second"]

    @pytest.mark.anyio
    async def test_start_invokes_post_setup_site_hooks_after_all_setup_hooks(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(tmp_path, "post_setup_recorder", "calls = []\n")
        _write_module(
            tmp_path,
            "first_post_module",
            "from post_setup_recorder import calls\n"
            'async def setup_site(site):\n    calls.append("first.setup")\n'
            'async def post_setup_site(site):\n    calls.append("first.post")\n',
        )
        _write_module(
            tmp_path,
            "second_post_module",
            "from post_setup_recorder import calls\n"
            'async def setup_site(site):\n    calls.append("second.setup")\n'
            'async def post_setup_site(site):\n    calls.append("second.post")\n',
        )

        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {"app": {"modules": ("first_post_module", "second_post_module")}}
            ),
        )

        from post_setup_recorder import calls

        assert calls == [
            "first.setup",
            "second.setup",
            "first.post",
            "second.post",
        ]

    @pytest.mark.anyio
    async def test_start_finalises_deferred_setup_after_all_module_setup(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(tmp_path, "deferred_setup_recorder", "calls = []\n")
        _write_module(
            tmp_path,
            "deferred_first_module",
            "from deferred_setup_recorder import calls\n"
            "async def setup_site(site):\n"
            '    calls.append("first.setup")\n'
            "    async def finalise():\n"
            '        calls.append("first.finalise")\n'
            "    site.defer_setup_finalisation(finalise)\n"
            "async def post_setup_site(site):\n"
            '    calls.append("first.post")\n',
        )
        _write_module(
            tmp_path,
            "deferred_second_module",
            "from deferred_setup_recorder import calls\n"
            "async def setup_site(site):\n"
            '    calls.append("second.setup")\n'
            "async def post_setup_site(site):\n"
            '    calls.append("second.post")\n',
        )

        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {
                        "modules": (
                            "deferred_first_module",
                            "deferred_second_module",
                        )
                    }
                }
            ),
        )

        from deferred_setup_recorder import calls

        assert calls == [
            "first.setup",
            "second.setup",
            "first.finalise",
            "first.post",
            "second.post",
        ]

    @pytest.mark.anyio
    async def test_start_accepts_async_callable_deferred_setup_finaliser(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(tmp_path, "async_callable_finaliser_recorder", "calls = []\n")
        _write_module(
            tmp_path,
            "async_callable_finaliser_module",
            "from async_callable_finaliser_recorder import calls\n"
            "class Finaliser:\n"
            "    async def __call__(self):\n"
            '        calls.append("finalise")\n'
            "async def setup_site(site):\n"
            '    calls.append("setup")\n'
            "    site.defer_setup_finalisation(Finaliser())\n",
        )

        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {"app": {"modules": ("async_callable_finaliser_module",)}}
            ),
        )

        from async_callable_finaliser_recorder import calls

        assert calls == ["setup", "finalise"]

    @pytest.mark.anyio
    async def test_start_attributes_deferred_setup_finalisation_failure(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(
            tmp_path,
            "failing_deferred_module",
            "async def setup_site(site):\n"
            "    async def finalise():\n"
            "        raise RuntimeError('boom')\n"
            "    site.defer_setup_finalisation(finalise)\n",
        )

        with pytest.raises(
            SiteCapabilityError,
            match=(
                "module=failing_deferred_module.*attribute=setup_finalisation.*"
                "error_type=RuntimeError"
            ),
        ):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {"app": {"modules": ("failing_deferred_module",)}}
                ),
            )

    @pytest.mark.anyio
    async def test_errors_module_registers_error_handling_capability(self) -> None:
        app = FastAPI()

        site = await start(
            app,
            config_source=MappingConfigSource({"app": {"modules": ("wybra.errors",)}}),
        )

        assert site.has_capability(ErrorHandlingCapability)
        assert StarletteHTTPException in app.exception_handlers
        assert EmptyBodyResponseException in app.exception_handlers

    @pytest.mark.anyio
    async def test_errors_module_uses_configured_module_error_mappings(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(
            tmp_path,
            "mapped_error_app",
            "from jinja2.exceptions import TemplateNotFound\n"
            "from wybra.core.exceptions import Http404\n"
            "from wybra.errors.mappings import ErrorMapping\n"
            "error_mappings = (\n"
            "    ErrorMapping(TemplateNotFound, Http404, 'Missing page.'),\n"
            ")\n",
        )
        app = FastAPI()

        await start(
            app,
            config_source=MappingConfigSource(
                {"app": {"modules": ("mapped_error_app", "wybra.errors")}}
            ),
        )

        from jinja2.exceptions import TemplateNotFound

        @app.get("/missing")
        async def missing() -> None:
            raise TemplateNotFound("missing.html")

        with WybraTestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/missing")

        assert response.status_code == Http404.default_status_code

    @pytest.mark.anyio
    async def test_start_ignores_modules_without_post_setup_site(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(tmp_path, "post_setup_missing_recorder", "calls = []\n")
        _write_module(
            tmp_path,
            "plain_module",
            "from post_setup_missing_recorder import calls\n"
            'async def setup_site(site):\n    calls.append("setup")\n',
        )

        await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ("plain_module",)}}),
        )

        from post_setup_missing_recorder import calls

        assert calls == ["setup"]

    @pytest.mark.anyio
    async def test_post_setup_site_can_finalise_late_capability_provider(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(
            tmp_path,
            "late_dependency_types",
            'class LateCapability:\n    def label(self):\n        return "late"\n',
        )
        _write_module(
            tmp_path,
            "dependent_module",
            "from late_dependency_types import LateCapability\n"
            "proxy = None\n"
            "async def setup_site(site):\n"
            "    global proxy\n"
            "    proxy = site.capability_proxy(LateCapability)\n"
            "async def post_setup_site(site):\n"
            "    await proxy.finalise_required()\n",
        )
        _write_module(
            tmp_path,
            "provider_module",
            "from late_dependency_types import LateCapability\n"
            "async def setup_site(site):\n"
            "    site.provide_capability(LateCapability, LateCapability())\n",
        )

        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {"app": {"modules": ("dependent_module", "provider_module")}}
            ),
        )

        try:
            capability_type = __import__("late_dependency_types").LateCapability

            assert site.require_capability(capability_type).label() == "late"
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_start_ignores_modules_without_setup_site(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(tmp_path, "plain_module", "")

        site = await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ("plain_module",)}}),
        )

        try:
            assert site.modules == ("plain_module",)
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_start_rejects_non_callable_setup_site(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(tmp_path, "invalid_module", "setup_site = object()\n")

        with pytest.raises(
            SiteCapabilityError,
            match="module=invalid_module.*attribute_type=object",
        ):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {"app": {"modules": ("invalid_module",)}}
                ),
            )

    @pytest.mark.anyio
    async def test_start_rejects_sync_setup_site(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(
            tmp_path,
            "sync_module",
            'def setup_site(site):\n    raise RuntimeError("should not be called")\n',
        )

        with pytest.raises(
            SiteCapabilityError,
            match="module=sync_module.*expected=async_callable",
        ):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {"app": {"modules": ("sync_module",)}}
                ),
            )

    @pytest.mark.anyio
    async def test_start_reports_setup_site_failure(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(
            tmp_path,
            "failing_module",
            'async def setup_site(site):\n    raise RuntimeError("boom")\n',
        )

        with pytest.raises(
            SiteCapabilityError,
            match="module=failing_module.*error_type=RuntimeError",
        ):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {"app": {"modules": ("failing_module",)}}
                ),
            )

    @pytest.mark.anyio
    async def test_start_rejects_non_callable_post_setup_site(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(tmp_path, "invalid_post_module", "post_setup_site = object()\n")

        with pytest.raises(
            SiteCapabilityError,
            match="module=invalid_post_module.*attribute=post_setup_site",
        ):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {"app": {"modules": ("invalid_post_module",)}}
                ),
            )

    @pytest.mark.anyio
    async def test_start_rejects_sync_post_setup_site(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(
            tmp_path,
            "sync_post_module",
            (
                "def post_setup_site(site):\n"
                '    raise RuntimeError("should not be called")\n'
            ),
        )

        with pytest.raises(
            SiteCapabilityError,
            match="module=sync_post_module.*expected=async_callable",
        ):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {"app": {"modules": ("sync_post_module",)}}
                ),
            )

    @pytest.mark.anyio
    async def test_start_reports_post_setup_site_failure(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.syspath_prepend(str(tmp_path))
        _write_module(
            tmp_path,
            "failing_post_module",
            'async def post_setup_site(site):\n    raise RuntimeError("boom")\n',
        )

        with pytest.raises(
            SiteCapabilityError,
            match="module=failing_post_module.*attribute=post_setup_site.*RuntimeError",
        ):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {"app": {"modules": ("failing_post_module",)}}
                ),
            )
