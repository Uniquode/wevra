from __future__ import annotations

import logging
import os
import re
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from importlib import import_module
from inspect import iscoroutinefunction
from pathlib import Path
from types import ModuleType
from typing import NoReturn, Protocol, TypeGuard, TypeVar, cast
from urllib.parse import unquote, urlparse

from fastapi import FastAPI

from wybra.config import (
    AppConfigSource,
    ConfigDef,
    ConfigService,
    ConfigSource,
    ConfigSourceError,
    ConfigSourceMetadata,
    discover_module_config_defs,
)
from wybra.core.composition import (
    APP_CONFIG_ENV,
    APP_ROOT_ENV,
    AppConfig,
    CompositionError,
    load_app_config,
    resolve_project_root,
)
from wybra.core.config import RUNTIME_CONFIG_DEF
from wybra.core.environment import load_environment
from wybra.core.exceptions import ConfigurationError
from wybra.core.logging import LoggingConfigurationError, configure_runtime_logging
from wybra.core.modules import CORE_MODULES
from wybra.core.runtime import (
    DEFAULT_DEPLOYMENT_ENVIRONMENT,
    DeploymentEnvironment,
    normalise_deployment_environment,
)
from wybra.core.settings import load_composition_config_from_environment
from wybra.errors.diagnostics import structured_error, type_name
from wybra.events import observe
from wybra.events.site import (
    capability_provided_event,
    capability_resolution_event,
    module_hook_event,
    site_lifecycle_event,
)
from wybra.tools.project import runtime_project_root

logger = logging.getLogger(__name__)

ConfigSourceInput = str | AppConfig | ConfigSource | None
ModuleLoader = Callable[[str], ModuleType]
AppT = TypeVar("AppT", bound=FastAPI)
SETUP_SITE_ATTRIBUTE = "setup_site"
POST_SETUP_SITE_ATTRIBUTE = "post_setup_site"
SETUP_FINALISATION_ATTRIBUTE = "setup_finalisation"
T = TypeVar("T")


class _CapabilityProxyState(Enum):
    UNRESOLVED = auto()
    AVAILABLE = auto()
    UNAVAILABLE = auto()


class SiteLifespan(Protocol[AppT]):
    def __call__(self, app: AppT) -> AbstractAsyncContextManager[None]: ...


type SiteSetupFinaliser = Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class _DeferredSetupFinaliser:
    module_name: str
    finaliser: SiteSetupFinaliser


class SiteCapabilityError(RuntimeError):
    """Raised when a site capability cannot be registered or resolved."""


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Site:
    app: FastAPI
    config: ConfigService
    deployment_environment: DeploymentEnvironment = DEFAULT_DEPLOYMENT_ENVIRONMENT
    _capabilities: dict[type[object], object] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _pending_capability_events: list[type[object]] = field(
        default_factory=list,
        init=False,
        repr=False,
    )
    _setup_finalisers: list[_DeferredSetupFinaliser] = field(
        default_factory=list,
        init=False,
        repr=False,
    )
    _setup_finaliser_module: str | None = field(default=None, init=False, repr=False)

    @property
    def modules(self) -> tuple[str, ...]:
        app_config = self.config.get_config("app") or {}
        modules = app_config.get("modules", ())
        if isinstance(modules, list | tuple) and all(
            isinstance(module, str) for module in modules
        ):
            return tuple(modules)
        return ()

    def has_module(self, owner: str) -> bool:
        return owner in self.modules

    def provide_capability(self, capability_type: type[T], value: T) -> None:
        """Register a capability under a concrete runtime-checkable type."""
        if capability_type in self._capabilities:
            raise SiteCapabilityError(
                structured_error(
                    "Capability is already provided",
                    capability_type=type_name(capability_type),
                )
            )
        if not _matches_capability_type(value, capability_type):
            raise SiteCapabilityError(
                structured_error(
                    "Capability value has invalid type",
                    capability_type=type_name(capability_type),
                    value_type=type(value).__name__,
                )
            )
        self._capabilities[capability_type] = value
        self._pending_capability_events.append(capability_type)

    def defer_setup_finalisation(self, finaliser: SiteSetupFinaliser) -> None:
        """Run an asynchronous finaliser after every module setup hook."""
        if not callable(finaliser) or not iscoroutinefunction(finaliser):
            raise SiteCapabilityError(
                structured_error(
                    "Deferred setup finaliser is invalid",
                    finaliser_type=type(finaliser).__name__,
                    expected="async_callable",
                )
            )
        module_name = self._setup_finaliser_module
        if module_name is None:
            raise SiteCapabilityError(
                "Deferred setup finalisers may only be registered from setup_site."
            )
        self._setup_finalisers.append(_DeferredSetupFinaliser(module_name, finaliser))

    async def _finalise_deferred_setup(self) -> None:
        while self._setup_finalisers:
            deferred = self._setup_finalisers.pop(0)

            async def run_finaliser(
                _site: Site,
                finaliser: SiteSetupFinaliser = deferred.finaliser,
            ) -> None:
                await finaliser()

            await _run_module_hook(
                self,
                module_name=deferred.module_name,
                attribute=SETUP_FINALISATION_ATTRIBUTE,
                hook=run_finaliser,
            )
        await self._publish_pending_capability_events()

    @contextmanager
    def _registering_setup_finalisers(self, module_name: str) -> Iterator[None]:
        previous_module = self._setup_finaliser_module
        object.__setattr__(self, "_setup_finaliser_module", module_name)
        try:
            yield
        finally:
            object.__setattr__(self, "_setup_finaliser_module", previous_module)

    async def _publish_pending_capability_events(self) -> None:
        """Publish capability registration observations at async boundaries."""

        if not self._pending_capability_events:
            return

        pending = tuple(self._pending_capability_events)
        self._pending_capability_events.clear()
        for capability_type in pending:
            await _record_capability_provided(type_name(capability_type))

    def require_capability(self, capability_type: type[T]) -> T:
        try:
            capability = self._capabilities[capability_type]
        except KeyError as exc:
            raise SiteCapabilityError(
                structured_error(
                    "Missing capability",
                    capability_type=type_name(capability_type),
                )
            ) from exc
        return cast(T, capability)

    def optional_capability(self, capability_type: type[T]) -> T | None:
        capability = self._capabilities.get(capability_type)
        return cast(T | None, capability)

    def has_capability(self, capability_type: type[object]) -> bool:
        return capability_type in self._capabilities

    def capability_proxy(self, capability_type: type[T]) -> SiteCapabilityProxy[T]:
        """Return a lazy proxy for a capability type."""
        return SiteCapabilityProxy(self, capability_type)

    async def close(self) -> None:
        """Close capabilities that expose an async ``close()`` hook.

        Capability cleanup hooks must be async. Synchronous close hooks are
        invalid.
        """
        if not self._capabilities:
            return

        from wybra.events import EventsCapability

        error_count = 0
        events = self.optional_capability(EventsCapability)
        for capability in tuple(self._capabilities.values()):
            if capability is events:
                continue
            close = getattr(capability, "close", None)
            if close is None:
                continue
            if not callable(close) or not iscoroutinefunction(close):
                error_count += 1
                logger.error(
                    "Capability close hook must be async",
                    extra={
                        "capability": type(capability).__name__,
                        "attribute": "close",
                        "attribute_type": type(close).__name__,
                    },
                )
                continue
            try:
                await close()
            except BaseException as exc:
                error_count += 1
                logger.exception(
                    "Capability close hook failed",
                    extra={
                        "capability": type(capability).__name__,
                        "attribute": "close",
                        "error_type": type(exc).__name__,
                    },
                )
                if not isinstance(exc, Exception):
                    raise

        await _publish_site_lifecycle(phase="shutdown", error_count=error_count)
        if events is not None:
            close_events = getattr(events, "close", None)
            if callable(close_events) and iscoroutinefunction(close_events):
                await close_events()
        self._capabilities.clear()
        self._pending_capability_events.clear()

        if error_count:
            raise SiteCapabilityError(
                structured_error(
                    "Capability close failed",
                    error_count=error_count,
                )
            )


@dataclass(slots=True)
class SiteCapabilityProxy[T]:
    """Lazily resolve a capability from the site and cache the first result.

    The first successful ``require()`` result is cached and reused for the
    lifetime of the proxy. Proxies are therefore immutable once bound; changes to
    registered capabilities at runtime are intentionally not reflected in
    existing proxies.
    """

    site: Site
    capability_type: type[T]
    _capability: T | None = field(default=None, init=False, repr=False)
    _state: _CapabilityProxyState = field(
        default=_CapabilityProxyState.UNRESOLVED,
        init=False,
        repr=False,
    )

    @property
    def finalised(self) -> bool:
        return self._state is not _CapabilityProxyState.UNRESOLVED

    @property
    def unavailable(self) -> bool:
        return self._state is _CapabilityProxyState.UNAVAILABLE

    def available(self) -> bool:
        if self._state is _CapabilityProxyState.AVAILABLE:
            return True
        if self._state is _CapabilityProxyState.UNAVAILABLE:
            return False
        return self.site.has_capability(self.capability_type)

    async def require(self) -> T:
        if self._state is _CapabilityProxyState.AVAILABLE:
            return self._require_cached_capability()
        if self._state is _CapabilityProxyState.UNAVAILABLE:
            self._raise_missing_capability()

        try:
            capability = self.site.require_capability(self.capability_type)
        except SiteCapabilityError:
            await self._record_resolution(available=False)
            raise
        self._set_available(capability)
        await self._record_resolution(available=True)
        return capability

    async def optional(self) -> T | None:
        if self._state is _CapabilityProxyState.AVAILABLE:
            return self._require_cached_capability()
        if self._state is _CapabilityProxyState.UNAVAILABLE:
            return None

        capability = self.site.optional_capability(self.capability_type)
        if capability is None:
            await self._record_resolution(available=False)
            return None

        self._set_available(capability)
        await self._record_resolution(available=True)
        return capability

    async def finalise_required(self) -> T:
        return await self.require()

    async def finalise_optional(self) -> T | None:
        capability = await self.optional()
        if capability is None:
            self._set_unavailable()
        return capability

    def _set_available(self, capability: T) -> None:
        self._capability = capability
        self._state = _CapabilityProxyState.AVAILABLE

    def _set_unavailable(self) -> None:
        self._capability = None
        self._state = _CapabilityProxyState.UNAVAILABLE

    async def _record_resolution(self, *, available: bool) -> None:
        await _record_capability_resolution(
            type_name(self.capability_type), available=available
        )

    def _require_cached_capability(self) -> T:
        if self._capability is None:
            self._raise_missing_capability()
        return self._capability

    def _raise_missing_capability(self) -> NoReturn:
        raise SiteCapabilityError(
            structured_error(
                "Missing capability",
                capability_type=type_name(self.capability_type),
            )
        )


@dataclass(frozen=True, slots=True)
class _StartupConfig:
    source: ConfigSource
    environ: object
    app_config: AppConfig | None


def start_site(
    *,
    config_source: ConfigSourceInput = None,
    module_loader: ModuleLoader | None = None,
    environ: Mapping[str, str] | None = None,
) -> SiteLifespan[FastAPI]:
    @asynccontextmanager
    async def _start_site(app: FastAPI):
        app.middleware_stack = None
        site = await start(
            app,
            config_source=config_source,
            module_loader=module_loader,
            environ=environ,
        )
        app.state.site = site
        try:
            yield
        finally:
            await site.close()

    return _start_site


def get_site(app: FastAPI) -> Site:
    try:
        site = getattr(app.state, "site", None)
    except AttributeError as exc:
        raise SiteCapabilityError(
            structured_error(
                "Site is not available on app state",
                attribute="state",
            )
        ) from exc
    if not isinstance(site, Site):
        raise SiteCapabilityError(
            structured_error(
                "Site is not available on app state",
                attribute="site",
            )
        )
    return site


@observe(site_lifecycle_event, context="lifecycle")
async def _publish_site_lifecycle(
    *,
    phase: str,
    error_count: int = 0,
) -> None:
    """Publish a non-controlling site lifecycle observation."""

    del phase, error_count


@observe(capability_provided_event, context="capability")
async def _record_capability_provided(capability_type: str) -> None:
    """Record a provided site capability without exposing dispatch mechanics."""

    del capability_type


@observe(capability_resolution_event, context="capability")
async def _record_capability_resolution(
    capability_type: str,
    *,
    available: bool,
) -> None:
    """Record a lazy capability resolution without exposing dispatch mechanics."""

    del capability_type, available


@observe(module_hook_event, context="module")
async def _invoke_module_hook(
    site: Site,
    *,
    module_name: str,
    attribute: str,
    hook: Callable[[Site], Awaitable[None]],
) -> None:
    """Run one configured hook while observing its lifecycle."""

    del module_name, attribute
    await hook(site)


async def start(
    app: FastAPI,
    *,
    config_source: ConfigSourceInput = None,
    module_loader: ModuleLoader | None = None,
    environ: Mapping[str, str] | None = None,
) -> Site:
    startup_config = _startup_config(config_source, environ)
    try:
        configure_runtime_logging(startup_config.app_config)
    except LoggingConfigurationError as exc:
        raise ConfigSourceError(str(exc)) from exc
    ConfigService.set_runtime_environment(startup_config.environ)
    config = ConfigService(
        [startup_config.source],
        config_defs=_core_config_defs(),
    )
    deployment_environment = _deployment_environment(config)
    _apply_runtime_debug(app, config, deployment_environment)
    # Startup hooks register middleware, so clear any stale stack once first.
    _reset_middleware_stack(app)
    site = Site(
        app=app,
        config=config,
        deployment_environment=deployment_environment,
    )
    app.state.site = site
    try:
        _setup_core_events(site)
        await site._publish_pending_capability_events()
        await _compose_site(site, module_loader or import_module)
        await _setup_core_diagnostics(site)
        await site._publish_pending_capability_events()
        _register_event_lifecycle_middleware(site)
        await _publish_site_lifecycle(phase="startup")
    except Exception:
        await site.close()
        raise
    return site


def _core_config_defs() -> tuple[ConfigDef, ...]:
    return (RUNTIME_CONFIG_DEF, *discover_module_config_defs(CORE_MODULES))


def _startup_config(
    config_source: ConfigSourceInput,
    environ: Mapping[str, str] | None,
) -> _StartupConfig:
    startup_environ = _startup_environ(config_source, environ)
    source, app_config = _normalise_config_source(config_source, environ)
    return _StartupConfig(
        source=source,
        environ=startup_environ,
        app_config=app_config,
    )


def _startup_environ(
    config_source: ConfigSourceInput,
    environ: Mapping[str, str] | None,
) -> object:
    if environ is not None:
        project_root = _config_source_project_root(config_source, environ)
        return load_environment(environ=environ, project_root=project_root)
    project_root = _config_source_project_root(config_source, environ)
    return load_environment(project_root=project_root)


def _normalise_config_source(
    config_source: ConfigSourceInput,
    environ: Mapping[str, str] | None,
) -> tuple[ConfigSource, AppConfig | None]:
    if config_source is None:
        project_root = _startup_project_root(environ)
        env = load_environment(environ=environ, project_root=project_root)
        app_config = load_composition_config_from_environment(
            env,
            project_root=project_root,
            app_config_env=APP_CONFIG_ENV,
            require_app_config=True,
        )
        if app_config is None:  # pragma: no cover - require_app_config prevents this
            raise ConfigSourceError(
                "Application config file could not be resolved; pass --config or set "
                f"{APP_CONFIG_ENV}."
            )
        return AppConfigSource(app_config), app_config
    if isinstance(config_source, AppConfig):
        return AppConfigSource(config_source), config_source
    if isinstance(config_source, str):
        app_config = _load_file_config_source(
            _file_config_path(config_source),
            project_root=_explicit_config_project_root(environ),
        )
        return AppConfigSource(app_config, source="file"), app_config
    if _is_config_source(config_source):
        return config_source, None
    raise ConfigSourceError(
        "Config source must be a string, AppConfig, or ConfigSource."
    )


def _load_file_config_source(config_path: Path, *, project_root: Path) -> AppConfig:
    try:
        return load_app_config(project_root=project_root, config_path=config_path)
    except CompositionError as exc:
        raise ConfigSourceError(f"file: {exc}") from exc


def _deployment_environment(
    config: ConfigService,
) -> DeploymentEnvironment:
    app_values = config.get_config("app") or {}
    config_value = app_values.get("deployment_environment")
    if config_value is not None:
        return _normalise_configured_deployment_environment(
            config_value,
            "app.deployment_environment",
        )
    return DEFAULT_DEPLOYMENT_ENVIRONMENT


def _runtime_debug(config: ConfigService) -> bool:
    app_values = config.get_config("app") or {}
    value = app_values.get("debug", False)
    if not isinstance(value, bool):
        raise ConfigSourceError("app.debug must be a boolean value.")
    return value


def _apply_runtime_debug(
    app: FastAPI,
    config: ConfigService,
    deployment_environment: DeploymentEnvironment,
) -> None:
    debug = _runtime_debug(config)
    if debug and deployment_environment != "local":
        raise ConfigurationError(
            "app.debug is only allowed when deployment_environment is local."
        )
    app.debug = debug


def _reset_middleware_stack(app: FastAPI) -> None:
    app.middleware_stack = None


def _normalise_configured_deployment_environment(
    value: object,
    name: str,
) -> DeploymentEnvironment:
    if not isinstance(value, str) or not value.strip():
        raise ConfigSourceError(f"{name} must be a non-blank string.")
    return normalise_deployment_environment(value.strip())


def _config_source_project_root(
    config_source: ConfigSourceInput,
    environ: Mapping[str, str] | None,
) -> Path | None:
    if isinstance(config_source, AppConfig):
        return config_source.project_root
    if config_source is None:
        return _startup_project_root(environ)
    return None


def _startup_project_root(environ: Mapping[str, str] | None) -> Path:
    """Resolve the root for default startup discovery.

    Default startup may use the process environment because this is the path
    used by imported ASGI apps after `wybra-runserver` has populated startup
    overrides.
    """
    if environ is not None and APP_ROOT_ENV in environ:
        return resolve_project_root(environ=environ)
    return runtime_project_root()


def _explicit_config_project_root(environ: Mapping[str, str] | None) -> Path:
    """Resolve the root for an explicit config source string.

    Explicit config paths are caller input, so process APP_ROOT must not affect
    them unless the caller also supplied an explicit environment mapping.
    """
    if environ is not None and APP_ROOT_ENV in environ:
        return resolve_project_root(environ=environ)
    return Path.cwd().resolve()


def _file_config_path(config_source: str) -> Path:
    value = config_source.strip()
    if not value:
        raise ConfigSourceError("Config source string must not be blank.")

    if _is_windows_absolute_path(value):
        return Path(value)

    parsed = urlparse(value)
    if parsed.scheme and parsed.scheme != "file":
        raise ConfigSourceError(
            f"Unsupported config source URI scheme: {parsed.scheme}."
        )
    if parsed.scheme == "file":
        if parsed.netloc not in {"", "localhost"}:
            raise ConfigSourceError(
                "file:// config source URI must refer to a local file."
            )
        if not parsed.path:
            raise ConfigSourceError("file:// config source URI must include a path.")
        return Path(_local_file_uri_path(parsed.path))

    return Path(value)


def _is_config_source(value: object) -> TypeGuard[ConfigSource]:
    return callable(getattr(value, "load", None)) and isinstance(
        getattr(value, "metadata", None),
        ConfigSourceMetadata,
    )


def _is_windows_absolute_path(value: str) -> bool:
    return re.match(r"^[A-Za-z]:(?:\\|/)", value) is not None


def _local_file_uri_path(path: str, *, windows: bool = os.name == "nt") -> str:
    decoded = unquote(path)
    if windows and re.match(r"^/[A-Za-z]:(?:/|\\)", decoded):
        return decoded[1:]
    return decoded


def _matches_capability_type(value: object, capability_type: type[object]) -> bool:
    try:
        return isinstance(value, capability_type)
    except TypeError as exc:
        raise SiteCapabilityError(
            structured_error(
                "Capability type cannot be runtime-validated",
                capability_type=type_name(capability_type),
                expected="runtime_checkable_type",
            )
        ) from exc


def _require_async_hook(
    *,
    module_name: str,
    attribute: str,
    hook: object,
) -> None:
    if not callable(hook) or not iscoroutinefunction(hook):
        raise SiteCapabilityError(
            structured_error(
                "Configured module hook is invalid",
                module=module_name,
                attribute=attribute,
                attribute_type=type(hook).__name__,
                expected="async_callable",
            )
        )


async def _compose_site(site: Site, module_loader: ModuleLoader) -> None:
    """Run site composition phases in dependency-safe order.

    Module setup hooks run first so modules can provide capabilities and route
    modules are importable. Core route registration then installs the final
    configured route tree. Route-level dependency validation runs before
    post-setup hooks so modules can finalise dependencies against the composed
    application.
    """

    loaded_modules = await _setup_module_hooks(site, module_loader)
    await site._finalise_deferred_setup()
    await _setup_core_sessions(site)
    _register_configured_routes(site)
    _validate_registered_route_dependencies(site)
    await _post_setup_module_hooks(site, loaded_modules)


async def _setup_core_diagnostics(site: Site) -> None:
    from wybra.diagnostics.setup import setup_core_diagnostics

    await setup_core_diagnostics(site)


def _setup_core_events(site: Site) -> None:
    from wybra.events._core import setup_core_events

    setup_core_events(site)


def _register_event_lifecycle_middleware(site: Site) -> None:
    from wybra.events_middleware import register_event_lifecycle_middleware

    register_event_lifecycle_middleware(site)


async def _setup_core_sessions(site: Site) -> None:
    from wybra.sessions.setup import setup_core_sessions

    await setup_core_sessions(site)


async def _setup_module_hooks(
    site: Site,
    module_loader: ModuleLoader,
) -> tuple[tuple[str, ModuleType], ...]:
    loaded_modules: list[tuple[str, ModuleType]] = []
    for module_name in site.modules:
        module = module_loader(module_name)
        loaded_modules.append((module_name, module))
        setup_site = getattr(module, SETUP_SITE_ATTRIBUTE, None)
        if setup_site is None:
            continue
        await _run_module_hook(
            site,
            module_name=module_name,
            attribute=SETUP_SITE_ATTRIBUTE,
            hook=setup_site,
        )
    return tuple(loaded_modules)


async def _post_setup_module_hooks(
    site: Site,
    loaded_modules: tuple[tuple[str, ModuleType], ...],
) -> None:
    for module_name, module in loaded_modules:
        post_setup_site = getattr(module, POST_SETUP_SITE_ATTRIBUTE, None)
        if post_setup_site is None:
            continue
        await _run_module_hook(
            site,
            module_name=module_name,
            attribute=POST_SETUP_SITE_ATTRIBUTE,
            hook=post_setup_site,
        )


def _register_configured_routes(site: Site) -> None:
    from wybra.core.routes import register_configured_routes_for_site

    register_configured_routes_for_site(site)


def _validate_registered_route_dependencies(site: Site) -> None:
    from wybra.api import ApiCapability
    from wybra.core.routes import RouteType, inspect_route_tree
    from wybra.template import TemplateCapability

    route_inspection = inspect_route_tree(site.app)
    if any(route.shape.template is not None for route in route_inspection.routes):
        site.require_capability(TemplateCapability)
    if any(
        route.shape.declared_route_type is RouteType.API
        for route in route_inspection.routes
    ):
        site.require_capability(ApiCapability)


async def _run_module_hook(
    site: Site,
    *,
    module_name: str,
    attribute: str,
    hook: object,
) -> None:
    _require_async_hook(module_name=module_name, attribute=attribute, hook=hook)
    async_hook = cast(Callable[[Site], Awaitable[None]], hook)
    try:
        if attribute == SETUP_SITE_ATTRIBUTE:
            with site._registering_setup_finalisers(module_name):
                await _invoke_module_hook(
                    site,
                    module_name=module_name,
                    attribute=attribute,
                    hook=async_hook,
                )
        else:
            await _invoke_module_hook(
                site,
                module_name=module_name,
                attribute=attribute,
                hook=async_hook,
            )
    except BaseException as exc:
        if not isinstance(exc, Exception):
            raise
        if isinstance(exc, SiteCapabilityError):
            raise
        raise SiteCapabilityError(
            structured_error(
                "Configured module hook failed",
                module=module_name,
                attribute=attribute,
                error_type=type(exc).__name__,
            )
        ) from exc
    else:
        await site._publish_pending_capability_events()
