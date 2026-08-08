import json
import sys
import tomllib
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import APIRouter, FastAPI, Form
from fastapi.responses import HTMLResponse
from starlette.routing import Route
from starlette.staticfiles import StaticFiles

import wybra.tools.routes as routes_tool
from wybra.core import InputValidationError
from wybra.core.routes import (
    ROUTE_METHODS_ATTRIBUTE,
    ROUTE_PATH_ATTRIBUTE,
    ROUTE_TEMPLATE_ATTRIBUTE,
    ROUTE_TYPE_ATTRIBUTE,
    ConfiguredModuleRouter,
    RouteKind,
    RouteOrigin,
    RouteProblemKind,
    RouteType,
    inspect_route_tree,
    record_route_origin,
    register_module_routes,
    route,
    route_template,
    route_type,
)
from wybra.tools.project import ProjectToolConfigurationError


def test_wybra_package_command_scripts_are_prefixed() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"

    with pyproject.open("rb") as handle:
        data = tomllib.load(handle)

    assert data["project"]["scripts"] == {
        "wybra-authmgr": "wybra.auth.cli.authmgr:main",
        "wybra-collect": "wybra.tools.collect:main",
        "wybra-debug": "wybra.tools.debug:main",
        "wybra-migrate": "wybra.tools.migrate:main",
        "wybra-routes": "wybra.tools.routes:main",
        "wybra-runserver": "wybra.tools.runserver:main",
        "wybra-secret": "wybra.secrets.cli:main",
        "wybra-validate": "wybra.tools.validate:main",
        "wybra-task-worker": "wybra.tasks.worker:main",
    }


def _app() -> FastAPI:
    return FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


class _AppWithRoutes:
    def __init__(self, routes: object) -> None:
        self.routes = routes


class _BrokenRouteIterable:
    def __iter__(self):
        raise TypeError("route iterator failed")


@pytest.mark.parametrize("template_name", ("", " "))
def test_route_template_rejects_invalid_template_name(template_name: str) -> None:
    with pytest.raises(InputValidationError, match="Route template name"):
        route_template(template_name)


def test_route_metadata_can_be_declared_on_view_class() -> None:
    @route("/home", RouteType.PAGE, template="pages/home.html", methods=("get",))
    class HomePage:
        pass

    assert getattr(HomePage, ROUTE_PATH_ATTRIBUTE) == "/home"
    assert getattr(HomePage, ROUTE_TYPE_ATTRIBUTE) == RouteType.PAGE.value
    assert getattr(HomePage, ROUTE_TEMPLATE_ATTRIBUTE) == "pages/home.html"
    assert getattr(HomePage, ROUTE_METHODS_ATTRIBUTE) == ("GET",)


@pytest.mark.parametrize("path", ("", " ", "   "))
def test_route_rejects_blank_or_whitespace_path(path: str) -> None:
    with pytest.raises(InputValidationError, match="Route path"):

        @route(path, RouteType.PAGE)
        class View:
            pass


@pytest.mark.parametrize("path", ("status", "status/health", "api/status", "v1/home"))
def test_route_rejects_paths_not_starting_with_slash(path: str) -> None:
    with pytest.raises(InputValidationError, match="Route path"):

        @route(path, RouteType.PAGE)
        class View:
            pass


@pytest.mark.parametrize(
    "methods",
    (
        ("",),
        (" ",),
        ("GET", " "),
        ("GET", "", "POST"),
    ),
)
def test_route_rejects_methods_with_blank_entries(methods: tuple[str, ...]) -> None:
    with pytest.raises(InputValidationError, match="Route methods"):

        @route("/status", RouteType.PAGE, methods=methods)
        class View:
            pass


@pytest.mark.parametrize("template", ("", " ", 123, object()))
def test_route_rejects_invalid_template_when_provided(template: object) -> None:
    with pytest.raises(InputValidationError, match="Route template name"):

        @route("/home", RouteType.PAGE, template=cast(Any, template))
        class View:
            pass


def test_inspect_route_tree_reports_installed_routes_and_endpoint_shape(
    tmp_path: Path,
) -> None:
    app = _app()

    @app.get("/", name="home", response_class=HTMLResponse)
    @route_template("pages/home.html")
    @route_type(RouteType.PAGE)
    async def home() -> HTMLResponse:
        return HTMLResponse("home")

    @app.post("/login", name="login")
    async def login(email: str = Form(...)) -> dict[str, str]:
        return {"email": email}

    @app.get("/api/items/{item_id}", name="item-detail")
    async def item_detail(item_id: int) -> dict[str, int]:
        return {"item_id": item_id}

    @app.websocket("/ws", name="events")
    async def events() -> None:
        return None

    static_root = tmp_path / "static"
    static_root.mkdir()
    app.mount("/static", StaticFiles(directory=static_root), name="static")

    subapp = _app()

    @subapp.get("/status", name="status")
    async def status() -> dict[str, str]:
        return {"status": "ok"}

    app.mount("/tools", subapp, name="tools")

    inspection = inspect_route_tree(app)
    records = {record.path: record for record in inspection.routes}

    assert tuple(records) == (
        "/",
        "/api/items/{item_id}",
        "/login",
        "/static",
        "/tools",
        "/tools/status",
        "/ws",
    )
    assert records["/"].kind == RouteKind.HTTP
    assert records["/"].shape.route_type == RouteType.PAGE
    assert records["/"].shape.template == "pages/home.html"
    assert records["/login"].shape.accepts_body is True
    assert records["/login"].shape.accepts_form is True
    assert records["/api/items/{item_id}"].shape.route_type == RouteType.API
    assert records["/api/items/{item_id}"].shape.path_parameters == ("item_id",)
    assert records["/static"].kind == RouteKind.STATIC
    assert records["/tools"].kind == RouteKind.MOUNT
    assert records["/tools/status"].kind == RouteKind.HTTP
    assert records["/ws"].kind == RouteKind.WEBSOCKET

    tree_nodes = json.loads(routes_tool.render_json(inspection))["tree"]["children"]
    tree_by_path = {node["path"]: node for node in tree_nodes}
    assert tree_by_path["/static"]["opaque"] is True
    assert tree_by_path["/tools"]["opaque"] is False


def test_inspect_route_tree_uses_wybra_origin_metadata() -> None:
    app = _app()
    router = APIRouter()

    @router.get("/status", name="module-status")
    async def status() -> dict[str, str]:
        return {"status": "ok"}

    register_module_routes(
        app,
        (
            ConfiguredModuleRouter(
                module_name="example",
                label="api",
                router=router,
                prefix="/api",
            ),
        ),
    )

    inspection = inspect_route_tree(app)
    record = next(route for route in inspection.routes if route.path == "/api/status")

    assert record.origin is not None
    assert record.origin.module_name == "example"
    assert record.origin.router_label == "api"
    assert record.origin.include_prefix == "/api"
    assert record.origin.route_name == "module-status"


def test_inspect_route_tree_detects_duplicate_route_names_and_method_paths() -> None:
    app = _app()

    async def first() -> dict[str, str]:
        return {"handler": "first"}

    async def second() -> dict[str, str]:
        return {"handler": "second"}

    app.add_api_route("/same", first, methods=["GET"], name="duplicate")
    app.add_api_route("/same", second, methods=["GET"], name="duplicate")

    inspection = inspect_route_tree(app)
    problem_kinds = {problem.kind for problem in inspection.problems}

    assert RouteProblemKind.DUPLICATE_NAME in problem_kinds
    assert RouteProblemKind.DUPLICATE_METHOD_PATH in problem_kinds


def test_inspect_route_tree_uses_effective_names_for_mounted_routes() -> None:
    app = _app()
    api_app = _app()
    admin_app = _app()

    @api_app.get("/status", name="status")
    async def api_status() -> dict[str, str]:
        return {"status": "ok"}

    @admin_app.get("/status", name="status")
    async def admin_status() -> dict[str, str]:
        return {"status": "ok"}

    app.mount("/api", api_app, name="api")
    app.mount("/admin", admin_app, name="admin")

    inspection = inspect_route_tree(app)
    route_names = {route.path: route.name for route in inspection.routes}

    assert route_names["/api/status"] == "api:status"
    assert route_names["/admin/status"] == "admin:status"
    assert not any(
        problem.kind == RouteProblemKind.DUPLICATE_NAME
        for problem in inspection.problems
    )


def test_inspect_route_tree_detects_unmatched_origin_metadata() -> None:
    app = _app()

    async def endpoint() -> dict[str, str]:
        return {"status": "ok"}

    orphaned_route = Route("/orphaned", endpoint=endpoint, methods=["GET"])
    record_route_origin(
        app,
        orphaned_route,
        RouteOrigin(
            module_name="example",
            router_label="api",
            include_prefix="/api",
            route_name="orphaned",
            path="/api/orphaned",
            methods=("GET",),
        ),
    )

    inspection = inspect_route_tree(app)

    assert any(
        problem.kind == RouteProblemKind.INCOHERENT_ORIGIN
        for problem in inspection.problems
    )


def test_renderers_use_the_same_route_tree_model() -> None:
    app = _app()

    @app.get("/api/status", name="status")
    async def status() -> dict[str, str]:
        return {"status": "ok"}

    inspection = inspect_route_tree(app)

    succinct = routes_tool.render_succinct(inspection)
    graph = routes_tool.render_graph(inspection)
    mermaid = routes_tool.render_mermaid(inspection)
    json_output = json.loads(routes_tool.render_json(inspection))

    assert "GET          /api/status name=status" in succinct
    assert "api" in graph
    assert "status" in graph
    assert mermaid.startswith("flowchart TD\n")
    assert "/api/status" in mermaid
    assert json_output["routes"][0]["path"] == "/api/status"
    assert json_output["tree"]["children"][0]["label"] == "api"


def test_render_inspection_rejects_unknown_output_format() -> None:
    inspection = inspect_route_tree(_app())

    with pytest.raises(ValueError, match="Unsupported route output format"):
        routes_tool.render_inspection(inspection, "yaml")


def test_graph_renderer_uses_compact_visual_tree() -> None:
    app = _app()
    home_router = APIRouter()
    account_router = APIRouter()
    partials_router = APIRouter()

    @home_router.get("/", name="public:home")
    async def home() -> dict[str, str]:
        return {"page": "home"}

    @home_router.get("/health", name="health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @account_router.get("", name="auth:account")
    async def account() -> dict[str, str]:
        return {"page": "account"}

    @account_router.api_route("/login", methods=["GET", "POST"], name="auth:login")
    async def login() -> dict[str, str]:
        return {"page": "login"}

    @partials_router.get("/partials/theme-mode", name="theme-mode")
    async def theme_mode() -> dict[str, str]:
        return {"partial": "theme-mode"}

    @partials_router.get("/partials/theme-selector", name="theme-selector")
    async def theme_selector() -> dict[str, str]:
        return {"partial": "theme-selector"}

    register_module_routes(
        app,
        (
            ConfiguredModuleRouter(
                module_name="app",
                label="default",
                router=home_router,
                prefix="",
            ),
            ConfiguredModuleRouter(
                module_name="wybra.auth",
                label="account",
                router=account_router,
                prefix="/account",
            ),
            ConfiguredModuleRouter(
                module_name="wybra.core",
                label="partials",
                router=partials_router,
                prefix="",
            ),
        ),
    )

    graph = routes_tool.render_graph(inspect_route_tree(app))

    assert graph.splitlines() == [
        "/ [get] public:home app:default",
        "├─ [get] /account auth:account wybra.auth:account",
        "│  └─ [get,post] /login auth:login",
        "├─ [get] /health health",
        "└─ /partials wybra.core:partials",
        "   ├─ [get] /theme-mode theme-mode (partial)",
        "   └─ [get] /theme-selector theme-selector (partial)",
    ]


def test_graph_renderer_preserves_trailing_slash_route_nodes() -> None:
    app = _app()

    @app.get("/account", name="account")
    async def account() -> dict[str, str]:
        return {"page": "account"}

    @app.get("/account/", name="account-trailing")
    async def account_trailing() -> dict[str, str]:
        return {"page": "account"}

    inspection = inspect_route_tree(app)
    tree = json.loads(routes_tool.render_json(inspection))["tree"]
    account_node = tree["children"][0]
    trailing_node = account_node["children"][0]

    assert [route.path for route in inspection.routes] == ["/account", "/account/"]
    assert account_node["path"] == "/account"
    assert trailing_node["path"] == "/account/"
    assert routes_tool.render_graph(inspection).splitlines() == [
        "/",
        "└─ [get] /account account",
        "   └─ [get] / account-trailing",
    ]
    assert "/account/" in routes_tool.render_mermaid(inspection)


def test_template_metadata_is_not_inferred_from_handler_source() -> None:
    app = _app()

    @app.get("/implicit-template", response_class=HTMLResponse)
    async def implicit_template() -> HTMLResponse:
        template_name = "pages/implicit.html"
        return HTMLResponse(template_name)

    inspection = inspect_route_tree(app)
    record = next(
        route for route in inspection.routes if route.path == "/implicit-template"
    )

    assert record.shape.route_type == RouteType.PAGE
    assert record.shape.template is None


def test_routes_command_outputs_configured_route_tree(
    monkeypatch,
    capsys,
) -> None:
    app = _app()

    @app.get("/api/status", name="status")
    async def status() -> dict[str, str]:
        return {"status": "ok"}

    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main(["--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out)["routes"][0]["path"] == "/api/status"


def test_routes_command_config_option_selects_app_config(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    selected_config = tmp_path / "selected.toml"
    selected_config.write_text(
        """
        [app]
        modules = ["configured_app"]

        [app.runserver]
        asgi_app = "configured_app:app"

        [app.templates]
        auto_reload = true
        cache_size = 0

        [app.assets]
        url_path = "/static/"
        root = "static"
        """,
        encoding="utf-8",
    )
    app = _app()

    @app.get("/selected", name="selected")
    async def selected() -> dict[str, str]:
        return {"source": "selected"}

    module = type("ConfiguredAppModule", (), {"app": app})
    monkeypatch.setitem(sys.modules, "configured_app", module)
    monkeypatch.setenv("APP_CONFIG", (tmp_path / "ambient.toml").as_posix())
    monkeypatch.chdir(tmp_path)

    exit_code = routes_tool.main(["--config", selected_config.as_posix(), "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert json.loads(captured.out)["routes"][0]["path"] == "/selected"
    assert captured.err == ""


def test_routes_command_inspects_routes_installed_during_lifespan(
    monkeypatch,
    capsys,
) -> None:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        @app.get("/runtime", name="runtime")
        async def runtime() -> dict[str, str]:
            return {"status": "ok"}

        yield

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main(["--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out)["routes"][0]["path"] == "/runtime"


def test_routes_command_ignores_incompatible_lifespan_context(
    monkeypatch,
    capsys,
) -> None:
    @asynccontextmanager
    async def lifespan():
        yield

    async def endpoint(request):
        return None

    app = _AppWithRoutes([Route("/plain", endpoint, name="plain")])
    app.router = type("Router", (), {})()
    app.router.lifespan_context = lifespan
    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main(["--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out)["routes"][0]["path"] == "/plain"


def test_routes_command_ignores_non_async_lifespan_context(
    monkeypatch,
    capsys,
) -> None:
    class SyncContext:
        def __aenter__(self):
            msg = "synchronous context should not be entered"
            raise AssertionError(msg)

        def __aexit__(self, *_args: object) -> None:
            return None

    async def endpoint(request):
        return None

    app = _AppWithRoutes([Route("/plain", endpoint, name="plain")])
    app.router = type("Router", (), {})()
    app.router.lifespan_context = SyncContext()
    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main(["--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out)["routes"][0]["path"] == "/plain"


def test_routes_command_ignores_router_without_lifespan_context(
    monkeypatch,
    capsys,
) -> None:
    async def endpoint(request):
        return None

    app = _AppWithRoutes([Route("/plain", endpoint, name="plain")])
    app.router = object()
    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main(["--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out)["routes"][0]["path"] == "/plain"


def test_routes_command_reports_lifespan_startup_type_error(
    monkeypatch,
    capsys,
) -> None:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        raise TypeError("startup failed")
        yield

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main(["--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert "configuration: failed" in captured.err
    assert "Failed to inspect configured ASGI app route tree" in captured.err
    assert "startup failed" in captured.err


@pytest.mark.parametrize(
    ("shortcut", "expected"),
    [
        ("--succinct", "GET          /api/status name=status"),
        ("--graph", "[get] /status status"),
        ("--mermaid", "flowchart TD\n"),
        ("--json", "/api/status"),
    ],
)
def test_routes_command_output_format_shortcuts(
    monkeypatch,
    capsys,
    shortcut: str,
    expected: str,
) -> None:
    app = _app()

    @app.get("/api/status", name="status")
    async def status() -> dict[str, str]:
        return {"status": "ok"}

    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main([shortcut])
    captured = capsys.readouterr()

    assert exit_code == 0
    if shortcut == "--json":
        assert json.loads(captured.out)["routes"][0]["path"] == expected
    elif shortcut == "--mermaid":
        assert captured.out.startswith(expected)
        assert "/api/status" in captured.out
    else:
        assert expected in captured.out
    assert captured.err == ""


def test_routes_command_rejects_conflicting_output_formats(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: _app())

    exit_code = routes_tool.main(["--json", "--graph"])
    captured = capsys.readouterr()

    assert exit_code == 2
    assert captured.out == ""
    assert "Choose only one route tree output format." in captured.err


def test_routes_command_check_fails_when_route_tree_has_problems(
    monkeypatch,
    capsys,
) -> None:
    app = _app()

    async def first() -> dict[str, str]:
        return {"handler": "first"}

    async def second() -> dict[str, str]:
        return {"handler": "second"}

    app.add_api_route("/same", first, methods=["GET"], name="duplicate")
    app.add_api_route("/same", second, methods=["GET"], name="duplicate")
    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main(["--check"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "problem duplicate-name" in captured.out
    assert captured.err == ""


def test_routes_command_quiet_check_reports_only_exit_status(
    monkeypatch,
    capsys,
) -> None:
    app = _app()

    @app.get("/api/status", name="status")
    async def status() -> dict[str, str]:
        return {"status": "ok"}

    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main(["--check", "--quiet"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == ""
    assert captured.err == ""


def test_routes_command_quiet_check_fails_without_output(
    monkeypatch,
    capsys,
) -> None:
    app = _app()

    async def first() -> dict[str, str]:
        return {"handler": "first"}

    async def second() -> dict[str, str]:
        return {"handler": "second"}

    app.add_api_route("/same", first, methods=["GET"], name="duplicate")
    app.add_api_route("/same", second, methods=["GET"], name="duplicate")
    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", lambda: app)

    exit_code = routes_tool.main(["--check", "--quiet"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == ""


def test_routes_command_reports_configuration_failure(
    monkeypatch,
    capsys,
) -> None:
    def fail_to_load_app() -> object:
        raise ProjectToolConfigurationError("missing app target")

    monkeypatch.setattr(routes_tool, "load_configured_asgi_app", fail_to_load_app)

    exit_code = routes_tool.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert "configuration: failed" in captured.err
    assert "missing app target" in captured.err


@pytest.mark.parametrize("routes", [42, {"route": object()}, "not routes"])
def test_routes_command_reports_unsupported_route_tree_shape(
    monkeypatch,
    capsys,
    routes: object,
) -> None:
    monkeypatch.setattr(
        routes_tool,
        "load_configured_asgi_app",
        lambda: _AppWithRoutes(routes),
    )

    exit_code = routes_tool.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert "configuration: failed" in captured.err
    assert "unsupported route tree" in captured.err


def test_routes_command_reports_route_tree_inspection_type_error(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        routes_tool,
        "load_configured_asgi_app",
        lambda: _AppWithRoutes(_BrokenRouteIterable()),
    )

    exit_code = routes_tool.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert "configuration: failed" in captured.err
    assert "Failed to inspect configured ASGI app route tree" in captured.err
    assert "route iterator failed" in captured.err
