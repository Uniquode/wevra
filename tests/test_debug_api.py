from __future__ import annotations

import importlib
import socket

import pytest
from fastapi import FastAPI, Request
from starlette.websockets import WebSocketDisconnect
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from wybra import start_site
from wybra.config import MappingConfigSource
from wybra.diagnostics import record_sql_query
from wybra.diagnostics.capabilities import DiagnosticsCapability
from wybra.diagnostics.records import RequestDiagnostics
from wybra.testing import WybraTestClient

debug_module = importlib.import_module("wybra.diagnostics.debug")


def _debug_config(
    *,
    allowed_hosts: tuple[str, ...] = ("testclient",),
    subscription_queue_limit: int = 32,
) -> MappingConfigSource:
    return MappingConfigSource(
        {
            "app": {"modules": (), "deployment_environment": "local"},
            "wybra.diagnostics": {
                "events_enabled": True,
                "debug_enabled": True,
                "debug_allowed_hosts": allowed_hosts,
                "subscription_queue_limit": subscription_queue_limit,
                "level": "trace",
            },
        }
    )


def test_debug_websocket_lists_scopes_and_returns_filtered_snapshot() -> None:
    app = FastAPI(lifespan=start_site(config_source=_debug_config()))

    @app.get("/work")
    async def work(request: Request) -> dict[str, str]:
        record_sql_query("select 1", duration_seconds=0.01)
        return {"status": "ok"}

    with WybraTestClient(app) as client:
        client.get("/work")
        with client.websocket_connect("/__debug/ws") as websocket:
            websocket.send_json(
                {"jsonrpc": "2.0", "id": 1, "method": "diagnostics.scopes"}
            )
            scopes = websocket.receive_json()
            websocket.send_json(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "diagnostics.snapshot",
                    "params": {"scope": "sql"},
                }
            )
            snapshot = websocket.receive_json()

    assert {item["name"] for item in scopes["result"]} == {
        "sql",
        "template",
        "cache",
        "form",
        "account",
        "credential",
        "session",
        "security",
        "content_types",
        "events",
        "events.errors",
        "request",
        "site",
        "task",
        "route",
        "view",
    }
    assert snapshot["result"][0]["events"][0]["attributes"]["statement"] == "select 1"


def test_debug_websocket_returns_openrpc_discovery_document() -> None:
    app = FastAPI(lifespan=start_site(config_source=_debug_config()))

    with WybraTestClient(app) as client:
        with client.websocket_connect("/__debug/ws") as websocket:
            websocket.send_json({"jsonrpc": "2.0", "id": 1, "method": "rpc.discover"})
            document = websocket.receive_json()["result"]

    assert document["openrpc"] == "1.2.6"
    assert {method["name"] for method in document["methods"]} == {
        "rpc.discover",
        "diagnostics.scopes",
        "diagnostics.snapshot",
        "diagnostics.subscribe",
        "diagnostics.unsubscribe",
    }
    assert document["x-wybra-notifications"] == ["diagnostics.notification"]


def test_debug_websocket_returns_parse_error_and_remains_available() -> None:
    app = FastAPI(lifespan=start_site(config_source=_debug_config()))

    with WybraTestClient(app) as client:
        with client.websocket_connect("/__debug/ws") as websocket:
            websocket.send_text("{not valid JSON")
            error = websocket.receive_json()
            websocket.send_json(
                {"jsonrpc": "2.0", "id": 1, "method": "diagnostics.scopes"}
            )
            result = websocket.receive_json()

    assert error == {
        "jsonrpc": "2.0",
        "id": None,
        "error": {"code": -32700, "message": "Parse error."},
    }
    assert result["id"] == 1


def test_debug_websocket_rejects_unknown_methods_and_invalid_parameters() -> None:
    app = FastAPI(lifespan=start_site(config_source=_debug_config()))

    with WybraTestClient(app) as client:
        with client.websocket_connect("/__debug/ws") as websocket:
            websocket.send_json({"jsonrpc": "2.0", "id": 1, "method": "unknown"})
            unknown = websocket.receive_json()
            websocket.send_json(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "diagnostics.snapshot",
                    "params": {"scope": ["sql"]},
                }
            )
            invalid = websocket.receive_json()

    assert unknown["error"]["code"] == -32601
    assert invalid["error"]["code"] == -32602


def test_debug_websocket_sends_a_notification_for_its_subscription() -> None:
    app = FastAPI(lifespan=start_site(config_source=_debug_config()))

    with WybraTestClient(app) as client:
        capability = app.state.site.require_capability(DiagnosticsCapability)
        with client.websocket_connect("/__debug/ws") as websocket:
            websocket.send_json(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "diagnostics.subscribe",
                    "params": {"scopes": ["sql"]},
                }
            )
            websocket.receive_json()
            diagnostics = RequestDiagnostics(method="GET", path="/", level="trace")
            diagnostics.record_sql_query("select 1", duration_seconds=0.01)
            capability.record_completed(diagnostics)
            notification = websocket.receive_json()

    assert notification["method"] == "diagnostics.notification"
    assert notification["params"]["events"][0]["category"] == "sql"


def test_debug_websocket_marks_a_notification_when_subscription_events_drop() -> None:
    app = FastAPI(
        lifespan=start_site(config_source=_debug_config(subscription_queue_limit=1))
    )

    @app.get("/burst")
    async def burst(request: Request) -> dict[str, str]:
        capability = request.app.state.site.require_capability(DiagnosticsCapability)
        for statement in ("select 1", "select 2", "select 3"):
            diagnostics = RequestDiagnostics(method="GET", path="/", level="trace")
            diagnostics.record_sql_query(statement, duration_seconds=0.01)
            capability.record_completed(diagnostics)
        return {"status": "ok"}

    with WybraTestClient(app) as client:
        with client.websocket_connect("/__debug/ws") as websocket:
            websocket.send_json(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "diagnostics.subscribe",
                    "params": {"scopes": ["sql"]},
                }
            )
            websocket.receive_json()
            assert client.get("/burst").status_code == 200
            notification = websocket.receive_json()

    assert notification["method"] == "diagnostics.notification"
    assert notification["params"]["dropped"] is True


def test_debug_websocket_is_not_registered_without_explicit_activation() -> None:
    app = FastAPI(
        lifespan=start_site(
            config_source=MappingConfigSource(
                {"app": {"modules": (), "deployment_environment": "local"}}
            )
        )
    )

    with WybraTestClient(app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/__debug/ws"):
                pass


def test_debug_websocket_rejects_a_host_outside_the_allow_list() -> None:
    app = FastAPI(
        lifespan=start_site(config_source=_debug_config(allowed_hosts=("localhost",)))
    )

    with WybraTestClient(app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/__debug/ws"):
                pass


@pytest.mark.anyio
async def test_peer_hostname_resolution_uses_the_event_loop_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Resolver:
        calls = 0

        async def getaddrinfo(
            self,
            host: str,
            port: object,
            *,
            type: socket.SocketKind,
        ) -> list[tuple[object, ...]]:
            self.calls += 1
            assert host == "debug.example"
            assert port is None
            assert type is socket.SOCK_STREAM
            return [(0, 0, 0, "", ("203.0.113.10", 0))]

    resolver = Resolver()
    debug_module._RESOLVED_HOSTS.clear()
    monkeypatch.setattr(debug_module.asyncio, "get_running_loop", lambda: resolver)
    monkeypatch.setattr(
        debug_module.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: pytest.fail("blocking resolver was used"),
    )

    assert await debug_module._resolved_hosts("debug.example") == {"203.0.113.10"}
    assert await debug_module._resolved_hosts("debug.example") == {"203.0.113.10"}
    assert resolver.calls == 1


def test_debug_websocket_rejects_a_spoofed_host_from_a_remote_peer() -> None:
    app = FastAPI(
        lifespan=start_site(config_source=_debug_config(allowed_hosts=("localhost",)))
    )

    with WybraTestClient(app, client=("203.0.113.10", 50000)) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect(
                "/__debug/ws",
                headers={"host": "localhost"},
            ):
                pass


def test_debug_websocket_uses_the_trusted_proxy_normalised_peer() -> None:
    app = FastAPI(
        lifespan=start_site(
            config_source=_debug_config(allowed_hosts=("203.0.113.10",))
        )
    )
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=["127.0.0.1"])

    with WybraTestClient(app, client=("127.0.0.1", 50000)) as client:
        with client.websocket_connect(
            "/__debug/ws",
            headers={"x-forwarded-for": "203.0.113.10"},
        ) as websocket:
            websocket.send_json(
                {"jsonrpc": "2.0", "id": 1, "method": "diagnostics.scopes"}
            )
            response = websocket.receive_json()

    assert response["id"] == 1


def test_debug_websocket_rejects_cross_origin_browser_handshakes() -> None:
    app = FastAPI(lifespan=start_site(config_source=_debug_config()))

    with WybraTestClient(app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect(
                "/__debug/ws",
                headers={"origin": "https://untrusted.example"},
            ):
                pass


def test_invalid_subscription_replacement_preserves_the_existing_subscription() -> None:
    app = FastAPI(lifespan=start_site(config_source=_debug_config()))

    with WybraTestClient(app) as client:
        capability = app.state.site.require_capability(DiagnosticsCapability)
        with client.websocket_connect("/__debug/ws") as websocket:
            websocket.send_json(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "diagnostics.subscribe",
                    "params": {"scopes": ["sql"]},
                }
            )
            websocket.receive_json()
            websocket.send_json(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "diagnostics.subscribe",
                    "params": {"scopes": ["content_types"]},
                }
            )
            assert websocket.receive_json()["error"]["code"] == -32602
            diagnostics = RequestDiagnostics(method="GET", path="/", level="trace")
            diagnostics.record_sql_query("select 1", duration_seconds=0.01)
            capability.record_completed(diagnostics)
            notification = websocket.receive_json()

    assert notification["method"] == "diagnostics.notification"


def test_debug_snapshots_do_not_retain_concrete_token_paths() -> None:
    app = FastAPI(lifespan=start_site(config_source=_debug_config()))

    @app.get("/reset/{token}", name="reset")
    async def reset(token: str) -> dict[str, str]:
        record_sql_query("select 1", duration_seconds=0.01)
        return {"status": "ok"}

    with WybraTestClient(app) as client:
        client.get("/reset/secret-token-value")
        with client.websocket_connect("/__debug/ws") as websocket:
            websocket.send_json(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "diagnostics.snapshot",
                    "params": {"scope": "sql"},
                }
            )
            snapshot = websocket.receive_json()

    assert "secret-token-value" not in repr(snapshot)


def test_diagnostics_retention_failure_does_not_break_a_completed_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI(lifespan=start_site(config_source=_debug_config()))

    @app.get("/work")
    async def work() -> dict[str, str]:
        return {"status": "ok"}

    with WybraTestClient(app) as client:
        capability = app.state.site.require_capability(DiagnosticsCapability)
        monkeypatch.setattr(
            capability,
            "record_completed",
            lambda _diagnostics: (_ for _ in ()).throw(RuntimeError("broken")),
        )
        response = client.get("/work")

    assert response.status_code == 200
