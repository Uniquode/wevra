import json
import logging
from typing import Any

import pytest
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request

from wybra.config import ConfigService, ConfigSourceError, MappingConfigSource
from wybra.events._core import EVT_SECURITY, Event, EventsCapability
from wybra.events.security import SecurityDenialEvent
from wybra.forms import (
    CSRF_COOKIE_NAME,
    CSRF_FIELD_NAME,
    CSRF_TOKEN_MAX_AGE_SECONDS,
    CSRF_TOKEN_SECRET_KEY_CURRENT,
    CSRF_TOKEN_SECRET_KEY_PREVIOUS,
    CsrfField,
    CsrfProtector,
    csrf_exempt,
    request_form_data,
    validate_csrf,
)
from wybra.forms.rotation import plan_csrf_token_secret_rotation
from wybra.forms.secrets import forms_keychain_secret_references
from wybra.site import start
from wybra.testing import WybraTestClient


def csrf_request(
    *,
    method: str,
    headers: dict[str, str],
    body: bytes = b"",
) -> Request:
    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": method,
            "path": "/",
            "headers": [
                (name.lower().encode("ascii"), value.encode("latin-1"))
                for name, value in headers.items()
            ],
        },
        receive,
    )


@pytest.mark.anyio
async def test_csrf_form_validation_rejects_non_form_content_type() -> None:
    nonce = "a" * 32
    protector = CsrfProtector("test-secret")
    token = protector.create_token(nonce)
    body = json.dumps({CSRF_FIELD_NAME: token}).encode("utf-8")
    request = csrf_request(
        method="POST",
        headers={
            "content-type": "application/json",
            "content-length": str(len(body)),
            "cookie": f"{CSRF_COOKIE_NAME}={nonce}",
        },
        body=body,
    )

    assert await protector.validate_request(request) is False


@pytest.mark.anyio
async def test_csrf_form_validation_rejects_oversized_form_body() -> None:
    nonce = "a" * 32
    protector = CsrfProtector("test-secret", max_form_body_bytes=8)
    token = protector.create_token(nonce)
    body = f"{CSRF_FIELD_NAME}={token}".encode()
    request = csrf_request(
        method="POST",
        headers={
            "content-type": "application/x-www-form-urlencoded",
            "content-length": str(len(body)),
            "cookie": f"{CSRF_COOKIE_NAME}={nonce}",
        },
        body=body,
    )

    assert await protector.validate_request(request) is False


@pytest.mark.anyio
async def test_csrf_form_validation_caches_parsed_form_for_downstream_views() -> None:
    nonce = "a" * 32
    protector = CsrfProtector("test-secret")
    token = protector.create_token(nonce)
    body = f"{CSRF_FIELD_NAME}={token}&field=value".encode()
    request = csrf_request(
        method="POST",
        headers={
            "content-type": "application/x-www-form-urlencoded",
            "content-length": str(len(body)),
            "cookie": f"{CSRF_COOKIE_NAME}={nonce}",
        },
        body=body,
    )

    assert await protector.validate_request(request) is True
    form_data = await request_form_data(request)
    assert form_data.get(CSRF_FIELD_NAME) == token
    assert form_data.get("field") == "value"


def test_csrf_tokens_include_signed_issue_time() -> None:
    nonce = "a" * 32
    protector = CsrfProtector("current-secret", clock=lambda: 1_000.0)

    token = protector.create_token(nonce)

    token_nonce, issued_at, signature = token.split(".")
    assert token_nonce == nonce
    assert issued_at == "1000"
    assert signature
    assert protector.validate_token(token, nonce)


def test_csrf_protector_creates_an_opaque_rendering_field() -> None:
    nonce = "a" * 32
    protector = CsrfProtector("current-secret", clock=lambda: 1_000.0)
    request = csrf_request(
        method="GET",
        headers={"cookie": f"{CSRF_COOKIE_NAME}={nonce}"},
    )

    csrf_field = protector.create_field(request)

    assert isinstance(csrf_field, CsrfField)
    assert csrf_field.rendering_context() == {
        "csrf_field_name": CSRF_FIELD_NAME,
        "csrf_token": protector.create_token(nonce),
    }
    assert csrf_field.token not in repr(csrf_field)


def test_csrf_token_validation_rejects_expired_current_secret_token() -> None:
    nonce = "a" * 32
    renderer = CsrfProtector("current-secret", clock=lambda: 1_000.0)
    validator = CsrfProtector(
        "current-secret",
        token_max_age_seconds=60,
        clock=lambda: 1_061.0,
    )

    assert validator.validate_token(renderer.create_token(nonce), nonce) is False


def test_csrf_token_validation_accepts_previous_secret_within_max_age() -> None:
    nonce = "a" * 32
    renderer = CsrfProtector("previous-secret", clock=lambda: 1_000.0)
    validator = CsrfProtector(
        "current-secret",
        previous_secrets=("previous-secret",),
        token_max_age_seconds=60,
        clock=lambda: 1_060.0,
    )

    assert validator.validate_token(renderer.create_token(nonce), nonce) is True


def test_csrf_token_validation_rejects_expired_previous_secret_token() -> None:
    nonce = "a" * 32
    renderer = CsrfProtector("previous-secret", clock=lambda: 1_000.0)
    validator = CsrfProtector(
        "current-secret",
        previous_secrets=("previous-secret",),
        token_max_age_seconds=60,
        clock=lambda: 1_061.0,
    )

    assert validator.validate_token(renderer.create_token(nonce), nonce) is False


def test_csrf_protector_rejects_invalid_previous_secret_entries() -> None:
    with pytest.raises(ValueError, match="index 1 is not a string") as non_string:
        CsrfProtector("current-secret", previous_secrets=("previous-secret", 7))  # type: ignore[arg-type]

    assert "7" not in str(non_string.value)

    with pytest.raises(ValueError, match="index 0 is blank"):
        CsrfProtector("current-secret", previous_secrets=("   ",))


def test_plan_csrf_token_secret_rotation_prepends_retired_current_secret() -> None:
    plan = plan_csrf_token_secret_rotation(
        current="current-csrf-secret",
        previous="previous-csrf-secret,older-csrf-secret",
    )

    assert plan.previous_secret_count == 3
    assert plan.previous_value == (
        "current-csrf-secret,previous-csrf-secret,older-csrf-secret"
    )
    assert plan.current_value not in {
        "current-csrf-secret",
        "previous-csrf-secret",
        "older-csrf-secret",
    }


def test_plan_csrf_token_secret_rotation_initialises_missing_previous_value() -> None:
    plan = plan_csrf_token_secret_rotation(
        current="current-csrf-secret",
        previous=None,
    )

    assert plan.previous_secret_count == 1
    assert plan.previous_value == "current-csrf-secret"


def test_plan_csrf_token_secret_rotation_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="current CSRF token secret"):
        plan_csrf_token_secret_rotation(current=None, previous=None)

    with pytest.raises(ValueError, match="current CSRF token secret"):
        plan_csrf_token_secret_rotation(current="   ", previous=None)

    with pytest.raises(ValueError, match="comma-separated"):
        plan_csrf_token_secret_rotation(
            current="current-csrf-secret",
            previous=",",
        )

    with pytest.raises(ValueError, match="unique"):
        plan_csrf_token_secret_rotation(
            current="current-csrf-secret",
            previous="current-csrf-secret",
        )


def test_csrf_token_secret_rotation_plan_repr_redacts_secret_values() -> None:
    plan = plan_csrf_token_secret_rotation(
        current="current-csrf-secret",
        previous=None,
    )

    rendered = repr(plan)

    assert plan.current_value not in rendered
    assert plan.previous_value not in rendered
    assert "current-csrf-secret" not in rendered
    assert str(plan.previous_secret_count) in rendered


@pytest.mark.anyio
async def test_csrf_form_validation_logs_rejection_reason(caplog) -> None:
    nonce = "a" * 32
    protector = CsrfProtector("test-secret")
    token = protector.create_token(nonce)
    body = f"{CSRF_FIELD_NAME}={token}".encode()
    request = csrf_request(
        method="POST",
        headers={
            "content-type": "application/x-www-form-urlencoded",
            "cookie": f"{CSRF_COOKIE_NAME}={nonce}",
        },
        body=body,
    )
    caplog.set_level(logging.DEBUG, logger="wybra.forms.csrf")

    assert await protector.validate_request(request) is False
    assert "CSRF request rejected." in caplog.text
    assert any(
        getattr(record, "csrf_reason", None) == "missing_content_length"
        for record in caplog.records
    )


@pytest.mark.anyio
async def test_csrf_denial_event_excludes_token_and_rejection_detail() -> None:
    app = FastAPI()
    site = await start(
        app,
        config_source=MappingConfigSource(
            {
                "app": {"modules": (), "deployment_environment": "local"},
                "wybra.events": {"enabled": True},
            }
        ),
    )
    observed: list[Event] = []

    async def handler(event: Event) -> None:
        observed.append(event)

    await site.require_capability(EventsCapability).subscribe(EVT_SECURITY, handler)
    token = "token-that-must-not-appear"
    request = csrf_request(
        method="POST",
        headers={
            "content-type": "application/x-www-form-urlencoded",
            "content-length": str(len(token)),
        },
        body=token.encode(),
    )
    request.scope["app"] = app
    app.state.csrf = CsrfProtector("test-secret")

    try:
        with pytest.raises(HTTPException):
            await validate_csrf(request)
    finally:
        await site.close()

    assert len(observed) == 1
    assert isinstance(observed[0], SecurityDenialEvent)
    assert str(observed[0].scope) == "security.csrf.denied"
    assert token not in repr(observed[0])


def test_csrf_dependency_allows_safe_methods_on_protected_router() -> None:
    app = FastAPI()
    app.state.csrf = CsrfProtector("test-secret")
    router = APIRouter(dependencies=[Depends(validate_csrf)])

    @router.get("/form")
    async def form() -> dict[str, bool]:
        return {"ok": True}

    app.include_router(router)

    response = WybraTestClient(app).get("/form")

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_csrf_dependency_is_noop_when_csrf_protector_missing() -> None:
    app = FastAPI()
    router = APIRouter(dependencies=[Depends(validate_csrf)])

    @router.get("/form")
    async def form() -> dict[str, bool]:
        return {"ok": True}

    @router.post("/submit")
    async def submit() -> dict[str, bool]:
        return {"ok": True}

    app.include_router(router)
    client = WybraTestClient(app)

    get_response = client.get("/form")
    post_response = client.post("/submit", data={"field": "value"})

    assert get_response.status_code == 200
    assert get_response.json() == {"ok": True}
    assert post_response.status_code == 200
    assert post_response.json() == {"ok": True}


def test_csrf_dependency_raises_when_csrf_protector_misconfigured() -> None:
    app = FastAPI()
    app.state.csrf = object()
    router = APIRouter(dependencies=[Depends(validate_csrf)])

    @router.post("/submit")
    async def submit() -> dict[str, bool]:
        return {"ok": True}

    app.include_router(router)

    with pytest.raises(
        RuntimeError, match="CSRF protector is not configured correctly"
    ):
        WybraTestClient(app).post("/submit", data={"field": "value"})


def test_csrf_dependency_rejects_unsafe_methods_without_token() -> None:
    app = FastAPI()
    app.state.csrf = CsrfProtector("test-secret")
    router = APIRouter(dependencies=[Depends(validate_csrf)])

    @router.post("/form")
    async def submit() -> dict[str, bool]:
        return {"ok": True}

    app.include_router(router)

    response = WybraTestClient(app).post("/form", data={"field": "value"})

    assert response.status_code == 403
    assert response.json() == {"detail": "Invalid CSRF token."}


def test_csrf_exempt_allows_route_to_bypass_protected_router() -> None:
    app = FastAPI()
    app.state.csrf = CsrfProtector("test-secret")
    router = APIRouter(dependencies=[Depends(validate_csrf)])

    @router.post("/callback")
    @csrf_exempt
    async def callback() -> dict[str, bool]:
        return {"ok": True}

    app.include_router(router)

    response = WybraTestClient(app).post("/callback", data={"field": "value"})

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_csrf_settings_generates_local_secret(caplog) -> None:
    from wybra.forms import FormsSettings

    caplog.set_level(logging.INFO, logger="wybra.forms.settings")

    settings = FormsSettings()

    assert settings.token_secret
    assert settings.cookie_secure is False
    assert "Generated startup-local CSRF token secret." in caplog.text


def test_csrf_settings_load_settings_generates_local_secret(caplog) -> None:
    from wybra.forms import FormsSettings

    caplog.set_level(logging.INFO, logger="wybra.forms.settings")

    settings = FormsSettings.load_settings({})

    assert settings.token_secret
    assert settings.cookie_secure is False
    assert settings.deployment_environment == "local"
    assert "Generated startup-local CSRF token secret." in caplog.text


def test_csrf_settings_requires_stable_secret_for_non_local_environment() -> None:
    from wybra.core.exceptions import ConfigurationError
    from wybra.forms import FormsSettings

    with pytest.raises(
        ConfigurationError,
        match="Non-local deployments must configure a stable CSRF token secret",
    ):
        FormsSettings(deployment_environment="production")

    with pytest.raises(ConfigurationError, match="CSRF token secret must not be blank"):
        FormsSettings(deployment_environment="production", csrf_token_secret="   ")

    with pytest.raises(
        ConfigurationError,
        match="Non-local deployments must use secure CSRF cookies",
    ):
        FormsSettings(
            deployment_environment="production",
            csrf_token_secret="production-csrf-secret",
            csrf_cookie_secure=False,
        )


def test_csrf_settings_accepts_stable_secure_non_local_configuration() -> None:
    from wybra.forms import FormsSettings

    settings = FormsSettings(
        deployment_environment="production",
        csrf_token_secret="production-csrf-secret",
        csrf_cookie_secure=True,
    )

    assert settings.token_secret == "production-csrf-secret"
    assert settings.cookie_secure is True


def test_csrf_settings_load_settings_uses_config_service_sources() -> None:
    from wybra.forms import FormsSettings

    config = ConfigService(
        [
            MappingConfigSource(
                {
                    "app": {
                        "deployment_environment": "production",
                        "modules": ("wybra.forms",),
                    },
                    "wybra.forms": {
                        "csrf_token_secret": "production-csrf-secret",
                        "csrf_cookie_secure": "true",
                    },
                }
            )
        ],
    )

    settings = FormsSettings.load_settings(
        config,
        deployment_environment="production",
    )

    assert settings.deployment_environment == "production"
    assert settings.token_secret == "production-csrf-secret"
    assert settings.cookie_secure is True


def test_csrf_settings_load_settings_requires_stable_secret_for_non_local() -> None:
    from wybra.core.exceptions import ConfigurationError
    from wybra.forms import FormsSettings

    config = ConfigService(
        [
            MappingConfigSource(
                {
                    "app": {
                        "deployment_environment": "production",
                        "modules": ("wybra.forms",),
                    },
                }
            )
        ],
    )

    with pytest.raises(
        ConfigurationError,
        match="Non-local deployments must configure a stable CSRF token secret",
    ):
        FormsSettings.load_settings(config, deployment_environment="production")


def test_csrf_settings_load_settings_transforms_secure_cookie_policy() -> None:
    from wybra.forms import FormsSettings

    settings = FormsSettings.load_settings(
        {
            "csrf_token_secret": "production-csrf-secret",
            "csrf_cookie_secure": "false",
            "deployment_environment": "local",
        }
    )

    assert settings.cookie_secure is False


def test_csrf_settings_load_settings_transforms_token_max_age() -> None:
    from wybra.forms import FormsSettings

    settings = FormsSettings.load_settings(
        {
            "csrf_token_secret": "production-csrf-secret",
            "csrf_token_max_age_seconds": "120",
            "deployment_environment": "local",
        }
    )

    assert settings.csrf_token_max_age_seconds == 120
    assert settings.protector().token_max_age_seconds == 120


def test_csrf_settings_load_settings_rejects_invalid_token_max_age() -> None:
    with pytest.raises(ConfigSourceError, match="csrf_token_max_age_seconds"):
        ConfigService(
            [
                MappingConfigSource(
                    {
                        "app": {"modules": ("wybra.forms",)},
                        "wybra.forms": {
                            "csrf_token_secret": "production-csrf-secret",
                            "csrf_token_max_age_seconds": "0",
                        },
                    }
                )
            ],
        )


def test_csrf_settings_load_settings_accepts_sectioned_mapping_environment() -> None:
    from wybra.forms import FormsSettings

    settings = FormsSettings.load_settings(
        {
            "wybra.forms": {
                "csrf_token_secret": "production-csrf-secret",
                "csrf_cookie_secure": "true",
            },
        },
        deployment_environment="production",
    )

    assert settings.deployment_environment == "production"
    assert settings.token_secret == "production-csrf-secret"
    assert settings.cookie_secure is True


def test_csrf_settings_accepts_keychain_reference_without_secret_value() -> None:
    from wybra.forms import FormsSettings

    config = ConfigService(
        [
            MappingConfigSource(
                {
                    "app": {"modules": ("wybra.forms",)},
                    "wybra.forms": {
                        "csrf_token_secret_source": "keychain",
                        "csrf_token_secret_key": CSRF_TOKEN_SECRET_KEY_CURRENT,
                        "csrf_cookie_secure": "true",
                    },
                }
            )
        ],
    )

    settings = FormsSettings.load_settings(
        config,
        deployment_environment="production",
    )

    assert settings.csrf_token_secret_reference == (
        "keychain",
        CSRF_TOKEN_SECRET_KEY_CURRENT,
    )
    assert settings.csrf_token_secret_previous_reference == (
        "keychain",
        CSRF_TOKEN_SECRET_KEY_PREVIOUS,
    )
    assert settings.token_secret is None
    assert settings.fallback_token_secret is None


def test_csrf_settings_preserves_keychain_and_inline_fallback() -> None:
    from wybra.forms import FormsSettings

    settings = FormsSettings.load_settings(
        {
            "wybra.forms": {
                "csrf_token_secret_source": "keychain",
                "csrf_token_secret_key": CSRF_TOKEN_SECRET_KEY_CURRENT,
                "csrf_token_secret": "fallback-csrf-secret",
                "csrf_cookie_secure": "true",
            },
        },
        deployment_environment="production",
    )

    assert settings.csrf_token_secret_reference == (
        "keychain",
        CSRF_TOKEN_SECRET_KEY_CURRENT,
    )
    assert settings.fallback_token_secret == "fallback-csrf-secret"


def test_csrf_settings_uses_default_keychain_reference() -> None:
    from wybra.forms import FormsSettings

    settings = FormsSettings.load_settings(
        {
            "wybra.forms": {"csrf_token_secret_source": "keychain"},
        },
        deployment_environment="production",
    )

    assert settings.csrf_token_secret_reference == (
        "keychain",
        CSRF_TOKEN_SECRET_KEY_CURRENT,
    )
    assert settings.csrf_token_secret_previous_reference == (
        "keychain",
        CSRF_TOKEN_SECRET_KEY_PREVIOUS,
    )
    assert settings.csrf_token_max_age_seconds == CSRF_TOKEN_MAX_AGE_SECONDS


def test_csrf_settings_accepts_previous_key_override() -> None:
    from wybra.forms import FormsSettings

    settings = FormsSettings.load_settings(
        {
            "wybra.forms": {
                "csrf_token_secret_source": "keychain",
                "csrf_token_secret_key": "custom/current",
                "csrf_token_secret_previous_key": "custom/previous",
            },
        },
        deployment_environment="production",
    )

    assert settings.csrf_token_secret_reference == ("keychain", "custom/current")
    assert settings.csrf_token_secret_previous_reference == (
        "keychain",
        "custom/previous",
    )


def test_csrf_settings_rejects_secret_key_without_source() -> None:
    from wybra.core.exceptions import ConfigurationError
    from wybra.forms import FormsSettings

    with pytest.raises(ConfigurationError, match="csrf_token_secret_source"):
        FormsSettings.load_settings(
            {
                "wybra.forms": {"csrf_token_secret_key": CSRF_TOKEN_SECRET_KEY_CURRENT},
            },
        )

    with pytest.raises(ConfigurationError, match="csrf_token_secret_source"):
        FormsSettings.load_settings(
            {
                "wybra.forms": {
                    "csrf_token_secret_previous_key": CSRF_TOKEN_SECRET_KEY_PREVIOUS
                },
            },
        )


def test_csrf_settings_load_settings_rejects_blank_token_secret() -> None:
    with pytest.raises(ConfigSourceError, match="csrf_token_secret"):
        ConfigService(
            [
                MappingConfigSource(
                    {
                        "app": {"modules": ("wybra.forms",)},
                        "wybra.forms": {"csrf_token_secret": "   "},
                    }
                )
            ],
        )


def test_csrf_settings_load_settings_rejects_blank_previous_key() -> None:
    with pytest.raises(ConfigSourceError, match="csrf_token_secret_previous_key"):
        ConfigService(
            [
                MappingConfigSource(
                    {
                        "app": {"modules": ("wybra.forms",)},
                        "wybra.forms": {
                            "csrf_token_secret_source": "keychain",
                            "csrf_token_secret_previous_key": "   ",
                        },
                    }
                )
            ],
        )


def test_forms_keychain_secret_references_include_csrf_current_and_previous() -> None:
    references = forms_keychain_secret_references(
        {
            "app": {"modules": ("wybra.forms",)},
            "wybra.forms": {"csrf_token_secret_source": "keychain"},
        }
    )

    assert references == (
        CSRF_TOKEN_SECRET_KEY_CURRENT,
        CSRF_TOKEN_SECRET_KEY_PREVIOUS,
    )
