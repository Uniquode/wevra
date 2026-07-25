from __future__ import annotations

import math

import pytest

from wybra.utils.safety import (
    MAX_SAFE_METADATA_BYTES,
    MAX_SAFE_METADATA_DEPTH,
    MAX_SAFE_METADATA_ITEMS,
    MAX_SAFE_STRING_LENGTH,
    safe_json_metadata,
    truncate_safe_string,
)


def test_safe_json_metadata_redacts_common_credential_names() -> None:
    metadata = safe_json_metadata(
        {
            "api_key": "api-secret",
            "privateKey": "private-secret",
            "signing-key": "signing-secret",
            "details": {"access_token": "token-secret"},
        }
    )

    assert metadata == {
        "api_key": "[redacted]",
        "details": {"access_token": "[redacted]"},
        "privateKey": "[redacted]",
        "signing-key": "[redacted]",
    }


def test_safe_json_metadata_redacts_credentials_inside_lists() -> None:
    metadata = safe_json_metadata(
        {
            "records": [
                {
                    "api_key": "secret",
                    "completed": 1,
                }
            ]
        }
    )

    assert metadata == {
        "records": [
            {
                "api_key": "[redacted]",
                "completed": 1,
            }
        ]
    }


def test_safe_json_metadata_preserves_non_sensitive_plural_metrics() -> None:
    metadata = safe_json_metadata(
        {
            "sessions_migrated": 7,
            "tokens_processed": 42,
        }
    )

    assert metadata == {
        "sessions_migrated": 7,
        "tokens_processed": 42,
    }


@pytest.mark.parametrize(
    "key",
    (
        "tokens",
        "secrets",
        "credentials",
        "passwords",
        "sessions",
        "cookies",
        "password1",
        "X-Auth-Tokens",
        "api_keys",
        "privateKeys",
    ),
)
def test_safe_json_metadata_redacts_plural_and_suffixed_secret_names(
    key: str,
) -> None:
    assert safe_json_metadata({key: "sensitive"}) == {key: "[redacted]"}


def test_safe_json_metadata_is_deeply_immutable() -> None:
    supplied = {"details": {"completed": 1}}
    metadata = safe_json_metadata(supplied)

    supplied["details"]["completed"] = 2  # type: ignore[index]
    exposed = metadata["details"]
    exposed["completed"] = 3  # type: ignore[index]

    assert metadata == {"details": {"completed": 1}}
    with pytest.raises(TypeError):
        metadata["details"] = {}  # type: ignore[index]


def test_safe_json_metadata_accepts_exact_item_limit() -> None:
    metadata = safe_json_metadata({"items": list(range(MAX_SAFE_METADATA_ITEMS - 1))})

    assert len(metadata["items"]) == MAX_SAFE_METADATA_ITEMS - 1  # type: ignore[arg-type]


def test_safe_json_metadata_rejects_first_item_over_limit() -> None:
    with pytest.raises(ValueError, match="item count"):
        safe_json_metadata({"items": list(range(MAX_SAFE_METADATA_ITEMS))})


def test_safe_json_metadata_rejects_excessive_depth() -> None:
    value: dict[str, object] = {}
    for _ in range(MAX_SAFE_METADATA_DEPTH + 1):
        value = {"nested": value}

    with pytest.raises(ValueError, match="nesting depth"):
        safe_json_metadata(value)


def test_safe_json_metadata_accepts_exact_depth_limit() -> None:
    value: dict[str, object] = {}
    for _ in range(MAX_SAFE_METADATA_DEPTH):
        value = {"nested": value}

    assert safe_json_metadata(value) == value


def test_safe_json_metadata_rejects_excessive_encoded_size() -> None:
    values = {
        f"field_{index}": "x" * MAX_SAFE_STRING_LENGTH
        for index in range(MAX_SAFE_METADATA_BYTES // MAX_SAFE_STRING_LENGTH + 1)
    }

    with pytest.raises(ValueError, match="encoded size"):
        safe_json_metadata(values)


def test_safe_json_metadata_enforces_exact_encoded_size_boundary() -> None:
    base = {f"field_{index:02d}": "x" * MAX_SAFE_STRING_LENGTH for index in range(31)}
    with_empty_padding = safe_json_metadata({**base, "padding": ""})
    padding_length = MAX_SAFE_METADATA_BYTES - len(with_empty_padding.to_json())
    assert 0 <= padding_length < MAX_SAFE_STRING_LENGTH

    exact = safe_json_metadata({**base, "padding": "x" * padding_length})

    assert len(exact.to_json()) == MAX_SAFE_METADATA_BYTES
    with pytest.raises(ValueError, match="encoded size"):
        safe_json_metadata({**base, "padding": "x" * (padding_length + 1)})


def test_safe_json_metadata_rejects_overlong_strings() -> None:
    with pytest.raises(ValueError, match="safe length"):
        safe_json_metadata({"message": "x" * (MAX_SAFE_STRING_LENGTH + 1)})


@pytest.mark.parametrize("value", (math.nan, math.inf, -math.inf))
def test_safe_json_metadata_rejects_non_finite_numbers(value: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        safe_json_metadata({"value": value})


def test_safe_json_metadata_rejects_nested_non_string_keys() -> None:
    with pytest.raises(ValueError, match="string keys"):
        safe_json_metadata({"nested": {1: "invalid"}})  # type: ignore[dict-item]


def test_safe_json_metadata_rejects_unsupported_values() -> None:
    with pytest.raises(ValueError, match="cannot include object"):
        safe_json_metadata({"value": object()})


def test_truncate_safe_string_keeps_marker_within_limit() -> None:
    truncated = truncate_safe_string(
        "x" * (MAX_SAFE_STRING_LENGTH + 1),
        maximum_length=MAX_SAFE_STRING_LENGTH,
    )

    assert len(truncated) == MAX_SAFE_STRING_LENGTH
    assert truncated.endswith("...")
