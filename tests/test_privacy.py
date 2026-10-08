# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_privacy.py — Share-safe output sanitization tests."""

from __future__ import annotations

import pyarrow as pa
import pytest

from usagebassoon.privacy import (
    redact_credentials,
    sanitize_doctor_text,
    sanitize_table,
)


@pytest.mark.parametrize(
    "text",
    [
        "md:usagebassoon_it?motherduck_token=synthetic-secret",
        "https://user:synthetic-secret@example.test/path",
        "Authorization: Bearer synthetic-secret",
        "{'Authorization': 'Basic synthetic-secret'}",
        'details={"password": "synthetic-secret", "status": "failed"}',
        "{'client_secret': 'synthetic-secret'}",
        "CUSTOM_AUTH=synthetic-secret",
        "https://example.test/?access_token=synthetic-secret&limit=1",
    ],
)
def test_credential_redaction_retains_diagnostic_locations(text: str) -> None:
    """Recognize credential forms without applying the sharing path policy."""
    location = "/workspace/collector.py:42 C:\\Users\\Alice\\worker.py"
    sanitized = redact_credentials(f"{location} failed: {text}")
    assert "synthetic-secret" not in sanitized
    assert location in sanitized
    assert "failed" in sanitized


def test_credential_redaction_covers_encoded_unlabeled_values() -> None:
    """Known values remain private when encoded in URLs or structured diagnostics."""
    secret = 'opaque /value"\ncredential'
    sanitized = redact_credentials(
        'opaque /value"\ncredential opaque%20%2Fvalue%22%0Acredential '
        'opaque+%2Fvalue%22%0Acredential opaque /value\\"\\ncredential',
        known_values=(secret,),
    )
    assert sanitized == "<redacted> <redacted> <redacted> <redacted>"


def test_sanitize_table_pseudonymizes_hosts_and_embedded_sensitive_text() -> None:
    """Keep host labels stable while redacting paths and credentials."""
    table = pa.table(
        {
            "host": ["runner-private", "runner-private", "runner-other"],
            "message": [
                "Read C:\\Users\\Alice\\secrets.txt TOKEN=super-secret "
                "https://example.test/?token=query-secret",
                "DATABASE_KEY='database-secret' from /home/alice/project",
                "machine runner-private reported PASSWORD=another-secret",
            ],
            "cpu_model": ["Private CPU Model"] * 3,
            "note": ["keep this private"] * 3,
        }
    )

    rows = sanitize_table(table).to_pylist()

    assert [row["host"] for row in rows] == [
        "host-alpha",
        "host-alpha",
        "host-bravo",
    ]
    assert "runner-private" not in rows[2]["message"]
    assert "Alice" not in rows[0]["message"]
    assert "super-secret" not in rows[0]["message"]
    assert "query-secret" not in rows[0]["message"]
    assert "database-secret" not in rows[1]["message"]
    assert "another-secret" not in rows[2]["message"]
    assert "<path>" in rows[0]["message"]
    assert "<path>" in rows[1]["message"]
    assert all(row["cpu_model"] == "Private CPU Model" for row in rows)
    assert all(row["note"] == "[redacted]" for row in rows)


def test_sanitize_doctor_text_redacts_windows_query_and_environment_secrets() -> None:
    """Remove common credential representations from doctor diagnostics."""
    value = (
        "failed at C:\\Users\\Alice\\config.toml?api_key=query-secret; "
        "PASSWORD=quoted-secret; SERVICE_SECRET=bare-secret"
    )

    sanitized = sanitize_doctor_text(
        value,
        config_path=None,
        database=None,
    )

    assert "Alice" not in sanitized
    assert "query-secret" not in sanitized
    assert "quoted-secret" not in sanitized
    assert "bare-secret" not in sanitized
    assert "<path>" in sanitized
    assert "PASSWORD=<redacted>" in sanitized
    assert "SERVICE_SECRET=<redacted>" in sanitized


@pytest.mark.parametrize(
    ("config_path", "expected"),
    [
        (
            "/home/alice/private/ub_dev.toml",
            "/directory-alpha/directory-bravo/directory-charlie/ub_dev.toml",
        ),
        (
            "/home/Alice Smith/ub_dev.toml",
            "/directory-alpha/directory-bravo/ub_dev.toml",
        ),
        (
            r"C:\Users\Alice Smith\ub_dev.toml",
            r"C:\directory-alpha\directory-bravo\ub_dev.toml",
        ),
        (
            r"\\private-server\private-share\Alice\ub_dev.toml",
            r"\\directory-alpha\directory-bravo\directory-charlie\ub_dev.toml",
        ),
    ],
)
def test_doctor_obfuscates_config_directories(config_path: str, expected: str) -> None:
    """Retain absolute path structure and filename without exposing directories."""
    sanitized = sanitize_doctor_text(
        f"path: {config_path}; failed to read {config_path}; PASSWORD=secret",
        config_path=config_path,
        database=None,
    )
    assert (
        sanitized == f"path: {expected}; failed to read {expected}; PASSWORD=<redacted>"
    )


@pytest.mark.parametrize(
    "database", ["usagebassoon_it", "/private/store.duckdb", r"C:\private\store.duckdb"]
)
def test_doctor_retains_named_targets_and_redacts_database_paths(database: str) -> None:
    """Remote target names remain actionable while file locations stay private."""
    sanitized = sanitize_doctor_text(
        f"database={database}; TOKEN=secret", config_path=None, database=database
    )
    expected = "<path>" if "store.duckdb" in database else database
    assert sanitized == f"database={expected}; TOKEN=<redacted>"
