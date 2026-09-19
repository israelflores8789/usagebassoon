# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_privacy.py — Share-safe output sanitization tests."""

from __future__ import annotations

import pyarrow as pa

from usagebassoon.privacy import sanitize_doctor_text, sanitize_table


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
