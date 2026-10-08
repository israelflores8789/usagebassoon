# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_privacy.py — Share-safe output sanitization tests."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from tests._cli import plain_cli_output
from usagebassoon.json_types import JsonObject
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


def test_fixture_sanitizer_preserves_batch_identity_and_fidelity(
    tmp_path: Path,
) -> None:
    """Obfuscate related synthetic files consistently without altering their facts."""
    script = Path(__file__).parent / "fixtures" / "sanitize_fixtures.py"
    inputs, outputs = tmp_path / "raw", tmp_path / "sanitized"
    inputs.mkdir()
    records: list[JsonObject] = [
        {
            "session_id": "ses_example",
            "workspace": "/customer-a/private",
            "workspace_label": "private",
            "title": "Sensitive narrative",
            "description": None,
            "task_category": "feature",
            "complexity": "moderate",
            "task_group": "Private Auth",
            "input": 42,
            "cost": 1.25,
        },
        {
            "session_ids": ["ses_example"],
            "workspace": "/customer-b/private",
            "workspace_label": "private",
            "task_group": "Private Auth",
            "reference": "ses_example",
        },
    ]
    for index, record in enumerate(records):
        (inputs / f"{index}.json").write_text(json.dumps(record))
    original = {path.name: path.read_bytes() for path in inputs.iterdir()}
    command = [sys.executable, str(script), str(inputs), "--output-dir", str(outputs)]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    assert result.returncode == 0, plain_cli_output(result.stderr)
    sanitized = [
        cast(JsonObject, json.loads((outputs / f"{index}.json").read_text()))
        for index in range(2)
    ]
    first, second = sanitized
    session = first["session_id"]
    assert isinstance(session, str)
    assert session.startswith("ses_") and len(session) == len("ses_example")
    assert second["session_ids"] == [session] and second["reference"] == session
    assert first["workspace"] == "/project-alpha"
    assert second["workspace"] == "/project-beta"
    assert first["workspace_label"] == "project-alpha"
    assert second["workspace_label"] == "project-beta"
    assert first["task_group"] == second["task_group"]
    assert first["task_group"] != "Private Auth"
    assert first["title"] == "<redacted>" and first["description"] is None
    for key in ("task_category", "complexity", "input", "cost"):
        assert first[key] == records[0][key]
    assert {path.name: path.read_bytes() for path in inputs.iterdir()} == original
    saved = {path.name: path.read_bytes() for path in outputs.iterdir()}
    repeated = subprocess.run(command, capture_output=True, text=True, check=False)
    assert repeated.returncode == 2
    assert "already exists" in plain_cli_output(repeated.stderr)
    assert {path.name: path.read_bytes() for path in outputs.iterdir()} == saved
