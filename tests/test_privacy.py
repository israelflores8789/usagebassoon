# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_privacy.py — Share-safe output sanitization tests."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from usagebassoon.privacy import (
    credential_values,
    google_doctor_principal,
    redact_credentials,
    sanitize_doctor_text,
    sanitize_table,
)


@pytest.mark.windows_compatible
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


@pytest.mark.windows_compatible
def test_credential_redaction_covers_encoded_unlabeled_values() -> None:
    """Known values remain private when encoded in URLs or structured diagnostics."""
    secret = 'opaque /value"\ncredential'
    sanitized = redact_credentials(
        'opaque /value"\ncredential opaque%20%2Fvalue%22%0Acredential '
        'opaque+%2Fvalue%22%0Acredential opaque /value\\"\\ncredential',
        known_values=(secret,),
    )
    assert sanitized == "<redacted> <redacted> <redacted> <redacted>"


@pytest.mark.windows_compatible
@pytest.mark.parametrize(
    "name",
    [
        "KEY",
        "KEY_SUFFIX",
        "PREFIX_KEY",
        "PREFIX_KEY_SUFFIX",
        "api-key",
        "APIKEY",
        "accessToken",
        "clientSecret",
        "PRIVATEKEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "MOTHERDUCK_TOKEN",
        "PGPASSWORD",
        "MYSQL_PWD",
        "SERVICE_CREDENTIALS",
        "HTTP_AUTHORIZATION",
    ],
)
def test_credential_names_cover_components_and_known_compounds(name: str) -> None:
    """Mask labelled credentials in assignments and queries across naming styles."""
    assert redact_credentials(f"{name}=synthetic-secret") == f"{name}=<redacted>"
    assert redact_credentials(f"https://example.test/?{name}=synthetic-secret") == (
        "https://example.test/<redacted-query>"
    )


@pytest.mark.windows_compatible
@pytest.mark.parametrize(
    "name",
    [
        "KEYBOARD",
        "KEYBOARD_LAYOUT",
        "MONKEY",
        "TOKENIZER",
        "AUTH_METHOD",
        "AUTH_SOURCE",
        "AUTH_TYPE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "CREDENTIAL_FILE",
        "CREDENTIALS_FILE",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "OLDPWD",
        "PWD",
        "SSH_AUTH_SOCK",
        "PUBLIC_WORKSPACE",
        "TOKSCALE_CONFIG_DIR",
        "TOKSCALE_EXTRA_DIRS",
    ],
)
def test_noncredential_names_preserve_labelled_values(name: str) -> None:
    """Keep known metadata and partial-word matches out of credential redaction."""
    text = f"{name}=ordinary-value https://example.test/?{name}=ordinary-value"
    assert redact_credentials(text) == text


@pytest.mark.windows_compatible
def test_bare_credentials_are_limited_to_supported_and_forwarded_scopes() -> None:
    """Forwarding metadata does not register its value or unrelated ambient secrets."""
    environment = {
        "MOTHERDUCK_TOKEN": "opaque-backend-credential",
        "CUSTOM_AUTH": "opaque-child-credential",
        "APIKEY": "opaque-api-key",
        "PUBLIC_WORKSPACE": r"C:\Users\Alice\Documents\project",
        "KEYBOARD_LAYOUT": "us",
        "GOOGLE_APPLICATION_CREDENTIALS": "/config/credentials.json",
        "PGPASSWORD": "root",
        "UNRELATED_TOKEN": "unrelated-credential",
    }
    names = tuple(
        name for name in environment if name not in {"PGPASSWORD", "UNRELATED_TOKEN"}
    )
    assert credential_values(environment) == ("opaque-backend-credential",)
    assert set(credential_values(environment, environment_names=names)) == {
        "opaque-backend-credential",
        "opaque-child-credential",
        "opaque-api-key",
    }
    assert credential_values(
        {"PGPASSWORD": "root"}, environment_names=("PGPASSWORD",)
    ) == ("root",)


@pytest.mark.windows_compatible
def test_credential_name_overrides_support_future_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact policy exceptions override heuristics, with redaction taking priority."""
    import usagebassoon.privacy as privacy

    monkeypatch.setattr(
        privacy,
        "_CREDENTIAL_NAME_ALLOWLIST",
        privacy._CREDENTIAL_NAME_ALLOWLIST | {"CUSTOM_TOKEN", "OPAQUE_SETTING"},
    )
    monkeypatch.setattr(
        privacy,
        "_CREDENTIAL_NAME_DENYLIST",
        privacy._CREDENTIAL_NAME_DENYLIST | {"OPAQUE_SETTING"},
    )
    environment = {"CUSTOM_TOKEN": "ordinary-value", "OPAQUE_SETTING": "secret-value"}
    assert credential_values(environment, environment_names=environment) == (
        "secret-value",
    )


@pytest.mark.windows_compatible
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


@pytest.mark.windows_compatible
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
    assert r"C:\Users\<user>\config.toml" in sanitized
    assert "PASSWORD=<redacted>" in sanitized
    assert "SERVICE_SECRET=<redacted>" in sanitized


@pytest.mark.windows_compatible
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            r"process: C:\Program Files\nodejs\node.exe[18212]: startup failed; "
            r"command: 'C:\Program Files\nodejs\npx.cmd' tokscale@latest",
            r"process: C:\Program Files\nodejs\node.exe[18212]: startup failed; "
            r"command: 'C:\Program Files\nodejs\npx.cmd' tokscale@latest",
        ),
        (
            "process: /home/alice/Documents/usagebassoon/config.toml failed",
            "process: /home/<user>/Documents/<project>/config.toml failed",
        ),
        (
            "command: '/home/Alice Smith/Documents/usagebassoon/config.toml'",
            "command: '/home/<user>/Documents/<project>/config.toml'",
        ),
        (
            r"process: C:\Users\Alice Smith\Documents\usagebassoon\config.toml",
            r"process: C:\Users\<user>\Documents\<project>\config.toml",
        ),
    ],
)
def test_sanitize_doctor_text_preserves_windows_and_posix_paths(
    value: str,
    expected: str,
) -> None:
    """Keep path context while masking recognized user and project directories."""
    sanitized = sanitize_doctor_text(value, config_path=None, database=None)

    assert sanitized == expected


@pytest.mark.windows_compatible
@pytest.mark.parametrize(
    ("config_path", "expected"),
    [
        (
            "/home/alice/Documents/usagebassoon/ub_dev.toml",
            "/home/<user>/Documents/<project>/ub_dev.toml",
        ),
        (
            "/home/Alice Smith/ub_dev.toml",
            "/home/<user>/ub_dev.toml",
        ),
        (
            r"C:\Users\Alice Smith\Documents\usagebassoon\ub_dev.toml",
            r"C:\Users\<user>\Documents\<project>\ub_dev.toml",
        ),
        (
            r"\\private-server\private-share\Users\Alice\Documents\usagebassoon\ub_dev.toml",
            r"\\<host>\<share>\Users\<user>\Documents\<project>\ub_dev.toml",
        ),
    ],
)
def test_doctor_preserves_common_config_path_components(
    config_path: str,
    expected: str,
) -> None:
    """Preserve common directories while hiding user and project names."""
    sanitized = sanitize_doctor_text(
        f"path: {config_path}; failed to read {config_path}; PASSWORD=secret",
        config_path=config_path,
        database=None,
    )
    assert (
        sanitized == f"path: {expected}; failed to read {expected}; PASSWORD=<redacted>"
    )


@pytest.mark.windows_compatible
@pytest.mark.parametrize(
    ("database", "expected"),
    [
        ("usagebassoon_it", "usagebassoon_it"),
        (
            "/home/alice/projects/private-project/store.duckdb",
            "/home/<user>/projects/<project>/store.duckdb",
        ),
        (
            r"C:\Users\Alice\projects\private-project\store.duckdb",
            r"C:\Users\<user>\projects\<project>\store.duckdb",
        ),
        (
            "/home/alice/Library/Application Support/usagebassoon/store.duckdb",
            "/home/<user>/Library/Application Support/usagebassoon/store.duckdb",
        ),
    ],
)
def test_doctor_retains_named_targets_and_obfuscates_database_paths(
    database: str, expected: str
) -> None:
    """Database paths follow the same user and project masking as other paths."""
    sanitized = sanitize_doctor_text(
        f"database={database}; TOKEN=secret", config_path=None, database=database
    )
    assert sanitized == f"database={expected}; TOKEN=<redacted>"


@pytest.mark.windows_compatible
@pytest.mark.parametrize(
    ("config_path", "expected"),
    [
        (
            r"C:\Users\Alice\AppData\Local\UsageBassoon\config.toml",
            r"C:\Users\<user>\AppData\Local\UsageBassoon\config.toml",
        ),
        (
            "/home/alice/.config/usagebassoon/config.toml",
            "/home/<user>/.config/usagebassoon/config.toml",
        ),
        (
            "/home/alice/.local/share/usagebassoon/config.toml",
            "/home/<user>/.local/share/usagebassoon/config.toml",
        ),
        (
            "/Users/alice/Library/Application Support/UsageBassoon/config.toml",
            "/Users/<user>/Library/Application Support/UsageBassoon/config.toml",
        ),
    ],
)
def test_doctor_preserves_application_directories_matching_working_directory(
    config_path: str, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Application names remain visible even when the working project has that name."""
    monkeypatch.setattr("usagebassoon.privacy.Path.cwd", lambda: Path("usagebassoon"))
    assert (
        sanitize_doctor_text(
            f"path: {config_path}", config_path=config_path, database=None
        )
        == f"path: {expected}"
    )


@pytest.mark.windows_compatible
def test_doctor_masks_service_account_project_and_preserves_gcs_resource() -> None:
    """Hide account project IDs without misclassifying Google resource names as UNC."""
    value = (
        "usagebassoon@gen-lang-client-0670612427.iam.gserviceaccount.com denied "
        "on '//storage.googleapis.com/projects/_/buckets/usagebassoon-snapshots'"
    )
    expected = (
        "usagebassoon@<project-id>.iam.gserviceaccount.com denied "
        "on '//storage.googleapis.com/projects/_/buckets/usagebassoon-snapshots'"
    )
    assert sanitize_doctor_text(value, config_path=None, database=None) == expected


@pytest.mark.windows_compatible
@pytest.mark.parametrize(
    ("email", "expected"),
    [
        ("isabelle@gmail.com", "is...@gmail.com"),
        ("abc@gmail.com", "a...@gmail.com"),
        ("ab@gmail.com", "...@gmail.com"),
        ("a@gmail.com", "...@gmail.com"),
        ("sam+usage@outlook.com", "sa...@outlook.com"),
        ("isabelle@company.example", "is...@<domain>"),
    ],
)
def test_doctor_masks_user_email_handles_and_non_public_domains(
    email: str, expected: str
) -> None:
    """Retain a recognizable public-domain label without exposing full emails."""
    sanitized = sanitize_doctor_text(
        f"Principal: user account {email}",
        config_path=None,
        database=None,
    )
    assert sanitized == f"Principal: user account {expected}"
    assert email not in sanitized


@pytest.mark.windows_compatible
def test_doctor_preserves_known_auth_labels_and_redacts_other_auth_values() -> None:
    """Safe doctor auth descriptions survive the generic secret-assignment filter."""
    safe = sanitize_doctor_text(
        "GCS snapshot Auth: ADC from a credentials file",
        config_path=None,
        database=None,
    )
    configured = sanitize_doctor_text(
        "Auth: configured service-account credentials file",
        config_path=None,
        database=None,
    )
    unsafe = sanitize_doctor_text(
        "Auth: credentials=synthetic-secret", config_path=None, database=None
    )
    assert safe == "GCS snapshot Auth: ADC from a credentials file"
    assert configured == "Auth: configured service-account credentials file"
    assert unsafe == "Auth: <redacted>"


@pytest.mark.windows_compatible
def test_google_doctor_principal_distinguishes_accounts_from_external_identity() -> (
    None
):
    """Describe known Google principal types without requiring provider requests."""
    from types import SimpleNamespace

    service_account = SimpleNamespace(
        service_account_email="usagebassoon@project.iam.gserviceaccount.com"
    )
    user_account = SimpleNamespace(account="isabelle@gmail.com")

    class ExternalCredentials:
        """Model a Google external-account credential without loading Google Auth."""

        __module__ = "google.auth.external_account"

    external_account = ExternalCredentials()

    assert google_doctor_principal(service_account) == (
        "service account usagebassoon@project.iam.gserviceaccount.com"
    )
    assert google_doctor_principal(user_account) == "user account isabelle@gmail.com"
    assert (
        google_doctor_principal(external_account)
        == "external identity (email unavailable)"
    )
