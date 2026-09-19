# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""privacy.py — Output-only obfuscation for shareable UsageBassoon artifacts."""

from __future__ import annotations

import re
from collections.abc import Mapping

import pyarrow as pa

_PSEUDONYM_PREFIXES: Mapping[str, str] = {
    "host": "host",
    "hostname": "host",
    "issue_key": "session",
    "machine_id": "host",
    "session_id": "session",
    "session_label": "session",
    "tag": "tag",
    "workspace": "workspace",
    "workspace_label": "workspace",
}
_REDACTED_COLUMNS = frozenset({"note"})
_URI_CREDENTIAL_PATTERN = re.compile(r"(\w+://)[^\s/@:]+:[^\s/@]+@")
_QUERY_SECRET_PATTERN = re.compile(
    r"(?P<prefix>[?&;](?:token|password|passwd|secret|api[_-]?key|access[_-]?token)=[^\s&#]*)",
    re.IGNORECASE,
)
_ENV_ASSIGNMENT_PATTERN = re.compile(
    r"\b(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\s,;]+)"
)
_WINDOWS_PATH_PATTERN = re.compile(
    r"(?<![\w:])(?:[A-Za-z]:[\\/]|\\\\[^\\/\s]+[\\/][^\s,:;)]+)[^\s,:;)]*"
)
_POSIX_PATH_PATTERN = re.compile(r"(?<!\w)(?:~|/)[^\s,:;)]*")


def _label(prefix: str, index: int) -> str:
    """Return a readable deterministic label for an output-local index.

    Args:
        prefix: Semantic type of the identifier.
        index: One-based value index within its identifier type.

    Returns:
        A share-safe label such as ``session-alpha``.
    """
    alphabet = "alpha bravo charlie delta echo foxtrot golf hotel india juliet"
    words = alphabet.split()
    if index <= len(words):
        return f"{prefix}-{words[index - 1]}"
    return f"{prefix}-{index}"


def _sanitize_text(value: str, replacements: Mapping[str, str]) -> str:
    """Redact credentials and paths embedded in an arbitrary string.

    Args:
        value: Potentially sensitive free text.
        replacements: Raw known host or machine values mapped to pseudonyms.

    Returns:
        Share-safe free text.
    """
    sanitized = _URI_CREDENTIAL_PATTERN.sub(r"\1<credentials>@", value)
    sanitized = _QUERY_SECRET_PATTERN.sub("<redacted-query>", sanitized)
    sanitized = _ENV_ASSIGNMENT_PATTERN.sub(_redact_environment_secret, sanitized)
    for private_value in sorted(replacements, key=len, reverse=True):
        if private_value:
            sanitized = sanitized.replace(private_value, replacements[private_value])
    sanitized = _WINDOWS_PATH_PATTERN.sub("<path>", sanitized)
    return _POSIX_PATH_PATTERN.sub("<path>", sanitized)


def _redact_environment_secret(match: re.Match[str]) -> str:
    """Redact one environment-style assignment when its name suggests a secret.

    Args:
        match: Parsed environment-style assignment.

    Returns:
        Original assignment or a redacted value.
    """
    name = match["name"]
    if any(token in name.upper() for token in ("TOKEN", "PASSWORD", "KEY", "SECRET")):
        return f"{name}=<redacted>"
    return match[0]


def _sanitize_value(value: object, replacements: Mapping[str, str]) -> object:
    """Sanitize arbitrary nested text while preserving its container shape.

    Args:
        value: Value from a shareable Arrow record.
        replacements: Raw known host or machine values mapped to pseudonyms.

    Returns:
        A value with embedded sensitive text removed.
    """
    if isinstance(value, str):
        return _sanitize_text(value, replacements)
    if isinstance(value, list):
        return [_sanitize_value(item, replacements) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_value(item, replacements) for item in value)
    if isinstance(value, dict):
        return {key: _sanitize_value(item, replacements) for key, item in value.items()}
    return value


def sanitize_table(table: pa.Table) -> pa.Table:
    """Obfuscate known personal columns without changing data relationships.

    Values retain a stable pseudonym within this one result table. Diagnostic
    columns remain intact except ``issue_key`` values that carry a session id
    for a models/report reconciliation issue.

    Args:
        table: Raw Arrow result table from a known UsageBassoon relation.

    Returns:
        A table with share-safe identifiers, redacted notes, and sanitized
        embedded credentials or filesystem paths.
    """
    records = table.to_pylist()
    mappings: dict[str, dict[str, str]] = {}
    for record in records:
        check_name = record.get("check_name")
        for column, prefix in _PSEUDONYM_PREFIXES.items():
            value = record.get(column)
            if value is None or not isinstance(value, str):
                continue
            if column == "issue_key" and check_name not in {
                "models_report_session_set",
                "models_report_metric",
                "models_report_cost",
            }:
                continue
            if column == "issue_key" and "/" in value:
                client, session_id = value.split("/", maxsplit=1)
                mapping = mappings.setdefault(prefix, {})
                label = mapping.setdefault(
                    session_id,
                    _label(prefix, len(mapping) + 1),
                )
                record[column] = f"{client}/{label}"
                continue
            mapping = mappings.setdefault(prefix, {})
            record[column] = mapping.setdefault(value, _label(prefix, len(mapping) + 1))
    host_replacements = mappings.get("host", {})
    for record in records:
        for column, value in record.items():
            if column in _REDACTED_COLUMNS and value is not None:
                record[column] = "[redacted]"
            else:
                record[column] = _sanitize_value(value, host_replacements)
    return pa.Table.from_pylist(records, schema=table.schema)


def sanitize_doctor_text(
    value: str,
    *,
    config_path: str | None,
    database: str | None,
) -> str:
    """Redact configuration locations, credentials, and filesystem paths.

    Args:
        value: One doctor display string.
        config_path: Resolved configuration file path, when available.
        database: Configured database or dataset, when available.

    Returns:
        A display-safe variant of ``value``.
    """
    sanitized = value
    for private_value, replacement in (
        (config_path, "<config-path>"),
        (database, "<database>"),
    ):
        if private_value:
            sanitized = sanitized.replace(private_value, replacement)
    return _sanitize_text(sanitized, {})
