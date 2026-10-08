# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""privacy.py — Output-only credential redaction and sharing obfuscation."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from pathlib import PurePosixPath, PureWindowsPath
from urllib.parse import quote, quote_plus

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
_CREDENTIAL_MARKERS = (
    "TOKEN",
    "PASSWORD",
    "PASSWD",
    "KEY",
    "SECRET",
    "AUTH",
    "CREDENTIAL",
)
_URI_CREDENTIAL_PATTERN = re.compile(r"(\w+://)[^\s/@]+@")
_QUERY_SECRET_PATTERN = re.compile(
    r"[?&;](?P<name>[\w-]+)=[^\s&#;\"']*",
    re.IGNORECASE,
)
_ENV_ASSIGNMENT_PATTERN = re.compile(
    r"(?P<prefix>[\"']?\b(?P<name>(?=[\w-]*(?:"
    + "|".join(_CREDENTIAL_MARKERS)
    + r"))[A-Za-z_][A-Za-z0-9_-]*)[\"']?\s*[=:]\s*)"
    r"(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\s,;{}\[\]\"']+)",
    re.IGNORECASE,
)
_AUTHORIZATION_PATTERN = re.compile(
    r"\b(?:Bearer|Basic)\s+[^\s\"',;}\]]+", re.IGNORECASE
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
    sanitized = redact_credentials(value)
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
    if _credential_name(name):
        return f"{match['prefix']}<redacted>"
    return match[0]


def _credential_name(name: str) -> bool:
    """Return whether a field or environment name denotes authentication material."""
    return any(token in name.upper() for token in _CREDENTIAL_MARKERS)


def _redact_query_secret(match: re.Match[str]) -> str:
    """Redact a credential query parameter while preserving ordinary parameters."""
    return "<redacted-query>" if _credential_name(match["name"]) else match[0]


def credential_values(
    environ: Mapping[str, str], *, environment_names: Iterable[str] = ()
) -> tuple[str, ...]:
    """Select active credentials, including explicitly supplied child variables.

    Args:
        environ: Active process environment.
        environment_names: Additional potentially sensitive variable names.

    Returns:
        Nonempty values requiring exact redaction even when echoed without labels.
    """
    names = set(environment_names)
    return tuple(
        value
        for name, value in environ.items()
        if value and (name in names or _credential_name(name))
    )


def redact_credentials(value: str, *, known_values: Iterable[str] = ()) -> str:
    """Redact authentication material while preserving diagnostic locations.

    Args:
        value: Rendered text, including exception chains or child diagnostics.
        known_values: Active credentials that may appear without a field label.

    Returns:
        Credential-safe text with paths and ordinary diagnostic context retained.
    """
    variants: set[str] = set()
    for secret in known_values:
        if secret:
            variants.update((secret, quote(secret, safe=""), quote_plus(secret)))
            variants.add(json.dumps(secret)[1:-1])
            variants.add(repr(secret)[1:-1])
    sanitized = value
    for secret in sorted(variants, key=len, reverse=True):
        sanitized = sanitized.replace(secret, "<redacted>")
    sanitized = _URI_CREDENTIAL_PATTERN.sub(r"\1<credentials>@", sanitized)
    sanitized = _QUERY_SECRET_PATTERN.sub(_redact_query_secret, sanitized)
    sanitized = _AUTHORIZATION_PATTERN.sub("<redacted-authorization>", sanitized)
    return _ENV_ASSIGNMENT_PATTERN.sub(_redact_environment_secret, sanitized)


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
    """Obfuscate configuration directories and redact credentials and other paths.

    Args:
        value: One doctor display string.
        config_path: Resolved configuration file path, when available.
        database: Configured database path or public database/dataset name.

    Returns:
        A display-safe variant of ``value``.
    """

    def sanitize_part(part: str) -> str:
        """Redact a diagnostic fragment while retaining named remote targets."""
        if database and any(separator in database for separator in ("/", "\\")):
            part = part.replace(database, "<path>")
        return _sanitize_text(part, {})

    if not config_path:
        return sanitize_part(value)
    path = (
        PureWindowsPath(config_path)
        if PureWindowsPath(config_path).is_absolute()
        else PurePosixPath(config_path)
    )
    anchor = path.anchor
    first_index = 1
    if isinstance(path, PureWindowsPath) and path.drive.startswith("\\\\"):
        anchor = "\\\\directory-alpha\\directory-bravo\\"
        first_index = 3
    directories = tuple(
        _label("directory", index)
        for index in range(first_index, first_index + len(path.parts) - 2)
    )
    obfuscated = str(type(path)(anchor, *directories, path.name))
    obfuscated = redact_credentials(obfuscated)
    return obfuscated.join(sanitize_part(part) for part in value.split(config_path))
