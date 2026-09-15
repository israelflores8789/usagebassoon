# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""privacy.py — Output-only obfuscation for shareable UsageBassoon artifacts."""

from __future__ import annotations

import re
from collections.abc import Mapping

import pyarrow as pa

_PSEUDONYM_PREFIXES: Mapping[str, str] = {
    "host": "host",
    "issue_key": "session",
    "session_id": "session",
    "session_label": "session",
    "tag": "tag",
    "workspace": "workspace",
    "workspace_label": "workspace",
}
_REDACTED_COLUMNS = frozenset({"note"})
_PATH_PATTERN = re.compile(r"(?<!\w)(?:~|/)[^\s,:;)]*")
_URI_CREDENTIAL_PATTERN = re.compile(r"(\w+://)[^\s/@:]+:[^\s/@]+@")


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


def sanitize_table(table: pa.Table) -> pa.Table:
    """Obfuscate known personal columns without changing data relationships.

    Values retain a stable pseudonym within this one result table. Diagnostic
    columns remain intact except ``issue_key`` values that carry a session id
    for a models/report reconciliation issue.

    Args:
        table: Raw Arrow result table from a known UsageBassoon relation.

    Returns:
        A table with share-safe identifiers and redacted note text.
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
                mapping = mappings.setdefault(column, {})
                label = mapping.setdefault(
                    session_id,
                    _label(prefix, len(mapping) + 1),
                )
                record[column] = f"{client}/{label}"
                continue
            mapping = mappings.setdefault(column, {})
            record[column] = mapping.setdefault(value, _label(prefix, len(mapping) + 1))
        for column in _REDACTED_COLUMNS:
            if record.get(column) is not None:
                record[column] = "[redacted]"
    return pa.Table.from_pylist(records, schema=table.schema)


def sanitize_doctor_text(
    value: str,
    *,
    config_path: str | None,
    database: str | None,
) -> str:
    """Redact configuration locations and credentials from doctor output.

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
    sanitized = _URI_CREDENTIAL_PATTERN.sub(r"\1<credentials>@", sanitized)
    return _PATH_PATTERN.sub("<path>", sanitized)
