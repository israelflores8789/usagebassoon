# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""sql_safety.py — Read-only SQL validation for the public query surfaces."""

from __future__ import annotations

from sqlglot import exp, parse
from sqlglot.errors import ParseError


def dialect_for_backend(backend: str) -> str:
    """Map a configured backend name to its SQLGlot source dialect.

    Args:
        backend: Configured UsageBassoon backend name.

    Returns:
        The matching SQLGlot dialect name.
    """
    return "duckdb" if backend == "motherduck" else backend


def validate_read_only_sql(sql: str, *, dialect: str) -> None:
    """Reject statements that are not exactly one result-producing query.

    Args:
        sql: User-supplied SQL text.
        dialect: SQLGlot source dialect for the configured backend.

    Raises:
        ValueError: If SQL is malformed, has multiple statements, or can
            modify the warehouse.
    """
    try:
        statements = parse(sql, read=dialect)
    except ParseError as error:
        raise ValueError("query must be valid read-only SQL") from error
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        raise ValueError("query accepts exactly one read-only SELECT or WITH query")
