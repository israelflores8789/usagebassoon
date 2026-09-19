# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""sql_safety.py — Allowlisted public relation-query validation and generation."""

from __future__ import annotations

from collections.abc import Mapping

from sqlglot import exp, parse
from sqlglot.errors import ParseError

PUBLIC_RELATIONS = frozenset(
    {
        "daily_cost",
        "noted_sessions",
        "report_models",
        "report_summary",
        "session_model_stats",
        "session_model_stats_current",
        "session_notes",
        "session_tags",
        "tagged_sessions",
    }
)
MAX_PUBLIC_QUERY_LIMIT = 10_000
_ALLOWED_PREDICATE_NODES = frozenset(
    {
        "And",
        "Boolean",
        "Column",
        "EQ",
        "GTE",
        "GT",
        "In",
        "Is",
        "LTE",
        "LT",
        "Literal",
        "NEQ",
        "Not",
        "Null",
        "Or",
        "Paren",
        "Placeholder",
        "Identifier",
        "Where",
    }
)


def dialect_for_backend(backend: str) -> str:
    """Map a configured backend name to its SQLGlot source dialect.

    Args:
        backend: Configured UsageBassoon backend name.

    Returns:
        The matching SQLGlot dialect name.
    """
    return "duckdb" if backend == "motherduck" else backend


def _identifier(value: str, *, name: str) -> str:
    """Validate and quote a portable public-query identifier."""
    if not value.isascii() or not value.isidentifier():
        raise ValueError(f"{name} must be a simple identifier")
    return f'"{value}"'


def build_relation_query(
    relation: str,
    *,
    filters: Mapping[str, str] | None = None,
    limit: int = 1_000,
) -> tuple[str, dict[str, str]]:
    """Build a bounded allowlisted relation query with bound equality filters."""
    if relation not in PUBLIC_RELATIONS:
        raise ValueError(f"query relation is not supported: {relation!r}")
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= MAX_PUBLIC_QUERY_LIMIT
    ):
        raise ValueError(f"query limit must be between 1 and {MAX_PUBLIC_QUERY_LIMIT}")
    predicates: list[str] = []
    parameters: dict[str, str] = {}
    for index, (column, value) in enumerate((filters or {}).items()):
        if not isinstance(value, str):
            raise ValueError("query filter values must be strings")
        parameter = f"filter_{index}"
        predicates.append(f"{_identifier(column, name='query filter')} = :{parameter}")
        parameters[parameter] = value
    where = f" WHERE {' AND '.join(predicates)}" if predicates else ""
    quoted_relation = _identifier(relation, name="query relation")
    return (
        f"SELECT * FROM {quoted_relation}{where} LIMIT {limit}",
        parameters,
    )


def validate_read_only_sql(sql: str, *, dialect: str) -> None:
    """Reject public SQL outside one bounded allowlisted relation read.

    Args:
        sql: User-supplied SQL text.
        dialect: SQLGlot source dialect for the configured backend.

    Raises:
        ValueError: If SQL is malformed, unbounded, or can reach relations
            outside the public UsageBassoon allowlist.
    """
    try:
        statements = parse(sql, read=dialect)
    except ParseError as error:
        raise ValueError("query must be valid read-only SQL") from error
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise ValueError("query accepts exactly one SELECT statement")
    statement = statements[0]
    if statement.args.get("with_") is not None or statement.args.get("joins"):
        raise ValueError("query does not permit subqueries, CTEs, or joins")
    if any(
        statement.args.get(name) is not None
        for name in ("distinct", "group", "having", "order")
    ):
        raise ValueError("query only permits relation filters and LIMIT")
    if any(True for _ in statement.find_all(exp.Subquery)):
        raise ValueError("query does not permit subqueries")
    if any(True for _ in statement.find_all(exp.Func)):
        raise ValueError("query does not permit function calls")
    tables = list(statement.find_all(exp.Table))
    if len(tables) != 1:
        raise ValueError("query must read exactly one supported relation")
    table = tables[0]
    if table.name not in PUBLIC_RELATIONS or table.db or table.catalog:
        raise ValueError("query relation is not supported")
    for projection in statement.expressions:
        if not isinstance(projection, (exp.Column, exp.Star)):
            raise ValueError("query projections must be columns or *")
        if isinstance(projection, exp.Column) and projection.table:
            raise ValueError("query projections must not qualify columns")
    where = statement.args.get("where")
    if where is not None:
        for node in where.walk():
            if type(node).__name__ not in _ALLOWED_PREDICATE_NODES:
                raise ValueError("query filter is not supported")
            if isinstance(node, exp.Column) and node.table:
                raise ValueError("query filters must not qualify columns")
    limit = statement.args.get("limit")
    if limit is None or not isinstance(limit.expression, exp.Literal):
        raise ValueError("query requires a literal LIMIT")
    try:
        parsed_limit = int(limit.expression.this)
    except (TypeError, ValueError) as error:
        raise ValueError("query requires an integer LIMIT") from error
    if not 1 <= parsed_limit <= MAX_PUBLIC_QUERY_LIMIT:
        raise ValueError(f"query limit must be between 1 and {MAX_PUBLIC_QUERY_LIMIT}")
