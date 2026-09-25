# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_sql_parity.py — Hermetic SQLGlot and DuckDB dialect parity tests."""

from __future__ import annotations

from importlib import resources

import pytest
import sqlglot

from tests._sql_parity import (
    DIALECTS,
    assert_view_results_match,
    asset_sql,
    seed_synthetic_data,
    statements,
    view_names,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.schema_assets import PARITY_SCHEMA_ASSETS, RUNTIME_SCHEMA_ASSETS

pytestmark = pytest.mark.sql_parity

_TYPE_ALIASES = {
    "ARRAY<STRING>": "string[]",
    "ARRAY<TEXT>": "string[]",
    "BOOLEAN": "bool",
    "BOOL": "bool",
    "BIGINT": "int64",
    "DATE": "date",
    "DOUBLE": "float64",
    "FLOAT64": "float64",
    "INT": "int64",
    "INT64": "int64",
    "INTEGER": "int64",
    "STRING": "string",
    "TEXT": "string",
    "TIMESTAMP": "timestamp",
    "TIMESTAMPTZ": "timestamp",
}


def test_paired_sql_asset_inventory_is_complete() -> None:
    """Require every dialect package to ship exactly the parity asset set."""
    expected = set(PARITY_SCHEMA_ASSETS)
    for dialect in DIALECTS:
        package = resources.files(f"usagebassoon.sql.{dialect}")
        actual = {item.name for item in package.iterdir() if item.name.endswith(".sql")}
        assert actual == expected


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("filename", PARITY_SCHEMA_ASSETS)
def test_every_sql_asset_parses_in_its_native_dialect(
    dialect: str,
    filename: str,
) -> None:
    """Parse every packaged statement with SQLGlot's strict default handling."""
    assert sqlglot.parse(asset_sql(dialect, filename), read=dialect)


def test_runtime_schema_assets_exclude_dormant_migrations() -> None:
    """Keep migration execution an explicit future runtime decision."""
    assert RUNTIME_SCHEMA_ASSETS == ("ddl.sql", "views.sql")
    assert "migrations.sql" not in RUNTIME_SCHEMA_ASSETS


def test_executable_migrations_require_an_explicit_replay_case() -> None:
    """Reject unplanned migration SQL until it receives a replay test case."""
    executable = {
        dialect: statements(dialect, "migrations.sql") for dialect in DIALECTS
    }
    assert executable == {"duckdb": [], "bigquery": []}, (
        "add a versioned predecessor-schema replay case before introducing "
        "executable migration SQL"
    )


def test_ddl_relations_and_logical_columns_are_dialect_paired() -> None:
    """Compare table names, ordered columns, logical types, and nullability."""
    duckdb = _ddl_contract("duckdb")
    bigquery = _ddl_contract("bigquery")
    assert bigquery == duckdb


def test_views_have_equivalent_normalized_duckdb_ast() -> None:
    """Compare view definitions after BigQuery-to-DuckDB SQLGlot normalization."""
    duckdb = [
        _normalized_view_sql(statement)
        for statement in statements("duckdb", "views.sql")
    ]
    bigquery = [
        _normalized_view_sql(statement)
        for statement in statements("bigquery", "views.sql")
    ]
    assert bigquery == duckdb


def test_synthetic_rows_produce_equal_results_for_every_shipped_view() -> None:
    """Replay synthetic data through native and transpiled dialect SQL in DuckDB."""
    duckdb = DuckDBBackend(":memory:")
    transpiled_bigquery = DuckDBBackend(":memory:")
    try:
        for filename in PARITY_SCHEMA_ASSETS:
            duckdb.connection.execute(asset_sql("duckdb", filename))
            for statement in sqlglot.transpile(
                asset_sql("bigquery", filename),
                read="bigquery",
                write="duckdb",
                unsupported_level=sqlglot.ErrorLevel.RAISE,
            ):
                if filename == "ddl.sql" and "source_leases" in statement:
                    continue
                transpiled_bigquery.connection.execute(statement)
        seed_synthetic_data(duckdb)
        seed_synthetic_data(transpiled_bigquery)
        assert_view_results_match(duckdb, transpiled_bigquery, view_names())
    finally:
        duckdb.close()
        transpiled_bigquery.close()


def _ddl_contract(
    dialect: str,
) -> dict[str, tuple[tuple[str, str, bool, str | None], ...]]:
    """Extract a logical cross-dialect column contract from DDL statements."""
    contract: dict[str, tuple[tuple[str, str, bool, str | None], ...]] = {}
    for statement in statements(dialect, "ddl.sql"):
        if (
            not isinstance(statement, sqlglot.exp.Create)
            or statement.args["kind"] != "TABLE"
        ):
            raise ValueError("ddl.sql must contain only CREATE TABLE statements")
        schema = statement.this
        table_name = schema.this.name
        columns = []
        for column in schema.expressions:
            if not isinstance(column, sqlglot.exp.ColumnDef):
                continue
            kind = _TYPE_ALIASES[column.args["kind"].sql().upper()]
            constraints = [
                constraint.sql().upper() for constraint in column.args["constraints"]
            ]
            default = next(
                (
                    constraint
                    for constraint in constraints
                    if constraint.startswith("DEFAULT")
                ),
                None,
            )
            columns.append(
                (
                    column.name,
                    kind,
                    "NOT NULL" in constraints or "PRIMARY KEY" in constraints,
                    default,
                )
            )
        contract[table_name] = tuple(columns)
    return contract


def _normalized_view_sql(statement: sqlglot.exp.Expr) -> str:
    """Render a view AST while ignoring dialect-default null ordering metadata."""
    normalized = statement.copy()
    for ordered in normalized.find_all(sqlglot.exp.Ordered):
        ordered.set("nulls_first", None)
    return normalized.sql(dialect="duckdb", comments=False)
