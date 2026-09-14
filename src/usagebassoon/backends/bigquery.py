# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""bigquery.py — BigQuery backend.

STRUCTURAL ONLY (v4 bundle): google-cloud-bigquery and GCP credentials
were unavailable in the offline conformance sandbox, so this module is
syntax-compiled and design-reviewed but not execution-verified. The merge
SQL is the same logical statement as the DuckDB merge (both dialects
support IS DISTINCT FROM and window QUALIFY); CI must run this module
against a real free-tier BigQuery dataset before release.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractContextManager, nullcontext
from importlib import resources

import pyarrow as pa
from google.cloud import bigquery


class BigQueryBackend:
    """StorageBackend implementation over a GCP BigQuery dataset."""

    dialect = "bigquery"

    def __init__(
        self,
        project: str,
        dataset: str,
        *,
        location: str = "US",
        credentials: object | None = None,
    ) -> None:
        """Create a backend bound to a BigQuery dataset.

        Args:
            project: GCP project id.
            dataset: BigQuery dataset name.
            location: BigQuery location for dataset and jobs.
            credentials: google-auth credentials; ADC used when omitted.
        """
        self.client = bigquery.Client(
            project=project, credentials=credentials, location=location
        )
        self.dataset_ref = f"{project}.{dataset}"

    def apply_ddl(self) -> None:
        """Ensure dataset, then run ddl.sql and views.sql statements."""
        ds = bigquery.Dataset(self.dataset_ref)
        self.client.create_dataset(ds, exists_ok=True)
        pkg = resources.files(f"tokledger.sql.{self.dialect}")
        for name in ("ddl.sql", "views.sql"):
            r = pkg.joinpath(name)
            if r.is_file():
                for stmt in r.read_text().split(";"):
                    if stmt.strip():
                        self.client.query(stmt).result()

    @staticmethod
    def _latest_sql(table: str, natural_keys: Sequence[str]) -> str:
        """Inline a fresh latest-per-key scan via QUALIFY.

        Args:
            table: Project.dataset-qualified table name.
            natural_keys: Natural key columns.

        Returns:
            SQL fragment selecting the newest row per key.
        """
        nk = ", ".join(natural_keys)
        return (
            f"SELECT * FROM {table} QUALIFY row_number() "
            f"OVER (PARTITION BY {nk} "
            f"ORDER BY collected_at DESC) = 1"
        )

    def merge(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        measure_fields: Sequence[str],
    ) -> int:
        """Single-statement delta merge; atomic by construction.

        Args:
            table: Target fact table.
            data: Staged Arrow batch.
            natural_keys: Natural key columns.
            measure_fields: Columns compared null-safe (IS DISTINCT FROM).

        Returns:
            Rows appended (job.num_dml_affected_rows).
        """
        if data.num_rows == 0:
            return 0
        stage = f"{self.dataset_ref}._stage_{table}"
        self.client.load_table_from_arrow(
            data,
            stage,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
        ).result()
        nk = list(natural_keys)
        cols = data.schema.names
        measures = " OR ".join(f"l.{m} IS DISTINCT FROM s.{m}" for m in measure_fields)
        change = f" OR ({measures})" if measures else ""
        latest = self._latest_sql(f"{self.dataset_ref}.{table}", nk)
        select_cols = ", ".join(
            f"COALESCE(l.{c}, s.{c})" if c == "first_seen_at" else f"s.{c}"
            for c in cols
        )
        job = self.client.query(
            f"INSERT INTO {self.dataset_ref}.{table} ({', '.join(cols)}) "
            f"SELECT {select_cols} FROM `{stage}` s "
            f"LEFT JOIN ({latest}) l USING ({', '.join(nk)}) "
            f"WHERE l.{nk[0]} IS NULL{change}"
        )
        job.result()
        return int(job.num_dml_affected_rows or 0)

    def append(self, table: str, data: pa.Table) -> None:
        """Append a batch via a WRITE_APPEND load job.

        Args:
            table: Target table.
            data: Arrow batch.
        """
        if data.num_rows == 0:
            return
        self.client.load_table_from_arrow(
            data,
            f"{self.dataset_ref}.{table}",
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            ),
        ).result()

    def query(self, sql: str) -> pa.Table:
        """Run BigQuery Standard SQL and return Arrow.

        Args:
            sql: BigQuery-dialect SQL.

        Returns:
            Result set as an Arrow table.
        """
        return self.client.query(sql).to_arrow()

    def transaction(self) -> AbstractContextManager[None]:
        """No-op context: merges are single atomic INSERT statements."""
        return nullcontext()

    def close(self) -> None:
        """Close the client."""
        self.client.close()
