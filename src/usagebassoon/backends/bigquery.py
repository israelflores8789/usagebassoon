# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""bigquery.py — BigQuery backend.

TODO!!
STRUCTURAL ONLY: google-cloud-bigquery and GCP credentials
were unavailable in the offline conformance sandbox, so this module is
syntax-compiled and design-reviewed but not execution-verified. The merge
SQL follows the same current-state upsert contract as DuckDB. CI must run
this module against a real free-tier BigQuery dataset before release.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractContextManager, nullcontext
from importlib import resources

import pyarrow as pa
from google.cloud import bigquery

from usagebassoon.backends.base import UpsertResult


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
        pkg = resources.files(f"usagebassoon.sql.{self.dialect}")
        for name in ("ddl.sql", "views.sql"):
            r = pkg.joinpath(name)
            if r.is_file():
                for stmt in r.read_text().split(";"):
                    if stmt.strip():
                        self.client.query(
                            stmt,
                            job_config=bigquery.QueryJobConfig(
                                default_dataset=self.dataset_ref
                            ),
                        ).result()

    def upsert(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> UpsertResult:
        """Insert new rows and update changed current-state rows.

        Args:
            table: Target fact table.
            data: Staged Arrow batch.
            natural_keys: Natural key columns.
            change_fields: Columns compared null-safe (IS DISTINCT FROM).

        Returns:
            Separate inserted and updated row counts.
        """
        if data.num_rows == 0:
            return UpsertResult()
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
        join = " AND ".join(f"target.{key} = source.{key}" for key in nk)
        changes = " OR ".join(
            f"target.{field} IS DISTINCT FROM source.{field}" for field in change_fields
        )
        change = changes or "FALSE"
        assignments: list[str] = []
        for column in cols:
            value = f"source.{column}"
            if column == "first_seen_at":
                value = f"COALESCE(target.{column}, {value})"
            assignments.append(f"{column} = {value}")
        source_values = ", ".join(f"source.{column}" for column in cols)
        counted = self.client.query(
            f"SELECT "
            f"countif(target.{nk[0]} IS NULL) AS inserted, "
            f"countif(target.{nk[0]} IS NOT NULL AND ({change})) AS updated "
            f"FROM `{stage}` source "
            f"LEFT JOIN `{self.dataset_ref}.{table}` target ON {join}"
        ).result()
        count_row = next(iter(counted), None)
        if count_row is None:
            return UpsertResult()
        job = self.client.query(
            f"MERGE `{self.dataset_ref}.{table}` target "
            f"USING `{stage}` source ON {join} "
            f"WHEN MATCHED AND ({change}) THEN UPDATE SET {', '.join(assignments)} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
            f"VALUES ({source_values})"
        )
        job.result()
        return UpsertResult(
            inserted=int(count_row["inserted"]),
            updated=int(count_row["updated"]),
        )

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
        return self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(default_dataset=self.dataset_ref),
        ).to_arrow()

    def transaction(self) -> AbstractContextManager[None]:
        """Return a no-op context because each BigQuery DML job is atomic."""
        return nullcontext()

    def close(self) -> None:
        """Close the client."""
        self.client.close()
