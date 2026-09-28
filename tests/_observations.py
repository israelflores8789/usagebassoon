# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_observations.py — Observation metadata for hand-written synthetic rows."""

import json
from uuid import NAMESPACE_URL, uuid5

import pyarrow as pa


def observations(data: pa.Table) -> pa.Table:
    """Fill observation metadata on hand-written synthetic test rows."""
    rows = data.to_pylist()
    if not rows:
        return data
    obsolete = {
        "rows_in",
        "rows_inserted",
        "rows_updated",
        "drift_events",
        "detected_run_id",
        "updated_run_id",
        "observed_at",
    }
    for row in rows:
        if "started_at" in row:
            row.setdefault("day", row["started_at"].date())
            row.setdefault("domain", "collection")
            row.setdefault("collected_at", row["started_at"])
        if "drift_key" in row:
            row.setdefault("run_id", row.get("updated_run_id", "synthetic-run"))
        if "note" in row or "tag" in row:
            row.setdefault("is_deleted", False)
        for field in obsolete:
            row.pop(field, None)
        if not row.get("event_id"):
            row["event_id"] = str(
                uuid5(NAMESPACE_URL, json.dumps(row, default=str, sort_keys=True))
            )
    fields = [field for field in data.schema if field.name not in obsolete]
    inferred = pa.Table.from_pylist(rows).schema
    fields.extend(
        field for field in inferred if field.name not in {f.name for f in fields}
    )
    return pa.Table.from_pylist(rows, schema=pa.schema(fields))
