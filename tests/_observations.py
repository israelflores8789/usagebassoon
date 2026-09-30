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
    for row in rows:
        if "started_at" in row:
            row.setdefault("day", row["started_at"].date())
            row.setdefault("domain", "collection")
            row.setdefault("collected_at", row["started_at"])
        if "drift_key" in row:
            row.setdefault("run_id", "synthetic-run")
        if "note" in row or "tag" in row:
            row.setdefault("op", "upsert")
            row.setdefault("updated_at", row["collected_at"])
            row.setdefault("op_id", None)
        if not row.get("event_id"):
            row["event_id"] = str(
                uuid5(NAMESPACE_URL, json.dumps(row, default=str, sort_keys=True))
            )
    fields = list(data.schema)
    inferred = pa.Table.from_pylist(rows).schema
    fields.extend(
        field for field in inferred if field.name not in {f.name for f in fields}
    )
    return pa.Table.from_pylist(rows, schema=pa.schema(fields))
