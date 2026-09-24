# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""drift.py — Persisted schema-drift records and presentation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SchemaDriftRecord:
    """An unresolved row from the persisted schema-drift log.

    Attributes:
        drift_id: Stable identifier assigned during collection.
        run_id: Collection run that observed the deviation.
        detected_at: Time the deviation was detected.
        payload_kind: Tokscale payload kind, when available.
        drift_kind: Contract deviation kind, when available.
        path: Observed JSON path, when available.
        detail: Human-readable deviation detail, when available.
        tokscale_ver: Tokscale version associated with the event.
    """

    drift_id: str
    run_id: str
    detected_at: datetime | None
    payload_kind: str | None
    drift_kind: str | None
    path: str | None
    detail: str | None
    tokscale_ver: str | None


def format_drift(record: SchemaDriftRecord) -> str:
    """Format one persisted drift event for terminal output.

    Args:
        record: Event to format.

    Returns:
        Compact human-readable event description.
    """
    payload = record.payload_kind or "unknown payload"
    path = record.path or "<root>"
    detail = record.detail or record.drift_kind or "schema drift"
    version = f"; tokscale {record.tokscale_ver}" if record.tokscale_ver else ""
    return f"{payload}: {path}: {detail} (run {record.run_id}{version})"
