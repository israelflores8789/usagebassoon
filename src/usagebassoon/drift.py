# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""drift.py — Persisted schema-drift records and presentation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SchemaDriftRecord:
    """A current schema-drift event returned by diagnostics.

    Attributes:
        domain: Tokscale command that produced the payload.
        tokscale_ver: Tokscale version that produced the payload.
        drift_key: Stable identity of the deviation within its domain.
        drift_kind: Contract deviation kind.
        path: JSON path associated with the deviation.
        detail: Human-readable deviation detail.
        contract_tokscale_ver: Tokscale version pinned by the contract.
        created_at: First observation time.
        updated_at: Most recent observation or resolution time.
        detected_run_id: First collection run that observed the event.
        updated_run_id: Most recent collection run that observed the event.
        observation_count: Number of payload observations while active.
    """

    domain: str
    tokscale_ver: str
    drift_key: str
    drift_kind: str
    path: str
    detail: str
    contract_tokscale_ver: str
    created_at: datetime
    updated_at: datetime
    detected_run_id: str
    updated_run_id: str
    observation_count: int


@dataclass(frozen=True, slots=True)
class SchemaDriftState:
    """Persisted lifecycle fields needed to resolve a drift event."""

    domain: str
    tokscale_ver: str
    drift_key: str
    drift_kind: str
    path: str
    detail: str
    contract_tokscale_ver: str
    created_at: datetime
    detected_run_id: str
    observation_count: int


type SchemaDriftIdentity = tuple[str, str, str]


def drift_identity(record: SchemaDriftState) -> SchemaDriftIdentity:
    """Return the natural key for one persisted event state."""
    return (record.domain, record.tokscale_ver, record.drift_key)


def format_drift(record: SchemaDriftRecord) -> str:
    """Format one persisted drift event for terminal output.

    Args:
        record: Event to format.

    Returns:
        Compact human-readable event description.
    """
    return (
        f"{record.domain}: {record.path}: {record.detail} "
        f"(tokscale {record.tokscale_ver}; contract "
        f"{record.contract_tokscale_ver}; observed {record.observation_count} times; "
        f"runs {record.detected_run_id}..{record.updated_run_id})"
    )
