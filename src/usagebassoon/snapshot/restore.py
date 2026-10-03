# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""restore.py — Validated recovery planning and provider-neutral atomic restore."""

from __future__ import annotations

from collections.abc import Callable
from uuid import NAMESPACE_URL, uuid5

from usagebassoon.backends.base import StorageBackend
from usagebassoon.snapshot.reader import PreparedSnapshot


def restore_operation_id(prepared: PreparedSnapshot) -> str:
    """Keep an operation identity stable across recovery retries."""
    return str(
        uuid5(NAMESPACE_URL, "usagebassoon:restore:" + prepared.candidate.identifier)
    )


def restore_prepared(
    backend: StorageBackend,
    prepared: PreparedSnapshot,
    *,
    notice: Callable[[str], None] | None = None,
    preflight_checked: bool = False,
) -> dict[str, int]:
    """Prepare maintenance then commit validated files, resolving lost replies."""
    operation = restore_operation_id(prepared)
    if backend.restore_committed(operation):
        if notice:
            notice(
                "This snapshot restore already committed: "
                f"{prepared.candidate.identifier}"
            )
        return prepared.rows
    if not preflight_checked:
        backend.check_restore_empty()
    backend.prepare_recovery(notice=notice)
    try:
        backend.restore_snapshot(
            prepared.files,
            operation_id=operation,
            snapshot_id=prepared.candidate.identifier,
        )
    except Exception:
        if not backend.restore_committed(operation):
            raise
        if notice:
            notice("Restore committed; its acknowledgement was interrupted.")
    if notice:
        notice(
            f"Recovered {prepared.candidate.uri}, "
            f"captured {prepared.candidate.captured_at}. Run bassoon init after "
            "verification to resume applicable maintenance."
        )
    return prepared.rows
