# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""restore.py — Validated recovery planning and provider-neutral atomic restore."""

from __future__ import annotations

import logging
from collections.abc import Callable
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5

from usagebassoon.backends.base import StorageBackend
from usagebassoon.snapshot.reader import PreparedSnapshot

_LOG = logging.getLogger("usagebassoon")


def restore_operation_id(prepared: PreparedSnapshot) -> str:
    """Keep an operation identity stable across recovery retries."""
    return str(
        uuid5(
            NAMESPACE_URL,
            "usagebassoon:restore:"
            + prepared.candidate.identifier
            + ":"
            + sha256(prepared.manifest_bytes).hexdigest(),
        )
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
        report_maintenance(backend, notice)
        return prepared.rows
    if not preflight_checked:
        backend.check_restore_empty()
    try:
        backend.prepare_recovery(notice=notice)
        try:
            backend.restore_snapshot(
                prepared.files,
                operation_id=operation,
                snapshot_id=prepared.candidate.identifier,
            )
        except Exception:
            try:
                committed = backend.restore_committed(operation)
            except Exception as inspection_error:
                raise RuntimeError(
                    "Restore completion could not be determined; "
                    "retry receipt inspection before further writes."
                ) from inspection_error
            if not committed:
                raise
            _LOG.warning(
                "Restore committed; its acknowledgement was interrupted", exc_info=True
            )
            if notice:
                notice("Restore committed; its acknowledgement was interrupted.")
    finally:
        report_maintenance(backend, notice)
    if notice:
        notice(
            f"Recovered {prepared.candidate.uri}, "
            f"captured {prepared.candidate.captured_at}. Run bassoon init after "
            "verification to resume applicable maintenance."
        )
    return prepared.rows


def report_maintenance(
    backend: StorageBackend, notice: Callable[[str], None] | None
) -> None:
    """Report known maintenance state without hiding the original restore error."""
    if notice is None:
        return
    try:
        status = backend.maintenance_status()
    except Exception as error:
        _LOG.warning("Restore maintenance state could not be determined", exc_info=True)
        message = (
            f"Maintenance state could not be determined: {error}. "
            "Inspect before resuming writers."
        )
    else:
        if status is None:
            return
        message = (
            f"Maintenance status: {status[1]}. Run bassoon init after "
            "recovery verification to resume maintenance."
        )
    try:
        notice(message)
    except Exception:
        _LOG.warning("could not deliver restore maintenance notice", exc_info=True)
