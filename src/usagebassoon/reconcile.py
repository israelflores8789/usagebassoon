# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""reconcile.py — Reconciliation result types for collection diagnostics."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ReconciliationIssue:
    """One non-fatal collection consistency observation.

    Attributes:
        check: Stable name for the observation.
        key: Optional affected natural key.
        message: Human-readable detail.
    """

    check: str
    key: str | None
    message: str


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """All observations produced for one collection run.

    Attributes:
        issues: Non-fatal consistency observations.
    """

    issues: tuple[ReconciliationIssue, ...]

    @property
    def ok(self) -> bool:
        """Return whether the pass produced zero issues."""
        return not self.issues


def reconcile_all() -> ReconciliationResult:
    """Return the reconciliation result for independent payload authorities.

    Graph supplies activity dates while daily models supplies token facts, so
    their metrics are intentionally not compared.

    Returns:
        An empty result because no current payloads share a reconciled metric.
    """
    return ReconciliationResult(())
