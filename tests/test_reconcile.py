# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_reconcile.py — Tests for independent daily payload authorities."""

from __future__ import annotations

from usagebassoon.reconcile import ReconciliationResult, reconcile_all


def test_daily_collection_has_no_graph_models_reconciliation(
    recon_result: ReconciliationResult,
) -> None:
    """Keep graph activity and date-filtered models facts independent."""
    assert recon_result == reconcile_all()
    assert recon_result.ok
