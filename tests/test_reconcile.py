# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""Reconciliation tests over a fixture set known to be partially stale.

Appendix A invariant: models-vs-report has 10 drift issues (8 metric + 2
cost, from two sessions served by report's cached assembly); models-vs-graph
has zero issues. These counts are the regression net.
"""

from __future__ import annotations

from collections import Counter

from tests.conftest import (
    EXPECTED_TOTAL_CACHE_READ,
    EXPECTED_TOTAL_CACHE_WRITE,
    EXPECTED_TOTAL_COST,
    EXPECTED_TOTAL_INPUT,
    EXPECTED_TOTAL_MESSAGES,
    EXPECTED_TOTAL_OUTPUT,
)
from usagebassoon.parsers.graph import GraphPayload
from usagebassoon.parsers.models import ModelsPayload
from usagebassoon.reconcile import ReconciliationResult, reconcile_models_graph


def test_reconcile_report_drift_is_exactly_ten(
    recon_result: ReconciliationResult,
) -> None:
    """Assert the fixture set exhibits the known report-cache drift."""
    counts = Counter(i.check for i in recon_result.issues)
    assert counts == {"models_report_metric": 8, "models_report_cost": 2}
    assert not recon_result.ok


def test_reconcile_graph_is_clean(recon_result: ReconciliationResult) -> None:
    """Assert graph reconciles with models with zero issues."""
    graph_checks = [
        i
        for i in recon_result.issues
        if i.check.startswith(("models_graph", "graph_summary"))
    ]
    assert graph_checks == []


def test_reconcile_grand_totals(models_payload: ModelsPayload) -> None:
    """Assert fixture grand totals match Appendix A exactly."""
    assert models_payload.total_input == EXPECTED_TOTAL_INPUT
    assert models_payload.total_output == EXPECTED_TOTAL_OUTPUT
    assert models_payload.total_cache_read == EXPECTED_TOTAL_CACHE_READ
    assert models_payload.total_cache_write == EXPECTED_TOTAL_CACHE_WRITE
    assert models_payload.total_messages == EXPECTED_TOTAL_MESSAGES
    assert abs(models_payload.total_cost - EXPECTED_TOTAL_COST) < 1e-9


def test_reconcile_reasoning_is_additive(
    models_payload: ModelsPayload,
    graph_payload: GraphPayload,
) -> None:
    """Assert reasoning sums on top of the four buckets."""
    four_bucket = (
        models_payload.total_input
        + models_payload.total_output
        + models_payload.total_cache_read
        + models_payload.total_cache_write
    )
    reasoning = sum(e.reasoning for e in models_payload.entries)
    assert four_bucket + reasoning == graph_payload.summary.total_tokens


def test_reconcile_clean_fixture_pair(
    models_payload: ModelsPayload,
    graph_payload: GraphPayload,
) -> None:
    """Assert models-vs-graph alone reconciles with zero issues."""
    result = reconcile_models_graph(models_payload, graph_payload)
    assert result.ok
