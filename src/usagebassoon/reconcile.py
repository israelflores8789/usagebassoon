# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""reconcile.py — Cross-payload consistency checks for tokscale collection runs.

Design-doc authority split: models is authoritative for metrics, report for
metadata, graph for the daily dimension. Disagreements are surfaced, never
silently resolved.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from math import isclose

from usagebassoon.parsers.graph import GraphPayload
from usagebassoon.parsers.models import ModelsPayload
from usagebassoon.parsers.report import SessionRow

TOKEN_BUCKETS = ("input", "output", "cache_read", "cache_write", "reasoning")


@dataclass(frozen=True, slots=True)
class ReconciliationIssue:
    """One non-fatal disagreement between tokscale payloads."""

    check: str
    key: str | None
    message: str


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """All observations from one reconciliation pass."""

    issues: tuple[ReconciliationIssue, ...]

    @property
    def ok(self) -> bool:
        """Return whether the pass produced zero issues."""
        return not self.issues


def _close(left: float, right: float, *, tolerance: float = 1e-9) -> bool:
    """Compare currency values with a small absolute tolerance.

    Args:
        left: First value.
        right: Second value.
        tolerance: Maximum permitted absolute difference.

    Returns:
        Whether the values agree within the tolerance.
    """
    return isclose(left, right, abs_tol=tolerance, rel_tol=0.0)


def reconcile_models_report(
    models: ModelsPayload,
    report_rows: list[SessionRow],
) -> ReconciliationResult:
    """Compare live model totals to the possibly-stale report assembly.

    Args:
        models: The validated models payload (metrics authority).
        report_rows: Validated report rows (metadata authority).

    Returns:
        All session-set and metric disagreements; warnings only.
    """
    issues: list[ReconciliationIssue] = []
    totals: dict[tuple[str, str], dict[str, float]] = {}
    for row in models.entries:
        agg = totals.setdefault(
            (row.client, row.session_id),
            {
                "input": 0.0,
                "output": 0.0,
                "cache_read": 0.0,
                "messages": 0.0,
                "cost": 0.0,
            },
        )
        agg["input"] += row.input_tokens
        agg["output"] += row.output_tokens
        agg["cache_read"] += row.cache_read
        agg["messages"] += row.message_count
        agg["cost"] += row.cost_usd

    by_key = {(r.client, r.session_id): r for r in report_rows}
    for key in sorted(set(totals) - set(by_key)):
        issues.append(
            ReconciliationIssue(
                "models_report_session_set",
                f"{key[0]}/{key[1]}",
                "session in models but not report",
            )
        )
    for key in sorted(set(by_key) - set(totals)):
        issues.append(
            ReconciliationIssue(
                "models_report_session_set",
                f"{key[0]}/{key[1]}",
                "session in report but not models",
            )
        )
    fields = (
        ("input", "total_input_tokens"),
        ("output", "total_output_tokens"),
        ("cache_read", "total_cache_read"),
        ("messages", "message_count"),
    )
    for key in sorted(set(totals) & set(by_key)):
        agg, rep = totals[key], by_key[key]
        for agg_key, rep_attr in fields:
            expected = getattr(rep, rep_attr)
            if agg_key == "cache_read" and expected is None:
                expected = 0
            if int(agg[agg_key]) != expected:
                issues.append(
                    ReconciliationIssue(
                        "models_report_metric",
                        f"{key[0]}/{key[1]}",
                        f"{agg_key}: models={int(agg[agg_key])}, "
                        f"report={expected}; models remains authoritative",
                    )
                )
        if not _close(agg["cost"], rep.cost_usd):
            issues.append(
                ReconciliationIssue(
                    "models_report_cost",
                    f"{key[0]}/{key[1]}",
                    f"cost: models={agg['cost']:.12f}, "
                    f"report={rep.cost_usd:.12f}; models remains authoritative",
                )
            )
    return ReconciliationResult(tuple(issues))


def reconcile_models_graph(
    models: ModelsPayload,
    graph: GraphPayload,
) -> ReconciliationResult:
    """Assert graph daily facts reproduce models' global totals.

    Only the five token buckets enter the token comparison; message counts
    are checked separately and must never be summed as tokens.

    Args:
        models: The validated models payload with grand totals.
        graph: The validated graph payload with additive day facts.

    Returns:
        All aggregate disagreements; empty for a consistent fixture set.
    """
    issues: list[ReconciliationIssue] = []
    graph_totals = {b: 0 for b in TOKEN_BUCKETS}
    graph_msgs, graph_cost = 0, Decimal("0")
    for c in graph.contributions:
        for client in c.clients:
            t = client.tokens
            graph_totals["input"] += t.input
            graph_totals["output"] += t.output
            graph_totals["cache_read"] += t.cache_read
            graph_totals["cache_write"] += t.cache_write
            graph_totals["reasoning"] += t.reasoning
            graph_msgs += client.messages
            graph_cost += Decimal(str(client.cost))
    expected = {
        "input": models.total_input,
        "output": models.total_output,
        "cache_read": models.total_cache_read,
        "cache_write": models.total_cache_write,
        "reasoning": sum(e.reasoning for e in models.entries),
    }
    for name in TOKEN_BUCKETS:
        if graph_totals[name] != expected[name]:
            issues.append(
                ReconciliationIssue(
                    "models_graph_token_totals",
                    name,
                    f"graph={graph_totals[name]}, models={expected[name]}",
                )
            )
    if graph_msgs != models.total_messages:
        issues.append(
            ReconciliationIssue(
                "models_graph_messages",
                "messages",
                f"graph={graph_msgs}, models={models.total_messages}",
            )
        )
    if abs(graph_cost - Decimal(str(models.total_cost))) > Decimal("0.000000001"):
        issues.append(
            ReconciliationIssue(
                "models_graph_cost_total",
                "cost",
                f"graph={graph_cost}, models={models.total_cost}",
            )
        )
    token_sum = sum(expected.values())
    if graph.summary.total_tokens != token_sum:
        issues.append(
            ReconciliationIssue(
                "graph_summary_total_tokens",
                "totalTokens",
                f"summary={graph.summary.total_tokens}, five-bucket sum={token_sum}",
            )
        )
    if not _close(graph.summary.total_cost, models.total_cost):
        issues.append(
            ReconciliationIssue(
                "graph_summary_total_cost",
                "totalCost",
                f"summary={graph.summary.total_cost}, models={models.total_cost}",
            )
        )
    return ReconciliationResult(tuple(issues))


def reconcile_all(
    models: ModelsPayload,
    report_rows: list[SessionRow],
    graph: GraphPayload,
) -> ReconciliationResult:
    """Run every payload-level reconciliation check for one run.

    Args:
        models: Validated models payload.
        report_rows: Validated report rows.
        graph: Validated graph payload.

    Returns:
        The concatenated issues from all checks.
    """
    return ReconciliationResult(
        (
            *reconcile_models_report(models, report_rows).issues,
            *reconcile_models_graph(models, graph).issues,
        )
    )
