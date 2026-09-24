# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""reconcile.py — Bounded consistency observations for collection diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from usagebassoon.parsers.daily import DailyModelsPayload

type ReconciliationIdentity = tuple[str, str]


@dataclass(frozen=True, slots=True)
class ReconciliationIssue:
    """One non-fatal collection consistency observation.

    Attributes:
        check: Stable name for the observation.
        key: Stable name of the failed assertion within the check.
        message: Human-readable detail.
    """

    check: str
    key: str
    message: str


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """All observations produced for one collection run.

    Attributes:
        issues: Non-fatal consistency observations.
        affected_days: Daily models targets that require another attempt.
        unsafe_days: Daily models targets with ambiguous natural keys.
        resolved: Previously recorded assertion identities proven clear.
    """

    issues: tuple[ReconciliationIssue, ...]
    affected_days: frozenset[date] = frozenset()
    unsafe_days: frozenset[date] = frozenset()
    resolved: tuple[ReconciliationIdentity, ...] = ()

    @property
    def ok(self) -> bool:
        """Return whether the pass produced zero issues."""
        return not self.issues


def reconcile_all(
    daily_models: dict[date, DailyModelsPayload],
) -> ReconciliationResult:
    """Check each models response against its own declared totals.

    Graph and models have different authorities and are not compared. The
    issue identity describes the failed assertion; days remain observations.

    Args:
        daily_models: Parsed date-filtered models responses.

    Returns:
        One observation per failed assertion with the latest affected day.
    """
    issues: dict[str, ReconciliationIssue] = {}
    affected_days: set[date] = set()
    unsafe_days: set[date] = set()
    for day, payload in sorted(daily_models.items()):
        totals = payload.totals
        rows = [item.stats for item in payload.entries]
        keys = [(row.client, row.session_id, row.model) for row in rows]
        if len(keys) != len(set(keys)):
            affected_days.add(day)
            unsafe_days.add(day)
            issues["duplicate_session_model"] = ReconciliationIssue(
                "models_payload_keys",
                "duplicate_session_model",
                f"{day.isoformat()}: duplicate client/session/model entries",
            )
        fields = (
            (
                "total_input_mismatch",
                "totalInput",
                totals.total_input,
                sum(row.input_tokens for row in rows),
            ),
            (
                "total_output_mismatch",
                "totalOutput",
                totals.total_output,
                sum(row.output_tokens for row in rows),
            ),
            (
                "total_cache_read_mismatch",
                "totalCacheRead",
                totals.total_cache_read,
                sum(row.cache_read for row in rows),
            ),
            (
                "total_cache_write_mismatch",
                "totalCacheWrite",
                totals.total_cache_write,
                sum(row.cache_write for row in rows),
            ),
            (
                "total_messages_mismatch",
                "totalMessages",
                totals.total_messages,
                sum(row.message_count for row in rows),
            ),
        )
        for key, declared_name, declared, actual in fields:
            if declared != actual:
                affected_days.add(day)
                issues[key] = ReconciliationIssue(
                    "models_payload_totals",
                    key,
                    f"{day.isoformat()}: {declared_name}={declared}, "
                    f"entries sum={actual}",
                )
        declared_cost = Decimal(str(totals.total_cost))
        actual_cost = sum(
            (Decimal(str(row.tokscale_cost_usd)) for row in rows), Decimal(0)
        )
        if abs(declared_cost - actual_cost) > Decimal("0.00000001"):
            affected_days.add(day)
            issues["total_cost_mismatch"] = ReconciliationIssue(
                "models_payload_totals",
                "total_cost_mismatch",
                f"{day.isoformat()}: totalCost={declared_cost}, "
                f"entries sum={actual_cost}",
            )
    return ReconciliationResult(
        tuple(issues[key] for key in sorted(issues)),
        frozenset(affected_days),
        frozenset(unsafe_days),
    )
