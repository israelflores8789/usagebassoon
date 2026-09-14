# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""graph.py — Parser for `tokscale graph` JSON on stdout."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from usagebassoon.json_types import JsonValue


class TokenBreakdown(BaseModel):
    """The five token buckets for one contribution client entry."""

    model_config = ConfigDict(populate_by_name=True)

    input: int = 0
    output: int = 0
    cache_read: int = Field(default=0, alias="cacheRead")
    cache_write: int = Field(default=0, alias="cacheWrite")
    reasoning: int = 0


class ContributionClient(BaseModel):
    """One (client, model) fact within a daily contribution."""

    model_config = ConfigDict(populate_by_name=True)

    client: str
    model_id: str = Field(alias="modelId")
    provider_id: str | None = Field(default=None, alias="providerId")
    tokens: TokenBreakdown
    cost: float = 0.0
    messages: int = 0


class Contribution(BaseModel):
    """One day of tokscale activity."""

    model_config = ConfigDict(populate_by_name=True)

    date: date
    intensity: int = 0
    active_time_ms: int = Field(default=0, alias="activeTimeMs")
    clients: list[ContributionClient] = []


class GraphSummary(BaseModel):
    """Graph-level totals used for reconciliation and run metrics."""

    model_config = ConfigDict(populate_by_name=True)

    total_tokens: int = Field(alias="totalTokens")
    total_cost: float = Field(alias="totalCost")
    active_days: int = Field(alias="activeDays")


class TimeMetrics(BaseModel):
    """Session-activity telemetry for one graph payload."""

    model_config = ConfigDict(populate_by_name=True)

    total_active_time_ms: int = Field(alias="totalActiveTimeMs")
    longest_continuous_ms: int = Field(alias="longestContinuousMs")
    max_concurrent_sessions: int = Field(alias="maxConcurrentSessions")
    session_count: int = Field(alias="sessionCount")


class GraphMeta(BaseModel):
    """Payload provenance: generation timestamp + tokscale version."""

    model_config = ConfigDict(populate_by_name=True)

    generated_at: datetime = Field(alias="generatedAt")
    version: str

    @field_validator("generated_at", mode="before")
    @classmethod
    def _iso(cls, value: Any) -> datetime:
        """Parse tokscale's precise ISO 8601 timestamp.

        Args:
            value: The raw generatedAt string.

        Returns:
            A timezone-aware datetime.

        Raises:
            ValueError: If the value is not an ISO timestamp string.
        """
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise ValueError("meta.generatedAt must be an ISO timestamp")


class GraphPayload(BaseModel):
    """The complete graph payload."""

    meta: GraphMeta
    summary: GraphSummary
    time_metrics: TimeMetrics = Field(alias="timeMetrics")
    contributions: list[Contribution]


def parse_graph(payload: JsonValue) -> GraphPayload:
    """Parse graph JSON into validated daily facts and telemetry.

    Args:
        payload: Decoded stdout from `tokscale graph`.

    Returns:
        The validated graph payload.

    Raises:
        ValueError: If the payload is not a JSON object or contains
            duplicate contribution dates.
        pydantic.ValidationError: If any field fails validation.
    """
    if not isinstance(payload, dict):
        raise ValueError("graph payload must be a JSON object")
    parsed = GraphPayload.model_validate(payload)
    seen: set[date] = set()
    for c in parsed.contributions:
        if c.date in seen:
            raise ValueError(f"duplicate contribution date: {c.date}")
        seen.add(c.date)
    return parsed
