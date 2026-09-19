# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""graph.py — Parser for `tokscale graph` JSON on stdout."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from usagebassoon.json_types import JsonValue
from usagebassoon.parsers._validation import (
    Identifier,
    Metadata,
    NonNegativeFloat,
    NonNegativeInt,
)


class TokenBreakdown(BaseModel):
    """The five token buckets for one contribution client entry."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    input: NonNegativeInt = 0
    output: NonNegativeInt = 0
    cache_read: NonNegativeInt = Field(default=0, alias="cacheRead")
    cache_write: NonNegativeInt = Field(default=0, alias="cacheWrite")
    reasoning: NonNegativeInt = 0


class ContributionClient(BaseModel):
    """One (client, model) fact within a daily contribution."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    client: Identifier
    model_id: Identifier = Field(alias="modelId")
    provider_id: Metadata | None = Field(default=None, alias="providerId")
    tokens: TokenBreakdown
    cost: NonNegativeFloat = 0.0
    messages: NonNegativeInt = 0


class Contribution(BaseModel):
    """One day of tokscale activity."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    date: date
    intensity: NonNegativeInt = 0
    active_time_ms: NonNegativeInt = Field(default=0, alias="activeTimeMs")
    clients: list[ContributionClient] = Field(default_factory=list)

    @field_validator("date", mode="before")
    @classmethod
    def _date(cls, value: Any) -> date:
        """Parse the ISO 8601 contribution day emitted by tokscale."""
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise ValueError("contribution date must be an ISO date")


class GraphSummary(BaseModel):
    """Graph-level totals used for reconciliation and run metrics."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    total_tokens: NonNegativeInt = Field(alias="totalTokens")
    total_cost: NonNegativeFloat = Field(alias="totalCost")
    active_days: NonNegativeInt = Field(alias="activeDays")


class TimeMetrics(BaseModel):
    """Session-activity telemetry for one graph payload."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    total_active_time_ms: NonNegativeInt = Field(alias="totalActiveTimeMs")
    longest_continuous_ms: NonNegativeInt = Field(alias="longestContinuousMs")
    max_concurrent_sessions: NonNegativeInt = Field(alias="maxConcurrentSessions")
    session_count: NonNegativeInt = Field(alias="sessionCount")


class GraphMeta(BaseModel):
    """Payload provenance: generation timestamp + tokscale version."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    generated_at: datetime = Field(alias="generatedAt")
    version: Identifier

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

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

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
