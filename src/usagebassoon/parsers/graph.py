# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""graph.py — Parser for `tokscale graph` JSON on stdout."""

from __future__ import annotations

from datetime import date
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


class GraphMeta(BaseModel):
    """Payload provenance used to identify the tokscale version."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    version: Identifier


class GraphPayload(BaseModel):
    """Graph payload fields used for daily activity collection."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    meta: GraphMeta
    contributions: list[Contribution]


def parse_graph(payload: JsonValue) -> GraphPayload:
    """Parse graph JSON into validated daily activity facts.

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
