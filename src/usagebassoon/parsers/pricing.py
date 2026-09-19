# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""pricing.py — Parser for `tokscale pricing <model> --json`."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from usagebassoon.json_types import JsonValue
from usagebassoon.parsers._validation import (
    Identifier,
    Metadata,
    NonNegativeFloat,
    NonNegativeInt,
)


class PricingResolution(BaseModel):
    """How tokscale selected a pricing record for a model."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    kind: Identifier
    candidate_count: NonNegativeInt = Field(alias="candidateCount")
    price_consensus: bool = Field(alias="priceConsensus")
    exact_model_identity: bool = Field(alias="exactModelIdentity")
    alias_applied: bool = Field(alias="aliasApplied")
    normalized: bool
    stripped: bool
    submission_safe: bool = Field(alias="submissionSafe")


class PricingRates(BaseModel):
    """Per-token rates; cache-write may be absent for some models."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    input_cost_per_token: NonNegativeFloat | None = Field(
        default=None,
        alias="inputCostPerToken",
    )
    output_cost_per_token: NonNegativeFloat | None = Field(
        default=None,
        alias="outputCostPerToken",
    )
    cache_read_input_token_cost: NonNegativeFloat | None = Field(
        default=None,
        alias="cacheReadInputTokenCost",
    )
    cache_write_input_token_cost: NonNegativeFloat | None = Field(
        default=None, alias="cacheWriteInputTokenCost"
    )


class PricingRow(BaseModel):
    """A tokscale-resolved rate card plus its match-quality metadata."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    model_id: Identifier = Field(alias="modelId")
    matched_key: Metadata | None = Field(default=None, alias="matchedKey")
    source: Identifier
    resolution: PricingResolution
    pricing: PricingRates


def parse_pricing(payload: JsonValue) -> PricingRow:
    """Parse pricing JSON into a validated rate card.

    Args:
        payload: Decoded stdout from `tokscale pricing <model> --json`.

    Returns:
        The validated rate card with resolution provenance.

    Raises:
        ValueError: If the payload is not a JSON object.
        pydantic.ValidationError: If any field fails validation.
    """
    if not isinstance(payload, dict):
        raise ValueError("pricing payload must be a JSON object")
    return PricingRow.model_validate(payload)
