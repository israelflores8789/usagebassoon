# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""daily.py — Parsers for date-filtered `tokscale models` output."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from usagebassoon.json_types import JsonObject, JsonValue
from usagebassoon.parsers._validation import (
    Identifier,
    Metadata,
    NonNegativeFloat,
    NonNegativeInt,
    UnitInterval,
)


class ModelStatsRow(BaseModel):
    """Normalized cumulative usage for one (client, session, model)."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    client: Identifier
    session_id: Identifier = Field(alias="sessionId")
    model: Identifier
    provider: Metadata | None = None
    merged_clients: tuple[Identifier, ...] = Field(default=(), alias="mergedClients")
    input_tokens: NonNegativeInt = Field(default=0, alias="input")
    output_tokens: NonNegativeInt = Field(default=0, alias="output")
    cache_read: NonNegativeInt = Field(default=0, alias="cacheRead")
    cache_write: NonNegativeInt = Field(default=0, alias="cacheWrite")
    reasoning: NonNegativeInt = 0
    message_count: NonNegativeInt = Field(default=0, alias="messageCount")
    tokscale_cost_usd: NonNegativeFloat = Field(default=0.0, alias="cost")
    tokscale_ms_per_1k_tokens: NonNegativeFloat | None = None
    perf_duration_ms: NonNegativeInt | None = None
    perf_timed_tokens: NonNegativeInt | None = None
    perf_sample_count: NonNegativeInt | None = None
    perf_token_coverage: UnitInterval | None = None

    @field_validator("merged_clients", mode="before")
    @classmethod
    def _merged_tuple(cls, value: Any) -> tuple[str, ...]:
        """Normalize tokscale's nullable mergedClients field.

        Args:
            value: The raw mergedClients value (null or list of strings).

        Returns:
            A tuple of merged client ids, empty when null.

        Raises:
            ValueError: If the value is neither null nor a string list.
        """
        if value is None:
            return ()
        if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
            raise ValueError("mergedClients must be null or a list of strings")
        return tuple(value)

    @classmethod
    def from_entry(cls, entry: JsonObject) -> ModelStatsRow:
        """Build a row from one raw models entry, flattening performance.

        Args:
            entry: One element of the payload's entries array.

        Returns:
            The validated row with nested performance fields promoted.
        """
        performance = entry.get("performance")
        perf = performance if isinstance(performance, dict) else {}
        return cls.model_validate(
            {
                **entry,
                "tokscale_ms_per_1k_tokens": perf.get("msPer1KTokens"),
                "perf_duration_ms": perf.get("totalDurationMs"),
                "perf_timed_tokens": perf.get("timedTokens"),
                "perf_sample_count": perf.get("sampleCount"),
                "perf_token_coverage": perf.get("tokenCoverage"),
            }
        )


class ModelsPayload(BaseModel):
    """Complete models payload including reported grand totals."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    group_by: Identifier = Field(alias="groupBy")
    entries: list[ModelStatsRow]
    total_input: NonNegativeInt = Field(alias="totalInput")
    total_output: NonNegativeInt = Field(alias="totalOutput")
    total_cache_read: NonNegativeInt = Field(alias="totalCacheRead")
    total_cache_write: NonNegativeInt = Field(alias="totalCacheWrite")
    total_messages: NonNegativeInt = Field(alias="totalMessages")
    total_cost: NonNegativeFloat = Field(alias="totalCost")
    processing_time_ms: NonNegativeInt | None = Field(
        default=None,
        alias="processingTimeMs",
    )


def parse_models(payload: JsonValue) -> ModelsPayload:
    """Parse the models JSON object into validated rows.

    Args:
        payload: Decoded stdout from `tokscale models --json ...`.

    Returns:
        The validated payload including all per-session-model entries.

    Raises:
        ValueError: If the payload is not a JSON object.
        pydantic.ValidationError: If any field fails validation.
    """
    if not isinstance(payload, dict):
        raise ValueError("models payload must be a JSON object")
    entries_value = payload.get("entries", [])
    if not isinstance(entries_value, list):
        raise ValueError("models entries must be a JSON array")
    entries: list[ModelStatsRow] = []
    for entry in entries_value:
        if not isinstance(entry, dict):
            raise ValueError("models entries must contain JSON objects")
        entries.append(ModelStatsRow.from_entry(entry))
    body = {k: v for k, v in payload.items() if k != "entries"}
    return ModelsPayload.model_validate({**body, "entries": entries})


@dataclass(frozen=True, slots=True)
class DailyModelStatsRow:
    """One session and model's usage assigned to a requested UTC day.

    Attributes:
        day: Requested date passed to tokscale.
        stats: Validated tokscale session and model statistics.
    """

    day: date
    stats: ModelStatsRow


@dataclass(frozen=True, slots=True)
class DailyModelsPayload:
    """Date-filtered model statistics from one tokscale invocation.

    Attributes:
        day: Requested date passed to tokscale.
        entries: Per-session and model statistics associated with that date.
        totals: The response's own aggregate values for consistency checks.
    """

    day: date
    entries: tuple[DailyModelStatsRow, ...]
    totals: ModelsPayload


def parse_daily(payload: JsonValue, *, day: date) -> DailyModelsPayload:
    """Parse date-filtered models output and attach its requested day.

    Args:
        payload: Decoded stdout from the date-filtered models command.
        day: Requested UTC day supplied to tokscale.

    Returns:
        Validated daily session and model statistics.
    """
    models = parse_models(payload)
    return DailyModelsPayload(
        day=day,
        entries=tuple(DailyModelStatsRow(day=day, stats=row) for row in models.entries),
        totals=models,
    )
