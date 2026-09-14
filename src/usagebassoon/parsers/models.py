# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""models.py — Parser for `tokscale models <...>`.

Ref Command: `tokscale models --json --group-by client,session,model --merge-worktrees`
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from usagebassoon.json_types import JsonObject, JsonValue


class ModelStatsRow(BaseModel):
    """Normalized cumulative usage for one (client, session, model)."""

    model_config = ConfigDict(populate_by_name=True)

    client: str
    session_id: str = Field(alias="sessionId")
    model: str
    provider: str | None = None
    merged_clients: tuple[str, ...] = Field(default=(), alias="mergedClients")
    input_tokens: int = Field(default=0, alias="input")
    output_tokens: int = Field(default=0, alias="output")
    cache_read: int = Field(default=0, alias="cacheRead")
    cache_write: int = Field(default=0, alias="cacheWrite")
    reasoning: int = 0
    message_count: int = Field(default=0, alias="messageCount")
    cost_usd: float = Field(default=0.0, alias="cost")
    ms_per_1k_tokens: float | None = None
    perf_duration_ms: int | None = None
    perf_token_coverage: float | None = None

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
        row = cls.model_validate(entry)
        return row.model_copy(
            update={
                "ms_per_1k_tokens": perf.get("msPer1KTokens"),
                "perf_duration_ms": perf.get("totalDurationMs"),
                "perf_token_coverage": perf.get("tokenCoverage"),
            }
        )


class ModelsPayload(BaseModel):
    """Complete models payload including reported grand totals."""

    model_config = ConfigDict(populate_by_name=True)

    group_by: str = Field(alias="groupBy")
    entries: list[ModelStatsRow]
    total_input: int = Field(alias="totalInput")
    total_output: int = Field(alias="totalOutput")
    total_cache_read: int = Field(alias="totalCacheRead")
    total_cache_write: int = Field(alias="totalCacheWrite")
    total_messages: int = Field(alias="totalMessages")
    total_cost: float = Field(alias="totalCost")
    processing_time_ms: int | None = Field(default=None, alias="processingTimeMs")


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
