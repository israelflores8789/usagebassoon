# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""report.py — Parser for `tokscale report --json --no-summarize`.

Design-doc rule: only stable structural fields enter the curated layer.
tokscale-generated summary fields (title, task_category, task_group,
description, complexity, summarized_at, fm_version) are dropped at this
boundary; they remain recoverable from raw_exports.
"""

from __future__ import annotations

from datetime import UTC, datetime
from math import isfinite
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from usagebassoon.json_types import JsonValue
from usagebassoon.parsers._validation import (
    Identifier,
    Metadata,
    NonNegativeFloat,
    NonNegativeInt,
)


class SessionRow(BaseModel):
    """Stable session metadata; LLM-summary fields intentionally excluded."""

    model_config = ConfigDict(strict=True, extra="ignore", populate_by_name=True)

    client: Identifier
    session_id: Identifier
    workspace: Metadata | None = None
    workspace_label: Metadata | None = None
    created_at: datetime | None = None
    last_active: datetime | None = None
    duration_minutes: NonNegativeInt | None = None
    total_input_tokens: NonNegativeInt = 0
    total_output_tokens: NonNegativeInt = 0
    total_cache_read: NonNegativeInt | None = 0
    message_count: NonNegativeInt = 0
    tokscale_cost_usd: NonNegativeFloat = Field(default=0.0, alias="total_cost")
    models_used: tuple[Identifier, ...] = ()

    @field_validator("created_at", "last_active", mode="before")
    @classmethod
    def _epoch_millis(cls, value: Any) -> datetime | None:
        """Convert epoch milliseconds or ISO strings to UTC datetimes.

        Args:
            value: Raw created_at / last_active value.

        Returns:
            A timezone-aware datetime, or None when absent.

        Raises:
            ValueError: For unsupported timestamp representations.
        """
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError("epoch timestamp must not be bool")
        if isinstance(value, (int, float)):
            if not isfinite(value):
                raise ValueError("epoch timestamp must be finite")
            return datetime.fromtimestamp(value / 1000, tz=UTC)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        raise ValueError(f"unsupported timestamp: {value!r}")

    @field_validator("models_used", mode="before")
    @classmethod
    def _models_tuple(cls, value: Any) -> tuple[str, ...]:
        """Coerce a nullable model list into a tuple.

        Args:
            value: The raw models_used value.

        Returns:
            A tuple of model ids, empty when null.
        """
        if value is None:
            return ()
        if not isinstance(value, list) or not all(
            isinstance(item, str) for item in value
        ):
            raise ValueError("models_used must be null or a list of strings")
        return tuple(value)


def parse_report(payload: JsonValue) -> list[SessionRow]:
    """Parse the report JSON array into validated session rows.

    Args:
        payload: Decoded stdout from `tokscale report --json --no-summarize`.

    Returns:
        Validated rows, one per (client, session_id).

    Raises:
        ValueError: If the payload is not an array or contains duplicate
            session keys.
        pydantic.ValidationError: If any row fails validation.
    """
    if not isinstance(payload, list):
        raise ValueError("report payload must be a JSON array")
    rows: list[SessionRow] = []
    seen: set[tuple[str, str]] = set()
    for row in payload:
        session = SessionRow.model_validate(row)
        key = (session.client, session.session_id)
        if key in seen:
            raise ValueError(f"duplicate report session key: {key}")
        seen.add(key)
        rows.append(session)
    return rows


def make_session_label(row: SessionRow) -> str:
    """Build the deterministic human-facing session label.

    Args:
        row: A validated session row.

    Returns:
        A label of the form '{workspace} · {date} · {short id}'.

    Notes:
        Codex ids are 'rollout-<timestamp>-<uuid>'; the uuid carries the
        identity, so the label uses the first uuid group. OpenCode ids are
        random base-62; the 12-char prefix distinguishes all fixture sessions.
    """
    label = row.workspace_label or "session"
    day = row.created_at.date().isoformat() if row.created_at else "unknown-date"
    sid = row.session_id
    # parts: rollout, YYYY, MM, DDTHH, MI, SS, then uuid groups
    parts = sid.split("-")
    short = parts[6] if sid.startswith("rollout-") and len(parts) > 6 else sid[:12]
    return f"{label} · {day} · {short}"
