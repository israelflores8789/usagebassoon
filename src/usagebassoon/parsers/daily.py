# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""daily.py — Parser for date-filtered `tokscale models` output."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from usagebassoon.json_types import JsonValue
from usagebassoon.parsers.models import ModelStatsRow, parse_models


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
    """

    day: date
    entries: tuple[DailyModelStatsRow, ...]


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
    )
