# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_report_render.py — CLI coverage for shared report rendering."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from tests.test_cli_report import _configured_store
from usagebassoon.cli.app import app
from usagebassoon.cli.reports._common import ReportRecord


def _rows(payload: str) -> list[ReportRecord]:
    """Read the JSON row array produced by a report command."""
    return cast(list[ReportRecord], json.loads(payload))


@pytest.mark.parametrize("report", ["daily", "models", "sessions"])
def test_json_stdout_matches_saved_csv(report: str, tmp_path: Path) -> None:
    """Exercise each report's format wiring, numeric schema, and full identifiers."""
    runner = CliRunner()
    base = ["report", report, "--test"]
    result = runner.invoke(app, [*base, "--json", "--width", "10"])
    assert result.exit_code == 0, result.output
    expected = _rows(result.stdout)
    assert expected
    assert isinstance(expected[0]["total_tokens"], int)
    assert isinstance(expected[0]["cost_usd"], (int, float))
    destination = tmp_path / f"{report}.csv"
    result = runner.invoke(app, [*base, "--csv", "--save", str(destination)])
    assert result.exit_code == 0, result.output
    assert plain_cli_output(result.stdout) == ""
    actual = list(csv.DictReader(io.StringIO(destination.read_text())))
    assert len(actual) == len(expected)
    for row, exported in zip(expected, actual, strict=True):
        assert exported.keys() == row.keys()
        for column, value in row.items():
            assert exported[column] == ("" if value is None else str(value))
    if report == "sessions":
        assert all("…" not in str(row["session_id"]) for row in expected)
        assert any(", " in row["model"] for row in actual)
        assert all(datetime.fromisoformat(row["last_active"]) for row in actual)
    if report == "models":
        for row in expected:
            duration = row["perf_duration_ms"]
            tokens = row["perf_timed_tokens"]
            assert isinstance(duration, (int, float))
            assert isinstance(tokens, int) and tokens > 0
            assert row["ms_per_1k_tokens"] == pytest.approx(duration * 1_000 / tokens)


@pytest.mark.parametrize("report", ["daily", "models", "sessions"])
def test_empty_rows_and_conflicting_formats(report: str) -> None:
    """Return valid empty JSON and a stable CSV header; reject conflicting flags."""
    runner = CliRunner()
    base = ["report", report, "--test", "--client", "nonexistent"]
    result = runner.invoke(app, [*base, "--json"])
    assert result.exit_code == 0, result.output
    assert _rows(result.stdout) == []
    result = runner.invoke(app, [*base, "--csv"])
    assert result.exit_code == 0, result.output
    reader = csv.DictReader(io.StringIO(result.stdout))
    assert reader.fieldnames is not None
    assert "cost_usd" in reader.fieldnames
    assert list(reader) == []
    result = runner.invoke(app, ["report", report, "--json", "--csv"])
    assert result.exit_code != 0
    assert "mutually exclusive" in plain_cli_output(result.output)


def test_session_output_preserves_grouping_limit_and_sanitization(
    tmp_path: Path,
) -> None:
    """Preserve session grouping, limits, and sharing controls in JSON output."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    runner = CliRunner()
    base = [
        "report",
        "sessions",
        "--config",
        str(config),
        "--source",
        "local",
        "--json",
    ]
    result = runner.invoke(app, [*base, "--limit", "1"])
    assert result.exit_code == 0, result.output
    rows = _rows(result.stdout)
    assert len(rows) == 1
    assert rows[0]["total_tokens"] == 278
    assert rows[0]["model"] == "gpt-mini, gpt-test"
    for flag in ("--sanitize", "--obfuscate"):
        result = runner.invoke(app, [*base, "--limit", "1", flag])
        assert result.exit_code == 0, result.output
        sanitized = _rows(result.stdout)[0]
        assert sanitized["session_id"] == "session-alpha"
        assert sanitized["total_tokens"] == rows[0]["total_tokens"]
        assert sanitized["cost_usd"] == rows[0]["cost_usd"]


@pytest.mark.parametrize("report", ["daily", "models", "sessions"])
def test_nullable_rates_and_decimal_costs(
    report: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve missing values and serialize decimal amounts as numbers."""
    records: list[ReportRecord] = [
        {"input_tokens": 0, "cache_read": 3, "total_tokens": 0, "cost_usd": None},
        {
            "input_tokens": 2,
            "cache_read": 3,
            "total_tokens": 10,
            "cost_usd": Decimal("0.123456789"),
            "perf_duration_ms": 15,
            "perf_timed_tokens": 5,
        },
    ]

    def sample(*_args: object, **_kwargs: object) -> list[ReportRecord]:
        """Supply nullable numeric records without a storage backend."""
        return records

    loader = {
        "daily": "sample_daily_usage",
        "models": "sample_model_usage",
        "sessions": "sample_session_usage",
    }[report]
    monkeypatch.setattr(f"usagebassoon.cli.reports.{report}.{loader}", sample)
    result = CliRunner().invoke(app, ["report", report, "--test", "--json"])
    assert result.exit_code == 0, result.output
    rows = _rows(result.stdout)
    assert rows[0]["cost_usd"] is None
    assert rows[0]["cache_multiplier"] is None
    assert rows[0]["cost_per_million"] is None
    assert rows[1]["cost_usd"] == 0.123456789
    assert rows[1]["cache_multiplier"] == 1.5
    if report == "models":
        assert rows[0]["ms_per_1k_tokens"] is None
        assert rows[1]["ms_per_1k_tokens"] == 3000
