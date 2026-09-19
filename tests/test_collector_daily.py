# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_collector_daily.py — Daily collection candidate and retry tests."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path
from subprocess import CompletedProcess
from typing import cast

import pytest

from usagebassoon import collector
from usagebassoon.config import LoggingConfig, UsageBassoonConfig
from usagebassoon.ingest import RawCollection
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.merge import PersistSummary
from usagebassoon.normalizer import CollectionBundle, NormalizedBundle, ProcessingTarget

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


class _FixedDatetime:
    """Provide one candidate day as the current UTC day for collection tests."""

    @classmethod
    def now(cls, tz: object | None = None) -> datetime:
        """Return the selected current day as an aware UTC timestamp."""
        del cls, tz
        return datetime(2026, 9, 10, tzinfo=UTC)


def _config(path: Path) -> UsageBassoonConfig:
    """Create the minimal local configuration consumed by collector helpers."""
    return UsageBassoonConfig(
        path=path,
        source_id=SOURCE_ID,
        backend="duckdb",
        database=":memory:",
        logging=LoggingConfig(directory=path.parent / "logs"),
    )


@pytest.mark.parametrize(
    ("command", "expected_prefix"),
    [
        ("npx tokscale@latest", ["npx", "tokscale@latest"]),
        ("bunx tokscale@latest", ["bunx", "tokscale@latest"]),
        ("deno x npm:tokscale@latest", ["deno", "x", "npm:tokscale@latest"]),
    ],
)
def test_package_runner_commands_are_passed_to_tokscale(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    expected_prefix: list[str],
) -> None:
    """Pass configured package-runner prefixes and tokscale args to subprocess."""
    configuration = UsageBassoonConfig(
        path=Path("config.toml"),
        source_id=SOURCE_ID,
        backend="duckdb",
        database=":memory:",
        tokscale_bin=command,
    )
    monkeypatch.setenv("TOKSCALE_BIN", "ignored-environment-override")
    invocations: list[list[str]] = []

    def run(arguments: list[str], **_kwargs: object) -> CompletedProcess[str]:
        """Capture the executable argument vector and return valid JSON."""
        invocations.append(arguments)
        return CompletedProcess(arguments, 0, stdout="{}", stderr="")

    monkeypatch.setattr(collector.subprocess, "run", run)

    prefix = collector._prefix(configuration)
    payload = collector._json_command(prefix, "graph", "--json")

    assert prefix == expected_prefix
    assert payload == {}
    assert invocations == [[*expected_prefix, "graph", "--json"]]


def test_daily_models_command_uses_each_candidate_day(
    monkeypatch: pytest.MonkeyPatch,
    daily_raws: dict[date, JsonObject],
) -> None:
    """Request date-filtered models facts with the canonical tokscale arguments."""
    day = min(daily_raws)
    calls: list[tuple[str, ...]] = []

    def command(_prefix: object, *arguments: str) -> JsonValue:
        """Record one generated tokscale invocation and return its fixture."""
        calls.append(arguments)
        return daily_raws[day]

    monkeypatch.setattr(collector, "_json_command", command)

    payloads, failed = collector._fetch_daily_models(
        ["tokscale"], [day], logging.getLogger("usagebassoon-test")
    )

    assert payloads == {day: daily_raws[day]}
    assert failed == set()
    assert calls == [
        (
            "models",
            "--json",
            "--group-by",
            "client,session,model",
            "--since",
            day.isoformat(),
            "--until",
            day.isoformat(),
        )
    ]


def test_graph_candidates_skip_completed_targets_and_leave_failures_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    graph_raw: JsonObject,
    report_raw: JsonArray,
    daily_raws: dict[date, JsonObject],
) -> None:
    """Use graph dates, refresh today, and only mark successfully fetched targets."""
    days = tuple(sorted(daily_raws))
    completed_day, failed_day, *_, current_day = days
    captured: list[RawCollection] = []
    requested_models: list[tuple[date, ...]] = []
    requested_prices: list[dict[date, set[str]]] = []

    def command(_prefix: object, *arguments: str) -> JsonValue:
        """Supply graph and report payloads while fetch helpers handle daily calls."""
        if arguments == ("graph",):
            return graph_raw
        if arguments == ("report", "--json", "--no-summarize"):
            return report_raw
        raise AssertionError(f"unexpected tokscale command: {arguments}")

    def daily_models(
        _prefix: object,
        selected_days: tuple[date, ...],
        _logger: logging.Logger,
    ) -> tuple[dict[date, JsonObject], set[tuple[date, str]]]:
        """Return all selected daily facts except one failed candidate day."""
        requested_models.append(selected_days)
        return (
            {day: daily_raws[day] for day in selected_days if day != failed_day},
            {(failed_day, "daily_stats")},
        )

    def pricing(
        _prefix: object,
        models_by_day: dict[date, set[str]],
        _logger: logging.Logger,
    ) -> tuple[dict[date, dict[str, JsonObject]], set[tuple[date, str]]]:
        """Record model-day pricing requests and complete the successful targets."""
        requested_prices.append(models_by_day)
        return ({day: {} for day in models_by_day}, set())

    def build(raw: RawCollection, **_kwargs: object) -> object:
        """Capture the raw bundle before normalization and persistence."""
        captured.append(raw)
        return object()

    def prefix(_configuration: UsageBassoonConfig) -> list[str]:
        """Return a deterministic tokscale executable for this unit test."""
        return ["tokscale"]

    def daily_state(
        _configuration: UsageBassoonConfig,
    ) -> tuple[frozenset[ProcessingTarget], dict[date, set[str]]]:
        """Return one completed historical day and one refreshable current day."""
        return (
            frozenset(
                {
                    (completed_day, "daily_stats"),
                    (completed_day, "price_versions"),
                    (current_day, "daily_stats"),
                    (current_day, "price_versions"),
                }
            ),
            {},
        )

    def normalized(_bundle: CollectionBundle) -> NormalizedBundle:
        """Return an opaque normalized value because persistence is replaced."""
        return cast(NormalizedBundle, object())

    def persist(
        _configuration: UsageBassoonConfig,
        _bundle: NormalizedBundle,
        _logger: logging.Logger,
    ) -> PersistSummary:
        """Return the persistence outcome used for collection assertions."""
        return PersistSummary(0, 0, {})

    monkeypatch.setattr(collector, "datetime", _FixedDatetime)
    monkeypatch.setattr(collector, "_prefix", prefix)
    monkeypatch.setattr(collector, "_json_command", command)
    monkeypatch.setattr(
        collector,
        "_daily_state",
        daily_state,
    )
    monkeypatch.setattr(collector, "_fetch_daily_models", daily_models)
    monkeypatch.setattr(collector, "_fetch_pricing", pricing)
    monkeypatch.setattr(collector, "build_collection_bundle", build)
    monkeypatch.setattr(collector, "normalize", normalized)
    monkeypatch.setattr(collector, "_persist_with_retries", persist)

    _, summary = collector.collect(_config(tmp_path / "config.toml"))

    expected_successes = set(days) - {completed_day, failed_day}
    assert summary == PersistSummary(0, 0, {})
    assert requested_models == [tuple(day for day in days if day != completed_day)]
    assert set(requested_prices[0]) == expected_successes
    assert captured[0].graph is graph_raw
    assert set(captured[0].daily_models) == expected_successes
    assert captured[0].processed_targets == frozenset(
        (day, target)
        for day in expected_successes
        for target in ("daily_stats", "price_versions")
    )
    assert captured[0].failed_targets == frozenset(
        {
            (failed_day, "daily_stats"),
            (failed_day, "price_versions"),
        }
    )
