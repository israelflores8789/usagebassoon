# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_collector_daily.py — Daily collection candidate and retry tests."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path
from sys import executable
from typing import cast

import pytest

from usagebassoon import collector as subprocess_collector
from usagebassoon import orchestrator as collector
from usagebassoon.collector import RawCollection
from usagebassoon.config import LoggingConfig, UsageBassoonConfig
from usagebassoon.ingest import CollectionBundle, IngestStatus, IngestTarget
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.logger import LOG_DIRECTORY_ENV_VAR
from usagebassoon.normalizer import NormalizedBundle
from usagebassoon.persistence import PersistSummary

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
    prefix = collector._prefix(configuration)

    assert prefix == expected_prefix


def test_daily_models_command_uses_each_candidate_day(
    monkeypatch: pytest.MonkeyPatch,
    daily_raws: dict[date, JsonObject],
) -> None:
    """Request date-filtered models facts with the canonical tokscale arguments."""
    day = min(daily_raws)
    calls: list[tuple[str, ...]] = []

    def command(
        _configuration: UsageBassoonConfig,
        _prefix: object,
        *arguments: str,
        **_kwargs: object,
    ) -> JsonValue:
        """Record one generated tokscale invocation and return its fixture."""
        calls.append(arguments)
        return daily_raws[day]

    monkeypatch.setattr(subprocess_collector, "_json_command", command)

    payloads = subprocess_collector._fetch_daily_models(
        _config(Path("config.toml")),
        ["tokscale"],
        [day],
    )

    assert payloads == {day: daily_raws[day]}
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


def test_tokscale_child_environment_excludes_unrelated_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass only documented and explicitly opted-in environment variables."""
    monkeypatch.setenv("PATH", "/test/bin")
    monkeypatch.setenv("LANG", "C.UTF-8")
    monkeypatch.setenv("TOKSCALE_EXTRA_DIRS", "/agent-storage")
    monkeypatch.setenv("CUSTOM_TOKSCALE_AUTH", "allowed")
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "secret")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/secret.json")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "secret")
    configuration = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=SOURCE_ID,
        backend="duckdb",
        database=":memory:",
        tokscale_env=("CUSTOM_TOKSCALE_AUTH",),
    )

    child = subprocess_collector._child_environment(configuration)

    assert child["PATH"] == "/test/bin"
    assert child["LANG"] == "C.UTF-8"
    assert child["TOKSCALE_EXTRA_DIRS"] == "/agent-storage"
    assert child["CUSTOM_TOKSCALE_AUTH"] == "allowed"
    assert "MOTHERDUCK_TOKEN" not in child
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in child
    assert "AWS_ACCESS_KEY_ID" not in child


def test_tokscale_timeout_kills_the_process(tmp_path: Path) -> None:
    """Terminate a subprocess that exceeds the configured hard timeout."""
    configuration = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=SOURCE_ID,
        backend="duckdb",
        database=":memory:",
        tokscale_timeout_seconds=0.05,
    )

    with pytest.raises(RuntimeError, match="exceeded 0 seconds and was killed"):
        collector._json_command(
            configuration,
            [executable, "-c", "import time; time.sleep(30)"],
            "graph",
        )


def test_tokscale_stdout_limit_kills_the_process(tmp_path: Path) -> None:
    """Reject excessive graph output before it can exhaust collector memory."""
    configuration = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=SOURCE_ID,
        backend="duckdb",
        database=":memory:",
        tokscale_max_stdout_bytes=128,
    )

    with pytest.raises(RuntimeError, match="stdout exceeded its 128 byte limit"):
        collector._json_command(
            configuration,
            [executable, "-c", "import sys; sys.stdout.write('x' * 4096)"],
            "graph",
            max_stdout_bytes=128,
        )


def test_tokscale_stderr_limit_kills_the_process(tmp_path: Path) -> None:
    """Reject excessive tokscale diagnostics before retaining unbounded text."""
    configuration = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=SOURCE_ID,
        backend="duckdb",
        database=":memory:",
        tokscale_max_stderr_bytes=128,
    )

    with pytest.raises(RuntimeError, match="stderr exceeded its 128 byte limit"):
        collector._json_command(
            configuration,
            [executable, "-c", "import sys; sys.stderr.write('x' * 4096)"],
            "graph",
        )


def test_graph_candidates_skip_completed_statuses_and_refresh_today(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    graph_raw: JsonObject,
    report_raws: dict[date, JsonArray],
    daily_raws: dict[date, JsonObject],
) -> None:
    """Use graph dates, skip completed history, and refresh the current day."""
    days = tuple(sorted(daily_raws))
    completed_day, *_, current_day = days
    captured: list[RawCollection] = []
    requested_models: list[tuple[date, ...]] = []
    requested_prices: list[dict[date, set[str]]] = []

    def command(
        _configuration: UsageBassoonConfig,
        _prefix: object,
        *arguments: str,
        **_kwargs: object,
    ) -> JsonValue:
        """Supply graph and report payloads while fetch helpers handle daily calls."""
        if arguments == ("graph",):
            return graph_raw
        if (
            len(arguments) == 7
            and arguments[:4] == ("report", "--json", "--no-summarize", "--since")
            and arguments[5] == "--until"
            and arguments[4] == arguments[6]
        ):
            return report_raws[date.fromisoformat(arguments[4])]
        raise AssertionError(f"unexpected tokscale command: {arguments}")

    def daily_models(
        _configuration: UsageBassoonConfig,
        _prefix: object,
        selected_days: tuple[date, ...],
    ) -> dict[date, JsonObject]:
        """Return all selected mandatory daily facts."""
        requested_models.append(selected_days)
        return {day: daily_raws[day] for day in selected_days}

    def pricing(
        _configuration: UsageBassoonConfig,
        _prefix: object,
        models_by_day: dict[date, set[str]],
        _logger: logging.Logger,
    ) -> tuple[dict[date, dict[str, JsonObject]], dict[date, frozenset[str]]]:
        """Record model-day pricing requests and complete the successful targets."""
        requested_prices.append(models_by_day)
        return ({day: {} for day in models_by_day}, {})

    def build(raw: RawCollection, **_kwargs: object) -> object:
        """Capture the raw bundle before normalization and persistence."""
        captured.append(raw)
        return object()

    def prefix(_configuration: UsageBassoonConfig) -> list[str]:
        """Return a deterministic tokscale executable for this unit test."""
        return ["tokscale"]

    def ingest_status(
        _configuration: UsageBassoonConfig,
    ) -> tuple[
        dict[IngestTarget, IngestStatus],
        dict[date, set[str]],
        dict[date, set[str]],
    ]:
        """Return one completed historical day and one refreshable current day."""
        return (
            {
                (completed_day, "models"): IngestStatus(
                    completed_day, "models", "complete", 1, 1, "old", "old"
                ),
                (completed_day, "pricing"): IngestStatus(
                    completed_day, "pricing", "complete", 1, 1, "old", "old"
                ),
                (current_day, "models"): IngestStatus(
                    current_day, "models", "complete", 1, 1, "old", "old"
                ),
                (current_day, "pricing"): IngestStatus(
                    current_day, "pricing", "complete", 1, 1, "old", "old"
                ),
            },
            {},
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
    monkeypatch.setattr(subprocess_collector, "_json_command", command)
    monkeypatch.setattr(collector, "load_ingest_status", ingest_status)
    monkeypatch.setattr(collector, "_fetch_daily_models", daily_models)
    monkeypatch.setattr(collector, "_fetch_pricing", pricing)
    monkeypatch.setattr(collector, "build_collection_bundle", build)
    monkeypatch.setattr(collector, "normalize", normalized)
    monkeypatch.setattr(collector, "persist_with_retries", persist)

    _, summary = collector.collect(_config(tmp_path / "config.toml"))

    expected_successes = set(days) - {completed_day}
    assert summary == PersistSummary(0, 0, {})
    assert requested_models == [tuple(day for day in days if day != completed_day)]
    assert set(requested_prices[0]) == expected_successes
    assert captured[0].graph is graph_raw
    assert set(captured[0].daily_models) == expected_successes
    assert set(captured[0].report_by_day) == set(report_raws)


def test_daily_models_failure_aborts_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat a required daily models failure as a failed collection run."""

    def command(*_args: object, **_kwargs: object) -> JsonValue:
        """Raise the kind of process-start failure a scheduled run can see."""
        raise OSError("tokscale executable unavailable")

    monkeypatch.setattr(subprocess_collector, "_json_command", command)

    with pytest.raises(OSError, match="tokscale executable unavailable"):
        subprocess_collector._fetch_daily_models(
            _config(Path("config.toml")),
            ["tokscale"],
            [date(2026, 9, 10)],
        )


def test_report_failure_is_logged_and_does_not_abort_collection(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep token collection usable when optional session metadata is unavailable."""
    logger = logging.getLogger("usagebassoon.test.report-failure")

    def command(*_args: object, **_kwargs: object) -> JsonValue:
        """Raise a transient report command failure."""
        raise RuntimeError("report process failed")

    monkeypatch.setattr(subprocess_collector, "_json_command", command)

    with caplog.at_level(logging.ERROR, logger=logger.name):
        reports, failures = subprocess_collector._fetch_reports(
            _config(Path("config.toml")),
            ["tokscale"],
            [date(2026, 9, 10)],
            logger=logger,
        )

    assert reports == {}
    assert failures == frozenset({date(2026, 9, 10)})
    assert "session report collection failed" in caplog.text


def test_graph_failure_aborts_cycle_and_logs_to_operational_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail the run when its required graph command is unavailable."""
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
    configuration = _config(tmp_path / "config.toml")

    def command(*_args: object, **_kwargs: object) -> JsonValue:
        """Raise a transient graph command failure."""
        raise RuntimeError("graph process failed")

    monkeypatch.setattr(collector, "_json_command", command)

    with pytest.raises(RuntimeError, match="graph process failed"):
        collector.collect(configuration)

    log = (tmp_path / "logs" / "usagebassoon.log").read_text()
    assert "collection cycle failed before completion" in log


def test_unexpected_graph_failure_aborts_cycle_and_logs_to_operational_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate unexpected required graph failures after recording context."""
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
    configuration = _config(tmp_path / "config.toml")

    def command(*_args: object, **_kwargs: object) -> JsonValue:
        """Raise an exception outside the normal subprocess error family."""
        raise TypeError("unexpected graph process failure")

    monkeypatch.setattr(collector, "_json_command", command)

    with pytest.raises(TypeError, match="unexpected graph process failure"):
        collector.collect(configuration)

    log = (tmp_path / "logs" / "usagebassoon.log").read_text()
    assert "collection cycle failed before completion" in log
