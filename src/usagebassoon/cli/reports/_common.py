# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_common.py — Private shared report filters, queries, formatting, and samples."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from functools import cache
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

import pyarrow as pa
import typer
from rich import box
from rich.table import Table

from usagebassoon.backends.base import StorageBackend, close_backend
from usagebassoon.cli._output import output_console
from usagebassoon.cli._utils import configured_backend
from usagebassoon.display import sanitize_display
from usagebassoon.json_types import JsonValue
from usagebassoon.parsers.daily import parse_daily
from usagebassoon.parsers.report import parse_report
from usagebassoon.privacy import sanitize_table

type ReportRecord = dict[str, object]
type ReportColumn = tuple[str, Literal["left", "right"]]

SAMPLE_LOCAL_SOURCE_ID = "11111111-1111-4111-8111-111111111111"
SAMPLE_LATEST_DAY = date(2026, 9, 10)
_MULTIPLICATION_SIGN = "\N{MULTIPLICATION SIGN}"
CACHE_MULTIPLIER_HEADER = f"Cache {_MULTIPLICATION_SIGN}"
_RAW_REPORT_WARNING = (
    "Sharing this report? Re-run with --sanitize to obfuscate identifiers "
    "and free text."
)


@dataclass(frozen=True, slots=True)
class ReportFilters:
    """Optional filters shared by all terminal reports.

    Attributes:
        source: Source UUID or the ``local`` sentinel before resolution.
        client: Exact client filter.
        model: Exact model filter.
        workspace: Exact workspace filter.
        tag: Effective source-aware curation tag filter.
    """

    source: str | None = None
    client: str | None = None
    model: str | None = None
    workspace: str | None = None
    tag: str | None = None


@dataclass(frozen=True, slots=True)
class _SampleUsage:
    """One deterministic usage fact used by report demonstration mode."""

    source_id: str
    day: date
    client: str
    session_id: str
    workspace: str
    model: str
    input_tokens: int
    output_tokens: int
    cache_read: int
    cache_write: int
    reasoning: int
    total_tokens: int
    cost_usd: float | None
    perf_duration_ms: int | None
    perf_timed_tokens: int | None
    created_at: datetime | None
    last_active: datetime
    tags: tuple[str, ...]


def resolve_filters(filters: ReportFilters, local_source_id: str) -> ReportFilters:
    """Resolve and validate the optional report source filter.

    Args:
        filters: Unresolved CLI filters.
        local_source_id: Configured or deterministic local source UUID.

    Returns:
        Filters with a canonical UUID source identifier when requested.

    Raises:
        typer.BadParameter: If the source value is neither ``local`` nor a UUID.
    """
    if filters.source is None:
        return filters
    if filters.source.lower() == "local":
        return replace(filters, source=local_source_id)
    try:
        return replace(filters, source=str(UUID(filters.source)))
    except ValueError as error:
        raise typer.BadParameter(
            "--source must be a UUID or 'local'", param_hint="--source"
        ) from error


def parse_report_dates(
    since: str | None, until: str | None
) -> tuple[date | None, date | None]:
    """Parse optional inclusive day bounds for a terminal report.

    Args:
        since: Lower day bound in YYYY-MM-DD form.
        until: Upper day bound in YYYY-MM-DD form.

    Returns:
        Parsed lower and upper bounds, with omitted bounds left open.

    Raises:
        typer.BadParameter: A bound is malformed or the range is reversed.
    """

    def parse(value: str | None, option: str) -> date | None:
        """Parse one optional day bound and name its CLI option on failure."""
        if value is None:
            return None
        try:
            parsed = date.fromisoformat(value)
        except ValueError as error:
            raise typer.BadParameter(
                f"{option} must use YYYY-MM-DD", param_hint=option
            ) from error
        if parsed.isoformat() != value:
            raise typer.BadParameter(f"{option} must use YYYY-MM-DD", param_hint=option)
        return parsed

    start = parse(since, "--since")
    end = parse(until, "--until")
    if start is not None and end is not None and start > end:
        raise typer.BadParameter("--since must be on or before --until")
    return start, end


def report_where(
    filters: ReportFilters,
    *,
    since: date | None = None,
    until: date | None = None,
    alias: str = "facts",
    date_column: Literal["day", "last_active", "created_at"] = "day",
) -> tuple[str, dict[str, str]]:
    """Build dialect-neutral report predicates and bound string parameters.

    The tag predicate uses a semijoin against ``session_tags`` so duplicate tag
    assignments at different scopes cannot duplicate usage facts.

    Args:
        filters: Resolved report filters.
        since: Inclusive lower day bound, when applicable.
        until: Inclusive upper day bound, when applicable.
        alias: SQL table alias for the report source view.
        date_column: Source date or timestamp field to constrain.

    Returns:
        SQL ``WHERE`` fragment and its named bindings.
    """
    conditions: list[str] = []
    parameters: dict[str, str] = {}
    for name, value in (
        ("source", filters.source),
        ("client", filters.client),
        ("model", filters.model),
        ("workspace", filters.workspace),
    ):
        if value is not None:
            column = "source_id" if name == "source" else name
            conditions.append(f"{alias}.{column} = :{name}")
            parameters[name] = value
    if filters.tag is not None:
        conditions.append(
            "EXISTS ("
            "SELECT 1 FROM session_tags AS tagged "
            f"WHERE tagged.source_id = {alias}.source_id "
            f"AND tagged.client = {alias}.client "
            f"AND tagged.session_id = {alias}.session_id "
            "AND tagged.tag = :tag"
            ")"
        )
        parameters["tag"] = filters.tag
    day_expression = (
        f"{alias}.day"
        if date_column == "day"
        else f"CAST({alias}.{date_column} AS DATE)"
    )
    if since is not None:
        conditions.append(f"{day_expression} >= CAST(:since AS DATE)")
        parameters["since"] = since.isoformat()
    if until is not None:
        conditions.append(f"{day_expression} <= CAST(:until AS DATE)")
        parameters["until"] = until.isoformat()
    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    return where, parameters


def load_daily_usage(
    backend: StorageBackend,
    filters: ReportFilters,
    *,
    limit: int | None = None,
    since: date | None = None,
    until: date | None = None,
) -> list[ReportRecord]:
    """Load daily report rows from the dialect-paired report source view.

    Args:
        backend: Initialized storage backend.
        filters: Resolved report filters.
        limit: Maximum rows after newest-first ordering, when requested.
        since: Inclusive lower day bound.
        until: Inclusive upper day bound.

    Returns:
        One aggregated record per usage day, newest first.
    """
    where, parameters = report_where(filters, since=since, until=until)
    limit_sql = "" if limit is None else f" LIMIT {limit}"
    result = backend.query(
        "SELECT "
        "facts.day AS day, "
        "SUM(COALESCE(facts.input_tokens, 0)) AS input_tokens, "
        "SUM(COALESCE(facts.output_tokens, 0)) AS raw_output_tokens, "
        "SUM(COALESCE(facts.reasoning, 0)) AS reasoning_tokens, "
        "SUM(COALESCE(facts.output_tokens, 0)) "
        "+ SUM(COALESCE(facts.reasoning, 0)) AS output_tokens, "
        "SUM(COALESCE(facts.cache_read, 0)) AS cache_read, "
        "SUM(COALESCE(facts.cache_write, 0)) AS cache_write, "
        "SUM(COALESCE(facts.total_tokens, 0)) AS total_tokens, "
        "CASE WHEN COUNT(facts.cost_usd) = COUNT(*) "
        "THEN SUM(facts.cost_usd) "
        "ELSE SUM(facts.tokscale_cost_usd) END AS cost_usd "
        "FROM report_daily_usage AS facts"
        f"{where} "
        "GROUP BY facts.day "
        "ORDER BY facts.day DESC"
        f"{limit_sql}",
        parameters,
    )
    return [dict(record) for record in result.to_pylist()]


def load_session_usage(
    backend: StorageBackend,
    filters: ReportFilters,
    *,
    by_model: bool,
    by_created_at: bool = False,
    limit: int | None = None,
    since: date | None = None,
    until: date | None = None,
) -> list[ReportRecord]:
    """Load session report rows from the dialect-paired report source view.

    Args:
        backend: Initialized storage backend.
        filters: Resolved report filters.
        by_model: Keep one row per session/model instead of one session row.
        by_created_at: Filter and order by session creation time.
        limit: Maximum rows after timestamp ordering, when requested.
        since: Inclusive lower date bound on the selected timestamp.
        until: Inclusive upper date bound on the selected timestamp.

    Returns:
        Aggregated session report records.
    """
    timestamp = "created_at" if by_created_at else "last_active"
    where, parameters = report_where(
        filters, since=since, until=until, date_column=timestamp
    )
    limit_sql = "" if limit is None else f" LIMIT {limit}"
    model_select = (
        "facts.model AS model, "
        "facts.input_tokens AS input_tokens, "
        "facts.output_tokens AS raw_output_tokens, "
        "facts.reasoning AS reasoning_tokens, "
        "COALESCE(facts.output_tokens, 0) + COALESCE(facts.reasoning, 0) "
        "AS output_tokens, "
        "facts.cache_read AS cache_read, "
        "facts.cache_write AS cache_write, "
        "facts.total_tokens AS total_tokens, "
        "COALESCE(facts.cost_usd, facts.tokscale_cost_usd) AS cost_usd, "
        "facts.last_active AS last_active, "
        "facts.created_at AS created_at "
        "FROM report_session_models AS facts"
        f"{where} "
        f"ORDER BY facts.{timestamp} DESC NULLS LAST, facts.client, "
        "facts.session_id, facts.model"
    )
    session_select = (
        "STRING_AGG(facts.model, ', ' ORDER BY facts.model) AS model, "
        "SUM(COALESCE(facts.input_tokens, 0)) AS input_tokens, "
        "SUM(COALESCE(facts.output_tokens, 0)) AS raw_output_tokens, "
        "SUM(COALESCE(facts.reasoning, 0)) AS reasoning_tokens, "
        "SUM(COALESCE(facts.output_tokens, 0)) "
        "+ SUM(COALESCE(facts.reasoning, 0)) AS output_tokens, "
        "SUM(COALESCE(facts.cache_read, 0)) AS cache_read, "
        "SUM(COALESCE(facts.cache_write, 0)) AS cache_write, "
        "SUM(COALESCE(facts.total_tokens, 0)) AS total_tokens, "
        "CASE WHEN COUNT(facts.cost_usd) = COUNT(*) "
        "THEN SUM(facts.cost_usd) "
        "ELSE SUM(facts.tokscale_cost_usd) END AS cost_usd, "
        "MAX(facts.last_active) AS last_active, "
        "MAX(facts.created_at) AS created_at "
        "FROM report_session_models AS facts"
        f"{where} "
        "GROUP BY facts.source_id, facts.client, facts.session_id "
        f"ORDER BY {timestamp} DESC NULLS LAST, facts.client, facts.session_id"
    )
    select = model_select if by_model else session_select
    sql = f"SELECT facts.source_id, facts.client, facts.session_id, {select}{limit_sql}"
    result = backend.query(sql, parameters)
    return [dict(record) for record in result.to_pylist()]


def load_model_usage(
    backend: StorageBackend,
    filters: ReportFilters,
    *,
    since: date | None = None,
    until: date | None = None,
) -> list[ReportRecord]:
    """Aggregate filtered daily model facts by model and client."""
    where, parameters = report_where(filters, since=since, until=until)
    result = backend.query(
        "SELECT "
        "facts.model AS model, "
        "facts.client AS client, "
        "SUM(COALESCE(facts.input_tokens, 0)) AS input_tokens, "
        "SUM(COALESCE(facts.output_tokens, 0) + COALESCE(facts.reasoning, 0)) "
        "AS output_tokens, "
        "SUM(COALESCE(facts.cache_read, 0)) AS cache_read, "
        "SUM(COALESCE(facts.cache_write, 0)) AS cache_write, "
        "SUM(COALESCE(facts.total_tokens, 0)) AS total_tokens, "
        "SUM(CASE WHEN facts.perf_duration_ms IS NOT NULL "
        "AND facts.perf_timed_tokens > 0 "
        "THEN facts.perf_duration_ms END) AS perf_duration_ms, "
        "SUM(CASE WHEN facts.perf_duration_ms IS NOT NULL "
        "AND facts.perf_timed_tokens > 0 "
        "THEN facts.perf_timed_tokens END) AS perf_timed_tokens, "
        "CASE WHEN COUNT(facts.cost_usd) = COUNT(*) "
        "THEN SUM(facts.cost_usd) ELSE SUM(facts.tokscale_cost_usd) "
        "END AS cost_usd "
        "FROM report_models AS facts"
        f"{where} "
        "GROUP BY facts.model, facts.client "
        "ORDER BY total_tokens DESC, facts.model, facts.client",
        parameters,
    )
    return [dict(record) for record in result.to_pylist()]


def load_configured_daily_usage(
    config: Path | None,
    filters: ReportFilters,
    *,
    limit: int | None = None,
    since: date | None = None,
    until: date | None = None,
) -> list[ReportRecord]:
    """Open the configured warehouse and load daily report records."""
    configuration, backend = configured_backend(config)
    try:
        return load_daily_usage(
            backend,
            resolve_filters(filters, configuration.source_id),
            limit=limit,
            since=since,
            until=until,
        )
    finally:
        close_backend(backend, context="rendering a daily report")


def load_configured_model_usage(
    config: Path | None,
    filters: ReportFilters,
    *,
    since: date | None = None,
    until: date | None = None,
) -> list[ReportRecord]:
    """Open the configured warehouse and load filtered model totals."""
    configuration, backend = configured_backend(config)
    try:
        return load_model_usage(
            backend,
            resolve_filters(filters, configuration.source_id),
            since=since,
            until=until,
        )
    finally:
        close_backend(backend, context="rendering a models report")


def load_configured_session_usage(
    config: Path | None,
    filters: ReportFilters,
    *,
    by_model: bool,
    by_created_at: bool = False,
    limit: int | None = None,
    since: date | None = None,
    until: date | None = None,
) -> list[ReportRecord]:
    """Open the configured warehouse and load session report records."""
    configuration, backend = configured_backend(config)
    try:
        return load_session_usage(
            backend,
            resolve_filters(filters, configuration.source_id),
            by_model=by_model,
            by_created_at=by_created_at,
            limit=limit,
            since=since,
            until=until,
        )
    finally:
        close_backend(backend, context="rendering a sessions report")


def sample_daily_usage(
    filters: ReportFilters,
    *,
    limit: int | None = None,
    since: date | None = None,
    until: date | None = None,
) -> list[ReportRecord]:
    """Return deterministic, filtered daily rows without opening a backend."""
    grouped: dict[date, list[_SampleUsage]] = {}
    for fact in _sample_usage(filters, since=since, until=until):
        grouped.setdefault(fact.day, []).append(fact)
    records = [_daily_record(day, facts) for day, facts in grouped.items()]
    records.sort(key=_daily_sort_key, reverse=True)
    return records if limit is None else records[:limit]


def sample_session_usage(
    filters: ReportFilters,
    *,
    by_model: bool,
    by_created_at: bool = False,
    limit: int | None = None,
    since: date | None = None,
    until: date | None = None,
) -> list[ReportRecord]:
    """Return deterministic, filtered session rows without opening a backend."""
    grouped: dict[tuple[str, str, str, str | None], list[_SampleUsage]] = {}
    for fact in _sample_usage(filters):
        key = (
            fact.source_id,
            fact.client,
            fact.session_id,
            fact.model if by_model else None,
        )
        grouped.setdefault(key, []).append(fact)
    records = [
        _session_record(source_id, client, session_id, facts)
        for (source_id, client, session_id, _), facts in grouped.items()
    ]
    timestamp = "created_at" if by_created_at else "last_active"
    if since is not None or until is not None:
        bounded: list[ReportRecord] = []
        for record in records:
            value = record[timestamp]
            if value is None:
                continue
            day = _as_datetime(value).date()
            if (since is None or day >= since) and (until is None or day <= until):
                bounded.append(record)
        records = bounded

    def sort_key(record: ReportRecord) -> tuple[datetime, str, str, str]:
        """Sort sample sessions by the selected timestamp and identity."""
        return _session_sort_key(record, timestamp)

    records.sort(key=sort_key, reverse=True)
    return records if limit is None else records[:limit]


def sample_model_usage(
    filters: ReportFilters, *, since: date | None = None, until: date | None = None
) -> list[ReportRecord]:
    """Aggregate filtered golden daily facts by model and client."""
    grouped: dict[tuple[str, str], list[_SampleUsage]] = {}
    for fact in _sample_usage(filters, since=since, until=until):
        grouped.setdefault((fact.model, fact.client), []).append(fact)
    records = [
        _model_record(model, client, facts)
        for (model, client), facts in grouped.items()
    ]
    records.sort(key=_model_sort_key)
    return records


def _model_sort_key(record: ReportRecord) -> tuple[int, str, str]:
    """Sort sample model rows by descending tokens and stable identifiers."""
    return (
        -integer_value(record["total_tokens"]),
        str(record["model"]),
        str(record["client"]),
    )


def parse_width(value: str) -> int | None:
    """Parse a positive report width or the ``max`` no-truncation sentinel."""
    if value.lower() == "max":
        return None
    try:
        width = int(value)
    except ValueError as error:
        raise typer.BadParameter(
            "--width must be a positive integer or 'max'", param_hint="--width"
        ) from error
    if width < 1:
        raise typer.BadParameter(
            "--width must be a positive integer or 'max'", param_hint="--width"
        )
    return width


def sanitize_records(records: list[ReportRecord], sanitize: bool) -> list[ReportRecord]:
    """Apply export-style pseudonymization to report records when requested."""
    if not sanitize or not records:
        return records
    table = pa.Table.from_pylist(records)
    return [dict(record) for record in sanitize_table(table).to_pylist()]


def format_tokens(value: object) -> str:
    """Format one token count with a one-decimal SI-style unit suffix."""
    amount = numeric_value(value)
    if amount is None:
        return "—"
    for divisor, suffix in ((1_000_000_000_000, "T"), (1_000_000, "M"), (1_000, "K")):
        if abs(amount) >= divisor:
            return f"{amount / divisor:.1f}{suffix}"
    return f"{amount:.1f}"


def format_cost(value: object, *, precision: int = 2) -> str:
    """Format one optional USD amount without treating missing pricing as zero.

    Args:
        value: Nullable USD amount.
        precision: Digits after the decimal point.

    Returns:
        Formatted USD amount or an em dash for incomplete pricing.
    """
    amount = numeric_value(value)
    return "—" if amount is None else f"${amount:,.{precision}f}"


def format_cost_per_million(
    cost: object, total_tokens: object, *, precision: int = 3
) -> str:
    """Format an optional USD-per-million-total-tokens rate.

    Args:
        cost: Nullable USD amount.
        total_tokens: Total token denominator.
        precision: Digits after the decimal point.

    Returns:
        Formatted rate or an em dash for an undefined rate.
    """
    amount = numeric_value(cost)
    total = integer_value(total_tokens)
    if amount is None or total == 0:
        return "—"
    return f"${amount * 1_000_000 / total:,.{precision}f}"


def format_ms_per_1k_tokens(
    duration_ms: object, timed_tokens: object, *, precision: int = 0
) -> str:
    """Format milliseconds per thousand timed tokens from additive components."""
    duration = numeric_value(duration_ms)
    tokens = integer_value(timed_tokens)
    if duration is None or tokens <= 0:
        return "—"
    return f"{duration * 1_000 / tokens:,.{precision}f}"


def format_cache_multiplier(cache_read: object, input_tokens: object) -> str:
    """Format cache-read divided by input as a Unicode multiplier."""
    input_total = integer_value(input_tokens)
    if input_total == 0:
        return "—"
    return f"{integer_value(cache_read) / input_total:,.2f}{_MULTIPLICATION_SIGN}"


def format_timestamp(value: object, *, compact: bool = False) -> str:
    """Format one optional session timestamp without seconds or timezone clutter.

    Args:
        value: Backend timestamp value, when present.
        compact: Omit the year for bounded-width report tables.

    Returns:
        A readable local timestamp fragment or an em dash when absent.
    """
    if value is None:
        return "—"
    pattern = "%m-%d %H:%M" if compact else "%Y-%m-%d %H:%M"
    return _as_datetime(value).strftime(pattern)


def truncate_middle(value: object, width: int | None, maximum: int) -> str:
    """Truncate one display value in the middle when bounded report width applies."""
    text = sanitize_display(value)
    if width is None or len(text) <= maximum:
        return text
    prefix = max(1, (maximum - 1) // 2)
    suffix = max(1, maximum - prefix - 1)
    return f"{text[:prefix]}…{text[-suffix:]}"


def render_table(
    title: str,
    columns: Sequence[ReportColumn],
    rows: Sequence[Mapping[str, str]],
    *,
    width: int | None,
    save: Path | None,
    sanitize: bool,
) -> None:
    """Render one bounded Rich report table and optionally save its plain text.

    Args:
        title: Human-readable table title.
        columns: Header and alignment pairs, where alignment is ``left`` or ``right``.
        rows: Already formatted display rows.
        width: Maximum terminal width, or ``None`` for unbounded output.
        save: Optional destination for captured plain text.
        sanitize: Whether identifiers have been intentionally obfuscated.
    """
    render_tables(((title, columns, rows),), width=width, save=save, sanitize=sanitize)


def render_tables(
    tables: Sequence[tuple[str, Sequence[ReportColumn], Sequence[Mapping[str, str]]]],
    *,
    width: int | None,
    save: Path | None,
    sanitize: bool,
) -> None:
    """Render related report tables through one console and one saved artifact.

    Args:
        tables: Titles, column specifications, and formatted display rows.
        width: Maximum terminal width, or ``None`` for unbounded output.
        save: Optional destination for captured plain text.
        sanitize: Whether identifiers have been intentionally obfuscated.
    """
    console = output_console(
        width=width if width is not None else 10_000,
        record=save is not None,
    )
    for title, columns, rows in tables:
        table = Table(
            title=title,
            box=box.SIMPLE_HEAVY,
            pad_edge=False,
            padding=(0, 0),
            show_header=True,
        )
        for header, justify in columns:
            protected_width = (
                max((len(header), *(len(row[header]) for row in rows)))
                if width is None
                or (width >= 80 and (justify == "right" or header == "Model"))
                else None
            )
            table.add_column(
                header,
                justify=justify,
                min_width=protected_width,
                no_wrap=True,
                overflow="ellipsis",
            )
        if rows:
            for row in rows:
                table.add_row(*(row[header] for header, _ in columns))
        else:
            table.add_row("No matching usage data.", *("" for _ in columns[1:]))
        console.print(table)
    if save is None and not sanitize:
        console.print(_RAW_REPORT_WARNING, style="yellow")
    if save is not None:
        save.write_text(console.export_text(), encoding="utf-8")


def render_graph(
    text: str,
    *,
    save: Path | None,
    sanitize: bool,
) -> None:
    """Print one pre-rendered terminal graph and optionally save it as text."""
    console = output_console()
    console.print(text, end="")
    if save is None and not sanitize:
        console.print(_RAW_REPORT_WARNING, style="yellow")
    if save is not None:
        save.write_text(text, encoding="utf-8")


def _sample_usage(
    filters: ReportFilters, *, since: date | None = None, until: date | None = None
) -> list[_SampleUsage]:
    """Return filtered facts parsed from the packaged golden test fixtures."""
    return [
        fact
        for fact in _golden_usage()
        if _matches(fact, filters)
        and (since is None or fact.day >= since)
        and (until is None or fact.day <= until)
    ]


@cache
def _golden_usage() -> tuple[_SampleUsage, ...]:
    """Parse the sanitized golden fixtures into deterministic report test facts."""
    fixture_root = _golden_fixture_root()
    facts: list[_SampleUsage] = []
    for daily_file in _golden_daily_files(fixture_root):
        day = _fixture_day(daily_file.name)
        report_file = fixture_root.joinpath(
            daily_file.name.removesuffix(".daily.json") + ".report.json"
        )
        session_metadata = {
            (row.client, row.session_id): (
                row.workspace or "unknown",
                row.created_at,
                row.last_active or datetime(day.year, day.month, day.day, tzinfo=UTC),
            )
            for row in parse_report(_load_fixture_json(report_file))
        }
        for daily_row in parse_daily(_load_fixture_json(daily_file), day=day).entries:
            stats = daily_row.stats
            workspace, created_at, last_active = session_metadata.get(
                (stats.client, stats.session_id),
                ("unknown", None, datetime(day.year, day.month, day.day, tzinfo=UTC)),
            )
            facts.append(
                _SampleUsage(
                    source_id=SAMPLE_LOCAL_SOURCE_ID,
                    day=day,
                    client=stats.client,
                    session_id=stats.session_id,
                    workspace=workspace,
                    model=stats.model,
                    input_tokens=stats.input_tokens,
                    output_tokens=stats.output_tokens,
                    cache_read=stats.cache_read,
                    cache_write=stats.cache_write,
                    reasoning=stats.reasoning,
                    total_tokens=(
                        stats.input_tokens
                        + stats.output_tokens
                        + stats.cache_read
                        + stats.cache_write
                        + stats.reasoning
                    ),
                    cost_usd=stats.tokscale_cost_usd,
                    perf_duration_ms=stats.perf_duration_ms,
                    perf_timed_tokens=stats.perf_timed_tokens,
                    created_at=created_at,
                    last_active=last_active,
                    tags=("golden",),
                )
            )
    session_times: dict[tuple[str, str, str], tuple[datetime | None, datetime]] = {}
    for fact in facts:
        key = (fact.source_id, fact.client, fact.session_id)
        previous = session_times.get(key)
        if previous is None:
            session_times[key] = (fact.created_at, fact.last_active)
            continue
        created_at = previous[0]
        if fact.created_at is not None and (
            created_at is None or fact.created_at < created_at
        ):
            created_at = fact.created_at
        session_times[key] = (created_at, max(previous[1], fact.last_active))
    normalized: list[_SampleUsage] = []
    for fact in facts:
        created_at, last_active = session_times[
            (fact.source_id, fact.client, fact.session_id)
        ]
        normalized.append(replace(fact, created_at=created_at, last_active=last_active))
    return tuple(normalized)


def _golden_fixture_root() -> Path | Traversable:
    """Return packaged fixtures, or source-tree fixtures during local development."""
    packaged = files("usagebassoon.cli.reports").joinpath("_fixtures")
    if packaged.is_dir():
        return packaged
    return Path(__file__).parents[4] / "tests" / "fixtures"


def _golden_daily_files(root: Path | Traversable) -> list[Path | Traversable]:
    """Return golden daily fixture files in chronological filename order."""
    candidates: list[Path | Traversable] = [
        candidate
        for candidate in root.iterdir()
        if candidate.name.endswith(".daily.json")
    ]
    return sorted(candidates, key=_fixture_filename)


def _fixture_filename(candidate: Path | Traversable) -> str:
    """Return one fixture filename for chronological sorting."""
    return candidate.name


def _fixture_day(filename: str) -> date:
    """Extract the collection day from one golden fixture filename."""
    return date.fromisoformat(filename.removeprefix("golden-").split("-tokscale-")[0])


def _load_fixture_json(path: Path | Traversable) -> JsonValue:
    """Decode one packaged golden fixture as the project's recursive JSON type."""
    return cast(JsonValue, json.loads(path.read_text(encoding="utf-8")))


def _matches(fact: _SampleUsage, filters: ReportFilters) -> bool:
    """Return whether one sample usage fact matches all active filters."""
    return (
        (filters.source is None or fact.source_id == filters.source)
        and (filters.client is None or fact.client == filters.client)
        and (filters.model is None or fact.model == filters.model)
        and (filters.workspace is None or fact.workspace == filters.workspace)
        and (filters.tag is None or filters.tag in fact.tags)
    )


def _daily_record(day: date, facts: Sequence[_SampleUsage]) -> ReportRecord:
    """Aggregate sample facts into one daily report record."""
    return _usage_record({"day": day}, facts)


def _session_record(
    source_id: str,
    client: str,
    session_id: str,
    facts: Sequence[_SampleUsage],
) -> ReportRecord:
    """Aggregate sample facts into one session or session/model report record."""
    creation_times = [fact.created_at for fact in facts if fact.created_at is not None]
    record = _usage_record(
        {
            "source_id": source_id,
            "client": client,
            "session_id": session_id,
            "model": ", ".join(sorted({fact.model for fact in facts})),
            "created_at": min(creation_times) if creation_times else None,
            "last_active": max(fact.last_active for fact in facts),
        },
        facts,
    )
    return record


def _model_record(
    model: str, client: str, facts: Sequence[_SampleUsage]
) -> ReportRecord:
    """Aggregate sample usage and paired timing components for one model/client."""
    record = _usage_record({"model": model, "client": client}, facts)
    timed = [
        fact
        for fact in facts
        if fact.perf_duration_ms is not None
        and fact.perf_timed_tokens is not None
        and fact.perf_timed_tokens > 0
    ]
    record["perf_duration_ms"] = (
        sum(fact.perf_duration_ms or 0 for fact in timed) if timed else None
    )
    record["perf_timed_tokens"] = (
        sum(fact.perf_timed_tokens or 0 for fact in timed) if timed else None
    )
    return record


def _usage_record(base: ReportRecord, facts: Sequence[_SampleUsage]) -> ReportRecord:
    """Add normalized usage totals to one sample aggregation record."""
    return {
        **base,
        "input_tokens": sum(fact.input_tokens for fact in facts),
        "raw_output_tokens": sum(fact.output_tokens for fact in facts),
        "reasoning_tokens": sum(fact.reasoning for fact in facts),
        "output_tokens": sum(fact.output_tokens + fact.reasoning for fact in facts),
        "cache_read": sum(fact.cache_read for fact in facts),
        "cache_write": sum(fact.cache_write for fact in facts),
        "total_tokens": sum(fact.total_tokens for fact in facts),
        "cost_usd": (
            None
            if any(fact.cost_usd is None for fact in facts)
            else sum(fact.cost_usd or 0 for fact in facts)
        ),
    }


def _daily_sort_key(record: ReportRecord) -> date:
    """Return the descending sort value for one sample daily record."""
    return _as_date(record["day"])


def _session_sort_key(
    record: ReportRecord, timestamp: str
) -> tuple[datetime, str, str, str]:
    """Return the descending sort values for one sample session record."""
    value = record[timestamp]
    return (
        _as_datetime(value) if value is not None else datetime.min.replace(tzinfo=UTC),
        _as_text(record["client"]),
        _as_text(record["session_id"]),
        _as_text(record["model"]),
    )


def integer_value(value: object) -> int:
    """Coerce a known numeric report value to an integer."""
    if value is None:
        return 0
    if isinstance(value, (int, float, str, Decimal)):
        return int(value)
    raise TypeError(f"expected numeric report value, got {type(value)!r}")


def numeric_value(value: object) -> float | None:
    """Coerce a nullable numeric report value to a float."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, Decimal)):
        return float(value)
    raise TypeError(f"expected numeric report value, got {type(value)!r}")


def _as_date(value: object) -> date:
    """Return a report date value or fail loudly on an invalid backend result."""
    if isinstance(value, date):
        return value
    raise TypeError(f"expected date report value, got {type(value)!r}")


def _as_datetime(value: object) -> datetime:
    """Return a report timestamp value or fail loudly on an invalid backend result."""
    if isinstance(value, datetime):
        return value
    raise TypeError(f"expected datetime report value, got {type(value)!r}")


def _as_text(value: object) -> str:
    """Return a report text value, using an empty string for absent values."""
    return "" if value is None else str(value)
