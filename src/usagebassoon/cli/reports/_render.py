# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_render.py — Shared text, JSON, and CSV report rendering and file output."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

import typer
from rich import box
from rich.table import Table

from usagebassoon.cli._output import output_console, write_output

type ReportColumn = tuple[str, Literal["left", "right"]]
type OutputFormat = Literal["text", "json", "csv"]
type DataFormat = Literal["json", "csv"]

_RAW_REPORT_WARNING = (
    "Sharing this report? Re-run with --sanitize to obfuscate identifiers "
    "and free text."
)


def output_format(json_output: bool, csv_output: bool) -> OutputFormat:
    """Resolve report flags to an explicit format, rejecting conflicting selections."""
    if json_output and csv_output:
        raise typer.BadParameter("--json and --csv are mutually exclusive")
    if json_output:
        return "json"
    return "csv" if csv_output else "text"


def render_table(
    title: str,
    columns: Sequence[ReportColumn],
    rows: Sequence[Mapping[str, str]],
    *,
    width: int | None,
    save: Path | None,
    sanitize: bool,
    column_widths: Mapping[str, int] | None = None,
) -> None:
    """Render one bounded Rich report table and optionally save its plain text.

    Args:
        title: Human-readable table title.
        columns: Header and alignment pairs, where alignment is ``left`` or ``right``.
        rows: Already formatted display rows.
        width: Maximum terminal width, or ``None`` for unbounded output.
        save: Optional destination for captured plain text.
        sanitize: Whether identifiers have been intentionally obfuscated.
        column_widths: Optional explicit widths for preformatted column contents.
    """
    render_tables(
        ((title, columns, rows),),
        width=width,
        save=save,
        sanitize=sanitize,
        column_widths=column_widths,
    )


def render_tables(
    tables: Sequence[tuple[str, Sequence[ReportColumn], Sequence[Mapping[str, str]]]],
    *,
    width: int | None,
    save: Path | None,
    sanitize: bool,
    column_widths: Mapping[str, int] | None = None,
) -> None:
    """Render related report tables through one console and one saved artifact.

    Args:
        tables: Titles, column specifications, and formatted display rows.
        width: Maximum terminal width, or ``None`` for unbounded output.
        save: Optional destination for captured plain text.
        sanitize: Whether identifiers have been intentionally obfuscated.
        column_widths: Optional explicit widths for preformatted column contents.
    """
    console = output_console(
        width=width if width is not None else 10_000,
        record=save is not None,
    )
    for title, columns, rows in tables:
        table = Table(
            title=title,
            width=width if column_widths is not None else None,
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
                width=column_widths[header] if column_widths is not None else None,
                min_width=(
                    column_widths[header]
                    if column_widths is not None
                    else protected_width
                ),
                max_width=column_widths[header] if column_widths is not None else None,
                no_wrap=True,
                overflow="ellipsis",
            )
        if rows:
            for row in rows:
                table.add_row(*(row[header] for header, _ in columns))
        elif column_widths is None:
            table.add_row("No matching usage data.", *("" for _ in columns[1:]))
        console.print(table)
        if not rows and column_widths is not None:
            console.print("No matching usage data.")
    if save is None and not sanitize:
        console.print(_RAW_REPORT_WARNING, style="yellow")
    if save is not None:
        write_output(console.export_text(), save=save)


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
        write_output(text, save=save)
