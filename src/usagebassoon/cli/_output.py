# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_output.py — Shared Rich console construction for CLI-owned presentation."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal, TextIO

import typer
import yaml
from rich.console import Console

type ColorSystem = Literal["auto", "standard", "256", "truecolor", "windows"] | None


def output_console(
    *,
    stderr: bool = False,
    file: TextIO | None = None,
    width: int | None = None,
    record: bool = False,
    force_terminal: bool | None = None,
    color_system: ColorSystem = "auto",
) -> Console:
    """Create a Rich console with UsageBassoon's safe output defaults.

    Args:
        stderr: Write to standard error rather than standard output.
        file: Optional explicit text stream.
        width: Optional terminal width override.
        record: Retain rendering for plain-text export or visual tests.
        force_terminal: Override terminal auto-detection when explicitly needed.
        color_system: Color capability, or ``None`` to disable color.

    Returns:
        A console that disables markup interpretation and automatic highlighting.
    """
    return Console(
        stderr=stderr,
        file=file,
        width=width,
        record=record,
        force_terminal=force_terminal,
        color_system=color_system,
        markup=False,
        highlight=False,
    )


def write_output(payload: str, *, save: Path | None) -> None:
    """Write a completed report payload to a UTF-8 file or unstyled stdout."""
    if save is None:
        typer.echo(payload, nl=False)
    else:
        save.write_text(payload, encoding="utf-8")


def _json_default(value: object) -> str | float:
    """Serialize dates as ISO text and decimal amounts as JSON numbers."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Unsupported report value: {type(value).__name__}")


def render_records(
    rows: Sequence[Mapping[str, object]],
    *,
    columns: Sequence[str],
    format: Literal["json", "csv", "yaml"],
    save: Path | None,
    json_payload: Mapping[str, object] | None = None,
) -> None:
    """Serialize report-owned rows using shared JSON/CSV and destination handling.

    Args:
        rows: Prepared records, including any requested obfuscation.
        columns: Stable CSV columns, including for empty results.
        format: Explicit JSON or CSV selection.
        save: Optional output path; otherwise write to stdout.
        json_payload: Optional report-specific JSON envelope instead of a row array.
    """
    if format == "yaml":
        normalized = json.loads(
            json.dumps(
                rows if json_payload is None else dict(json_payload),
                default=_json_default,
                allow_nan=False,
            )
        )
        payload = yaml.safe_dump(normalized, sort_keys=False, allow_unicode=True)
    elif format == "json":
        payload = (
            json.dumps(
                list(rows) if json_payload is None else dict(json_payload),
                default=_json_default,
                allow_nan=False,
                indent=2,
            )
            + "\n"
        )
    else:
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(
            {
                column: value.isoformat()
                if isinstance(value, (date, datetime))
                else value
                for column, value in row.items()
            }
            for row in rows
        )
        payload = stream.getvalue()
    write_output(payload, save=save)
