# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_validation.py — Shared strict constraints for tokscale-derived payloads."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

MAX_IDENTIFIER_LENGTH = 1_024
MAX_METADATA_LENGTH = 4_096

type Identifier = Annotated[
    str,
    Field(min_length=1, max_length=MAX_IDENTIFIER_LENGTH),
]
type Metadata = Annotated[str, Field(max_length=MAX_METADATA_LENGTH)]
type NonNegativeInt = Annotated[int, Field(ge=0)]
type NonNegativeFloat = Annotated[float, Field(ge=0, allow_inf_nan=False)]
type UnitInterval = Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)]
