# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""schema_assets.py — Ordered packaged SQL assets for schema initialization."""

from __future__ import annotations

# Migrations are intentionally tested but excluded from runtime initialization.
# Enabling them for user databases requires the deliberate one-line addition of
# "migrations.sql" between DDL and views below.
RUNTIME_SCHEMA_ASSETS = ("ddl.sql", "views.sql")

# Parity tests cover every packaged asset, including dormant migrations.
PARITY_SCHEMA_ASSETS = ("ddl.sql", "migrations.sql", "views.sql")
