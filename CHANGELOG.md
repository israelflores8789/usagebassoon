<!--
SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
SPDX-License-Identifier: AGPL-3.0-only
-->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added pipx-installable `bassoon` CLI token usage history collection, persistence, querying, reporting, and exporting.
- Added the `usagebassoon` Python API for querying token usage history into `pandas` and `polars` (optional) dataframes and Arrow tables.
- Added support for `tokscale` v4.15.1.
- Added `tokscale` collection of daily per-session and per-model usage, session metadata, observed pricing, and collector-host metadata.
- Added ephemeral append-only schema-drift observations and resolution events with replay-safe identities and deduplicated observation counts.
- Added an Arrow-based `StorageBackend` protocol with dialect-specific schemas and views and backend-specific idempotent publication that preserves token usage history.
- Added support for local DuckDB databases using the `StorageBackend` protocol.
- Added support for BigQuery datasets with concurrent append publication, immediately queryable canonical views, and nightly transactional compaction.
- Added support for MotherDuck databases for remote persistence using the `StorageBackend` protocol.
- Added terminal summary, model, daily, session, and graph reports; bounded read-only SQL queries; CSV, JSON, and Parquet exports; and Python results as pandas, Polars, or Arrow.
- Added workspace, client, model, tag, source, and inclusive date filters to daily and model reports, session last-active date filters, and a session creation-date display and filter mode; date bounds combine with all other report filters.
- Added the Daily Activity calendar report with total-token, component, cost, cost-per-million, and agent session-time metrics; shared report filters; linear/log scaling; 3–10 intensity colors.
- Added solid, terminal-themed Daily Activity colors blended with the terminal background, bounded theme discovery, and `--use-ascii` to force character shading.
- Added JSON and CSV output to daily, model, activity, and session reports with existing filters, grouping, limits, optional obfuscation, full identifiers, and numeric costs and derived rates.
- Added persisted daily model timing components, derived milliseconds-per-thousand-token rates, and reconciliation against tokscale's reported rate.
- Added global workspace, client, and session tags with mutation provenance and source-scoped session notes for user-curated reports.
- Added permanent collection audits, models payload reconciliation, and diagnostics using append-only outcomes and issue events.
- Added portable Parquet snapshots and restore for local and remote object store archives, with consistent warehouse capture, catalog-based publication, integrity checks, retention, optional collection-triggered cadence, and restoration into initialized empty destinations across supported backends.
- Added support for Google Cloud Storage for snapshot archives.
- Added scheduled token usage history collection for Linux (systemd), macOS (launchd), and container environments (worker script) with status, log controls, per-operation timeouts, and retries.
- Added sharing controls: reports are raw by default with `--sanitize`, exports pseudonymize session, workspace, tag, and host fields and redact notes, `doctor` diagnostics output is sanitized by default, and raw queries warn on stderr.
- Added comprehensive unit test suite that ensures consistent behavior across backends and object stores.
- Added a live persistence test suite against all implemented remote backends and object stores including BigQuery, MotherDuck, and Google Cloud Storage. Tests are automated in the CI Live GitHub workflow.
- Added SQL parity test suite that ensures structural and synthetic replay behavior against all SQL dialects using SQLGlot.
- Added explicit, idempotent warehouse initialization and schema-compatibility preflight checks.
- Added local collector exclusion, replay-safe remote publication, and bounded persistence retries for concurrent ephemeral environments.
- Added configurable yaspin indicators for interactive CLI waits, with Pong by default, 64 randomized messages, and terminal theme colors.
