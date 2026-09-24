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
- Added `tokscale` collection of daily per-session and per-model usage, activity, session metadata, observed pricing, and collector-host metadata, with versioned payload contracts and schema-drift reporting.
- Added an Arrow-based `StorageBackend` protocol with dialect-specific schemas and views and idempotent transactional upserts that preserve token usage history.
- Added support for local DuckDB databases using the `StorageBackend` protocol.
- Added support for BigQuery datasets for remote persistence using the `StorageBackend` protocol.
- Added support for MotherDuck databases for remote persistent using the `StorageBackend` protocol.
- Added terminal summary, daily, session, and graph reports; bounded read-only SQL queries; CSV, JSON, and Parquet exports; and Python results as pandas, Polars, or Arrow.
- Added source-scoped workspace, client, and session tags and notes for user-curated reports.
- Added ingest audits, models payload reconciliation, and diagnostics. Repeated detections update issues by source, check, and issue key while retaining first and latest detection metadata.
- Added portable Parquet snapshots and restore for local and remote object store archives, with catalog-based publication, integrity checks, retention, and optional collection-triggered cadence.
- Added support for Google Cloud Storage for snapshot archives.
- Added scheuduled token usage history collection for Linux (systemd), macOS (launchd), and container environments (worker script) with status, log controls, per-operation timeouts, and retries.
- Added sharing controls: reports are raw by default with `--sanitize`, exports pseudonymize session, workspace, tag, and host fields and redact notes, `doctor` diagnostics output is sanitized by default, and raw queries warn on stderr.
