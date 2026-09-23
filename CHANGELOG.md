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

- Added the installable `bassoon` CLI and `usagebassoon` Python API for collecting, querying, and exporting persistent agent token-usage history.
- Added `tokscale` collection of daily per-session and per-model usage, activity, session metadata, observed pricing, and collector-host metadata, with versioned payload contracts and schema-drift reporting.
- Added an Arrow-based `StorageBackend` protocol for local DuckDB, MotherDuck, and BigQuery, with dialect-specific schemas and views and idempotent transactional upserts that preserve history.
- Added terminal summary, daily, session, and graph reports; bounded read-only SQL queries; CSV, JSON, and Parquet exports; and Python results as pandas, Polars, or Arrow.
- Added source-scoped workspace, client, and session tags and notes, ingest audits, reconciliation checks, and diagnostics.
- Added portable Parquet snapshots and restore for local and Google Cloud Storage archives, with catalog-based publication, integrity checks, retention, and optional collection-triggered cadence.
- Added scheduled collection workers for macOS, Debian-based Linux, and container environments, with status, log controls, per-operation timeouts, and retries.
- Added sharing controls: reports are raw by default with `--sanitize`, exports pseudonymize session, workspace, tag, and host fields and redact notes, `doctor` output is sanitized by default, and raw queries warn on stderr.
