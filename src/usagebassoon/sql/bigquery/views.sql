-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- Current-state compatibility views.
--
-- The base fact tables contain one row per natural key because collection
-- upserts existing facts and inserts only new facts.

CREATE OR REPLACE VIEW sessions_current AS
SELECT * FROM sessions;

CREATE OR REPLACE VIEW session_model_stats_current AS
SELECT * FROM session_model_stats;

CREATE OR REPLACE VIEW daily_stats_current AS
SELECT * FROM daily_stats;

CREATE OR REPLACE VIEW daily_activity_current AS
SELECT * FROM daily_activity;
