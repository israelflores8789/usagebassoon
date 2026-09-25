-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- See sql/duckdb/views.sql for canonical comments on the purpose of each view.

-- Reasoning tokens use the output rate, as tokscale's pricing semantics do.
CREATE OR REPLACE VIEW daily_cost AS
SELECT
    daily_stats.*,
    CASE
        WHEN daily_stats.perf_timed_tokens > 0
            AND daily_stats.perf_duration_ms IS NOT NULL
        THEN 1000.0 * daily_stats.perf_duration_ms / daily_stats.perf_timed_tokens
    END AS ms_per_1k_tokens,
    CASE
        WHEN (daily_stats.input_tokens <> 0 AND price_versions.price_input_per_token IS NULL)
            OR ((daily_stats.output_tokens <> 0 OR daily_stats.reasoning <> 0)
                AND price_versions.price_output_per_token IS NULL)
            OR (daily_stats.cache_read <> 0 AND price_versions.price_cache_read_per_token IS NULL)
            OR (daily_stats.cache_write <> 0 AND price_versions.price_cache_write_per_token IS NULL)
        THEN NULL
        ELSE COALESCE(daily_stats.input_tokens, 0) * COALESCE(price_versions.price_input_per_token, 0)
            + (COALESCE(daily_stats.output_tokens, 0) + COALESCE(daily_stats.reasoning, 0))
                * COALESCE(price_versions.price_output_per_token, 0)
            + COALESCE(daily_stats.cache_read, 0) * COALESCE(price_versions.price_cache_read_per_token, 0)
            + COALESCE(daily_stats.cache_write, 0) * COALESCE(price_versions.price_cache_write_per_token, 0)
    END AS cost_usd
FROM daily_stats
LEFT JOIN price_versions
    ON price_versions.source_id = daily_stats.source_id
    AND price_versions.day = daily_stats.day
    AND price_versions.model = daily_stats.model;

CREATE OR REPLACE VIEW session_model_stats AS
SELECT
    totals.*,
    CASE
        WHEN totals.perf_timed_tokens > 0
        THEN 1000.0 * totals.perf_duration_ms / totals.perf_timed_tokens
    END AS ms_per_1k_tokens
FROM (
SELECT
    source_id,
    client,
    session_id,
    model,
    MAX(provider) AS provider,
    SUM(input_tokens) AS input_tokens,
    SUM(output_tokens) AS output_tokens,
    SUM(cache_read) AS cache_read,
    SUM(cache_write) AS cache_write,
    SUM(reasoning) AS reasoning,
    SUM(total_tokens) AS total_tokens,
    SUM(message_count) AS message_count,
    SUM(tokscale_cost_usd) AS tokscale_cost_usd,
    SUM(CASE
        WHEN perf_duration_ms IS NOT NULL AND perf_timed_tokens > 0
        THEN perf_duration_ms
    END) AS perf_duration_ms,
    SUM(CASE
        WHEN perf_duration_ms IS NOT NULL AND perf_timed_tokens > 0
        THEN perf_timed_tokens
    END) AS perf_timed_tokens,
    SUM(perf_sample_count) AS perf_sample_count,
    CASE WHEN COUNT(cost_usd) = COUNT(*) THEN SUM(cost_usd) END AS cost_usd,
    MAX(updated_at) AS updated_at
FROM daily_cost
GROUP BY source_id, client, session_id, model
) AS totals;

CREATE OR REPLACE VIEW session_model_stats_current AS
SELECT * FROM session_model_stats;

-- Report source views preserve filter dimensions; terminal commands aggregate
-- them after applying source, client, model, workspace, and effective-tag filters.
CREATE OR REPLACE VIEW report_daily_usage AS
SELECT
    daily_cost.source_id,
    daily_cost.day,
    daily_cost.client,
    daily_cost.session_id,
    daily_cost.model,
    sessions.workspace,
    daily_cost.input_tokens,
    daily_cost.output_tokens,
    daily_cost.cache_read,
    daily_cost.cache_write,
    daily_cost.reasoning,
    daily_cost.total_tokens,
    daily_cost.perf_duration_ms,
    daily_cost.perf_timed_tokens,
    daily_cost.perf_sample_count,
    daily_cost.perf_token_coverage,
    daily_cost.tokscale_ms_per_1k_tokens,
    daily_cost.ms_per_1k_tokens,
    daily_cost.cost_usd,
    daily_cost.tokscale_cost_usd
FROM daily_cost
LEFT JOIN sessions
    ON sessions.source_id = daily_cost.source_id
    AND sessions.client = daily_cost.client
    AND sessions.session_id = daily_cost.session_id;

CREATE OR REPLACE VIEW report_session_models AS
SELECT
    session_model_stats.source_id,
    session_model_stats.client,
    session_model_stats.session_id,
    session_model_stats.model,
    sessions.workspace,
    sessions.last_active,
    session_model_stats.input_tokens,
    session_model_stats.output_tokens,
    session_model_stats.cache_read,
    session_model_stats.cache_write,
    session_model_stats.reasoning,
    session_model_stats.total_tokens,
    session_model_stats.perf_duration_ms,
    session_model_stats.perf_timed_tokens,
    session_model_stats.perf_sample_count,
    session_model_stats.ms_per_1k_tokens,
    session_model_stats.cost_usd,
    session_model_stats.tokscale_cost_usd
FROM session_model_stats
LEFT JOIN sessions
    ON sessions.source_id = session_model_stats.source_id
    AND sessions.client = session_model_stats.client
    AND sessions.session_id = session_model_stats.session_id;

-- Retained public relations for the query API and doctor checks. The terminal
-- summary command now uses report_session_models directly.
CREATE OR REPLACE VIEW report_summary AS
SELECT
    COUNT(*) AS sessions,
    COALESCE(SUM(cost_usd), 0) AS cost_usd
FROM (
    SELECT
        source_id,
        client,
        session_id,
        CASE WHEN COUNT(cost_usd) = COUNT(*) THEN SUM(cost_usd) END AS cost_usd
    FROM report_session_models
    GROUP BY source_id, client, session_id
) AS session_costs;

CREATE OR REPLACE VIEW report_summary_models AS
SELECT
    model,
    COALESCE(SUM(total_tokens), 0) AS total_tokens,
    COALESCE(SUM(cost_usd), 0) AS cost_usd
FROM report_session_models
GROUP BY model
ORDER BY cost_usd DESC, model;

-- Model reports filter source facts before aggregating by client and model.
CREATE OR REPLACE VIEW report_models AS
SELECT
    source_id,
    day,
    client,
    session_id,
    model,
    workspace,
    input_tokens,
    output_tokens,
    cache_read,
    cache_write,
    reasoning,
    total_tokens,
    perf_duration_ms,
    perf_timed_tokens,
    perf_sample_count,
    perf_token_coverage,
    tokscale_ms_per_1k_tokens,
    ms_per_1k_tokens,
    cost_usd,
    tokscale_cost_usd
FROM report_daily_usage;

CREATE OR REPLACE VIEW session_tags AS
SELECT DISTINCT
    sessions.source_id,
    sessions.client,
    sessions.session_id,
    sessions.workspace,
    tags.tag,
    tags.scope AS tag_scope
FROM sessions
JOIN tags
    ON tags.source_id = sessions.source_id
    AND (
        (tags.scope = 'client' AND tags.client = sessions.client)
        OR (tags.scope = 'workspace' AND tags.workspace = sessions.workspace)
        OR (
            tags.scope = 'session'
            AND tags.client = sessions.client
            AND tags.session_id = sessions.session_id
        )
    );

CREATE OR REPLACE VIEW tagged_sessions AS
SELECT sessions.*, session_tags.tag, session_tags.tag_scope
FROM sessions
JOIN session_tags
    ON session_tags.source_id = sessions.source_id
    AND session_tags.client = sessions.client
    AND session_tags.session_id = sessions.session_id;

CREATE OR REPLACE VIEW noted_sessions AS
SELECT sessions.*, notes.note, notes.created_at AS note_created_at,
       notes.updated_at AS note_updated_at
FROM sessions
JOIN notes
    ON notes.source_id = sessions.source_id
    AND notes.client = sessions.client
    AND notes.session_id = sessions.session_id;

-- Curation commands read notes through this stable, dialect-paired view.
CREATE OR REPLACE VIEW session_notes AS
SELECT source_id, client, session_id, note, created_at, updated_at
FROM notes;
