-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- Reasoning tokens use the output rate, as tokscale's pricing semantics do.
CREATE OR REPLACE VIEW daily_cost AS
SELECT
    daily_stats.*,
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

-- Familiar all-time session and model totals are calculated from daily facts.
CREATE OR REPLACE VIEW session_model_stats AS
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
    CASE WHEN COUNT(cost_usd) = COUNT(*) THEN SUM(cost_usd) END AS cost_usd,
    MAX(updated_at) AS updated_at
FROM daily_cost
GROUP BY source_id, client, session_id, model;

CREATE OR REPLACE VIEW session_model_stats_current AS
SELECT * FROM session_model_stats;

CREATE OR REPLACE VIEW report_summary AS
SELECT
    COUNT(*) AS sessions,
    COALESCE(SUM(cost_usd), 0) AS cost_usd
FROM (
    SELECT source_id, client, session_id, SUM(cost_usd) AS cost_usd
    FROM session_model_stats
    GROUP BY source_id, client, session_id
) AS session_costs;

CREATE OR REPLACE VIEW report_models AS
SELECT
    model,
    COALESCE(SUM(total_tokens), 0) AS total_tokens,
    COALESCE(SUM(cost_usd), 0) AS cost_usd
FROM session_model_stats
GROUP BY model
ORDER BY cost_usd DESC, model;

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
