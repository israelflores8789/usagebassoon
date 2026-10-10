-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- Named read queries over installed views. Filter values are bound separately;
-- placeholders accept only trusted WHERE, ordering, and limit fragments.
-- Apply filters at the input grain before any report aggregation.

-- name: report_daily
SELECT
    facts.day AS day,
    SUM(COALESCE(facts.input_tokens, 0)) AS input_tokens,
    SUM(COALESCE(facts.output_tokens, 0)) AS raw_output_tokens,
    SUM(COALESCE(facts.reasoning, 0)) AS reasoning_tokens,
    SUM(COALESCE(facts.output_tokens, 0)) + SUM(COALESCE(facts.reasoning, 0)) AS output_tokens,
    SUM(COALESCE(facts.cache_read, 0)) AS cache_read,
    SUM(COALESCE(facts.cache_write, 0)) AS cache_write,
    SUM(COALESCE(facts.total_tokens, 0)) AS total_tokens,
    SUM(facts.perf_duration_ms) AS perf_duration_ms,
    COUNT(facts.perf_duration_ms) AS measured_fact_count,
    COUNT(*) AS total_fact_count,
    CASE WHEN COUNT(DISTINCT facts.cost_basis) = 1
        THEN MAX(facts.cost_basis) ELSE 'mixed' END AS cost_basis,
    CASE WHEN COUNT(facts.cost_usd) = COUNT(*)
        THEN SUM(facts.cost_usd) ELSE NULL END AS cost_usd
FROM report_daily_usage AS facts
{where}
GROUP BY facts.day
ORDER BY facts.day DESC
{limit};

-- name: report_models
SELECT
    facts.model AS model,
    facts.client AS client,
    SUM(COALESCE(facts.input_tokens, 0)) AS input_tokens,
    SUM(COALESCE(facts.output_tokens, 0) + COALESCE(facts.reasoning, 0)) AS output_tokens,
    SUM(COALESCE(facts.cache_read, 0)) AS cache_read,
    SUM(COALESCE(facts.cache_write, 0)) AS cache_write,
    SUM(COALESCE(facts.total_tokens, 0)) AS total_tokens,
    SUM(CASE WHEN facts.perf_duration_ms IS NOT NULL AND facts.perf_timed_tokens > 0
        THEN facts.perf_duration_ms END) AS perf_duration_ms,
    SUM(CASE WHEN facts.perf_duration_ms IS NOT NULL AND facts.perf_timed_tokens > 0
        THEN facts.perf_timed_tokens END) AS perf_timed_tokens,
    CASE WHEN COUNT(facts.cost_usd) = COUNT(*)
        THEN SUM(facts.cost_usd) ELSE NULL END AS cost_usd
FROM report_models AS facts
{where}
GROUP BY facts.model, facts.client
ORDER BY total_tokens DESC, facts.model, facts.client;

-- name: report_sessions
SELECT
    usage.*,
    CASE WHEN usage.perf_timed_tokens > 0
        THEN 1000.0 * usage.perf_timed_duration_ms / usage.perf_timed_tokens END AS ms_per_1k_tokens,
    CASE WHEN usage.total_tokens > 0
        THEN 1000000.0 * usage.cost_usd / usage.total_tokens END AS cost_per_million
FROM (
    SELECT
        facts.source_id,
        facts.client,
        facts.session_id,
        STRING_AGG(facts.model, ', ' ORDER BY facts.model) AS model,
        SUM(COALESCE(facts.input_tokens, 0)) AS input_tokens,
        SUM(COALESCE(facts.output_tokens, 0)) AS raw_output_tokens,
        SUM(COALESCE(facts.reasoning, 0)) AS reasoning_tokens,
        SUM(COALESCE(facts.output_tokens, 0)) + SUM(COALESCE(facts.reasoning, 0)) AS output_tokens,
        SUM(COALESCE(facts.cache_read, 0)) AS cache_read,
        SUM(COALESCE(facts.cache_write, 0)) AS cache_write,
        SUM(COALESCE(facts.total_tokens, 0)) AS total_tokens,
        SUM(facts.perf_duration_ms) AS perf_duration_ms,
        SUM(facts.perf_timed_duration_ms) AS perf_timed_duration_ms,
        SUM(facts.perf_timed_tokens) AS perf_timed_tokens,
        CASE WHEN COUNT(facts.cost_usd) = COUNT(*)
            THEN SUM(facts.cost_usd) ELSE NULL END AS cost_usd,
        MAX(facts.last_active) AS last_active,
        MAX(facts.activity_day) AS activity_day,
        MAX(facts.last_usage_day) AS last_usage_day,
        MAX(CASE WHEN facts.last_active_stale THEN 1 ELSE 0 END) AS last_active_stale,
        MAX(facts.created_at) AS created_at
    FROM report_session_models AS facts
    {where}
    GROUP BY facts.source_id, facts.client, facts.session_id
) AS usage
ORDER BY {ordering}, last_active DESC NULLS LAST, source_id, client, session_id, model
{limit};

-- name: report_session_models
SELECT
    usage.*,
    CASE WHEN usage.perf_timed_tokens > 0
        THEN 1000.0 * usage.perf_timed_duration_ms / usage.perf_timed_tokens END AS ms_per_1k_tokens,
    CASE WHEN usage.total_tokens > 0
        THEN 1000000.0 * usage.cost_usd / usage.total_tokens END AS cost_per_million
FROM (
    SELECT
        facts.source_id,
        facts.client,
        facts.session_id,
        facts.model AS model,
        facts.input_tokens AS input_tokens,
        facts.output_tokens AS raw_output_tokens,
        facts.reasoning AS reasoning_tokens,
        COALESCE(facts.output_tokens, 0) + COALESCE(facts.reasoning, 0) AS output_tokens,
        facts.cache_read AS cache_read,
        facts.cache_write AS cache_write,
        facts.total_tokens AS total_tokens,
        facts.perf_duration_ms AS perf_duration_ms,
        facts.perf_timed_duration_ms AS perf_timed_duration_ms,
        facts.perf_timed_tokens AS perf_timed_tokens,
        facts.cost_usd AS cost_usd,
        facts.last_active AS last_active,
        facts.activity_day AS activity_day,
        facts.last_usage_day AS last_usage_day,
        facts.last_active_stale AS last_active_stale,
        facts.created_at AS created_at
    FROM report_session_models AS facts
    {where}
) AS usage
ORDER BY {ordering}, last_active DESC NULLS LAST, source_id, client, session_id, model
{limit};
