-- SQL Query for Viewer Engagement Analysis in Amazon Live
-- This query analyzes viewer engagement patterns to optimize programming strategies

-- 1. Overall Engagement Metrics by Category
SELECT
    s.primary_category,
    COUNT(DISTINCT s.session_id) AS session_count,
    AVG(s.avg_viewers) AS avg_viewers_per_session,
    AVG(s.peak_viewers) AS avg_peak_viewers,
    SUM(s.view_minutes) AS total_view_minutes,
    AVG(s.engagement_rate)::NUMERIC(5,2) AS avg_engagement_rate,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.units_sold) * 1.0 / SUM(s.avg_viewers))::NUMERIC(5,2) AS units_per_avg_viewer,
    (SUM(s.revenue) * 1.0 / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute
FROM 
    stream_sessions s
GROUP BY 
    s.primary_category
ORDER BY 
    avg_engagement_rate DESC;

-- 2. Viewer Engagement by Time of Day
SELECT
    s.time_slot,
    s.day_of_week,
    COUNT(DISTINCT s.session_id) AS session_count,
    AVG(s.avg_viewers) AS avg_viewers_per_session,
    AVG(s.peak_viewers) AS avg_peak_viewers,
    SUM(s.view_minutes) AS total_view_minutes,
    AVG(s.engagement_rate)::NUMERIC(5,2) AS avg_engagement_rate,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate
FROM 
    stream_sessions s
GROUP BY 
    s.time_slot, s.day_of_week
ORDER BY 
    avg_engagement_rate DESC;

-- 3. Engagement to Conversion Correlation
-- Analyzes the relationship between engagement and conversion rates
SELECT
    s.primary_category,
    COUNT(DISTINCT s.session_id) AS session_count,
    AVG(s.engagement_rate)::NUMERIC(5,2) AS avg_engagement_rate,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    CORR(s.engagement_rate, s.conversion_rate)::NUMERIC(5,2) AS engagement_conversion_correlation,
    -- Regression slope approximation
    REGR_SLOPE(s.conversion_rate, s.engagement_rate)::NUMERIC(5,2) AS conversion_per_engagement_unit,
    REGR_INTERCEPT(s.conversion_rate, s.engagement_rate)::NUMERIC(5,2) AS base_conversion_rate
FROM 
    stream_sessions s
GROUP BY 
    s.primary_category
HAVING
    COUNT(DISTINCT s.session_id) >= 10 -- Minimum sessions for statistical significance
ORDER BY 
    engagement_conversion_correlation DESC;

-- 4. Creator Engagement Performance
-- Identify creators who drive high engagement
SELECT
    c.creator_id,
    c.creator_name,
    c.primary_category,
    c.creator_tier,
    COUNT(DISTINCT s.session_id) AS session_count,
    AVG(s.avg_viewers) AS avg_viewers_per_session,
    AVG(s.peak_viewers) AS avg_peak_viewers,
    AVG(s.engagement_rate)::NUMERIC(5,2) AS avg_engagement_rate,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (AVG(s.engagement_rate) - 
        AVG(AVG(s.engagement_rate)) OVER (PARTITION BY c.primary_category)
    )::NUMERIC(5,2) AS engagement_vs_category_avg
FROM 
    stream_sessions s
JOIN
    creator_dimension c ON s.creator_id = c.creator_id
GROUP BY 
    c.creator_id, c.creator_name, c.primary_category, c.creator_tier
HAVING
    COUNT(DISTINCT s.session_id) >= 5 -- Minimum sessions for statistical significance
ORDER BY 
    avg_engagement_rate DESC;

-- 5. Viewer Retention Simulation
-- Simulates viewer retention patterns based on engagement metrics
WITH session_series AS (
    SELECT 
        s.session_id,
        s.creator_id,
        c.creator_name,
        s.session_date,
        s.time_slot,
        s.engagement_rate,
        s.conversion_rate,
        ROW_NUMBER() OVER (PARTITION BY s.creator_id ORDER BY s.session_date) AS session_seq
    FROM 
        stream_sessions s
    JOIN
        creator_dimension c ON s.creator_id = c.creator_id
),
retention_metrics AS (
    SELECT
        a.creator_id,
        a.creator_name,
        a.session_id AS initial_session,
        a.session_date AS initial_date,
        a.engagement_rate AS initial_engagement,
        COUNT(DISTINCT b.session_id) AS future_sessions,
        AVG(b.engagement_rate)::NUMERIC(5,2) AS avg_future_engagement,
        MAX(b.session_seq - a.session_seq) AS session_span
    FROM
        session_series a
    LEFT JOIN
        session_series b ON a.creator_id = b.creator_id AND b.session_seq > a.session_seq
    GROUP BY
        a.creator_id, a.creator_name, a.session_id, a.session_date, a.engagement_rate
)
SELECT
    creator_id,
    creator_name,
    AVG(initial_engagement)::NUMERIC(5,2) AS avg_initial_engagement,
    AVG(future_sessions)::NUMERIC(5,2) AS avg_subsequent_sessions,
    AVG(CASE WHEN future_sessions > 0 THEN avg_future_engagement ELSE NULL END)::NUMERIC(5,2) AS avg_sustained_engagement,
    AVG(session_span)::NUMERIC(5,2) AS avg_session_span,
    CORR(initial_engagement, future_sessions)::NUMERIC(5,2) AS engagement_retention_correlation
FROM
    retention_metrics
GROUP BY
    creator_id, creator_name
HAVING
    COUNT(*) >= 5 -- Minimum initial sessions
ORDER BY
    avg_subsequent_sessions DESC;

-- 6. Engagement Drivers Analysis
-- Identifies factors that correlate with higher engagement
SELECT
    s.time_slot,
    s.day_of_week,
    c.creator_tier,
    s.primary_category,
    COUNT(DISTINCT s.session_id) AS session_count,
    AVG(s.avg_viewers) AS avg_viewers,
    AVG(s.engagement_rate)::NUMERIC(5,2) AS avg_engagement_rate,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    -- Calculate percentile rank within each dimension
    PERCENT_RANK() OVER (PARTITION BY s.time_slot ORDER BY AVG(s.engagement_rate))::NUMERIC(5,2) AS time_slot_percentile,
    PERCENT_RANK() OVER (PARTITION BY s.day_of_week ORDER BY AVG(s.engagement_rate))::NUMERIC(5,2) AS day_percentile,
    PERCENT_RANK() OVER (PARTITION BY c.creator_tier ORDER BY AVG(s.engagement_rate))::NUMERIC(5,2) AS tier_percentile,
    PERCENT_RANK() OVER (PARTITION BY s.primary_category ORDER BY AVG(s.engagement_rate))::NUMERIC(5,2) AS category_percentile
FROM 
    stream_sessions s
JOIN
    creator_dimension c ON s.creator_id = c.creator_id
GROUP BY 
    s.time_slot, s.day_of_week, c.creator_tier, s.primary_category
HAVING
    COUNT(DISTINCT s.session_id) >= 5 -- Minimum sessions for statistical significance
ORDER BY 
    avg_engagement_rate DESC;

-- 7. Long-Term Viewer Loyalty Analysis
-- Tracks engagement patterns over time for repeat viewers
WITH monthly_engagement AS (
    SELECT
        DATE_TRUNC('month', s.session_date)::DATE AS month,
        s.primary_category,
        COUNT(DISTINCT s.session_id) AS session_count,
        SUM(s.avg_viewers) AS total_avg_viewers,
        SUM(s.peak_viewers) AS total_peak_viewers,
        AVG(s.engagement_rate)::NUMERIC(5,2) AS avg_engagement_rate,
        AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate
    FROM
        stream_sessions s
    GROUP BY
        month, s.primary_category
),
rolling_metrics AS (
    SELECT
        e.*,
        AVG(e.avg_engagement_rate) OVER (
            PARTITION BY e.primary_category 
            ORDER BY e.month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::NUMERIC(5,2) AS rolling_3m_engagement,
        AVG(e.avg_conversion_rate) OVER (
            PARTITION BY e.primary_category 
            ORDER BY e.month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::NUMERIC(5,2) AS rolling_3m_conversion
    FROM
        monthly_engagement e
)
SELECT
    r.primary_category,
    r.month,
    r.avg_engagement_rate,
    r.avg_conversion_rate,
    r.rolling_3m_engagement,
    r.rolling_3m_conversion,
    (r.avg_engagement_rate - LAG(r.avg_engagement_rate, 3) OVER (
        PARTITION BY r.primary_category ORDER BY r.month
    ))::NUMERIC(5,2) AS engagement_change_3m,
    (r.avg_conversion_rate - LAG(r.avg_conversion_rate, 3) OVER (
        PARTITION BY r.primary_category ORDER BY r.month
    ))::NUMERIC(5,2) AS conversion_change_3m
FROM
    rolling_metrics r
ORDER BY
    r.primary_category, r.month;
