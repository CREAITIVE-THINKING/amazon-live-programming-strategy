-- SQL Query for Creator Performance Analysis in Amazon Live
-- This query analyzes creator performance metrics across different categories and time slots

-- 1. Top Performing Creators by Revenue and Conversion Rate
SELECT
    c.creator_id,
    c.creator_name,
    c.primary_category,
    c.creator_tier,
    COUNT(DISTINCT s.session_id) AS total_sessions,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate
FROM 
    creator_dimension c
JOIN 
    stream_sessions s ON c.creator_id = s.creator_id
GROUP BY 
    c.creator_id, c.creator_name, c.primary_category, c.creator_tier
ORDER BY 
    revenue_per_minute DESC
LIMIT 20;

-- 2. Creator Performance by Time Slot (Time of Day)
SELECT
    c.creator_tier,
    s.time_slot,
    COUNT(DISTINCT c.creator_id) AS creator_count,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate
FROM 
    creator_dimension c
JOIN 
    stream_sessions s ON c.creator_id = s.creator_id
GROUP BY 
    c.creator_tier, s.time_slot
ORDER BY 
    c.creator_tier, revenue_per_minute DESC;

-- 3. Creator Performance by Day of Week
SELECT
    c.creator_tier,
    s.day_of_week,
    COUNT(DISTINCT c.creator_id) AS creator_count,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate
FROM 
    creator_dimension c
JOIN 
    stream_sessions s ON c.creator_id = s.creator_id
GROUP BY 
    c.creator_tier, s.day_of_week
ORDER BY 
    c.creator_tier, revenue_per_minute DESC;

-- 4. Creator Category Performance Matrix
SELECT
    c.primary_category,
    c.creator_tier,
    COUNT(DISTINCT c.creator_id) AS creator_count,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    AVG(s.engagement_rate)::NUMERIC(5,2) AS avg_engagement_rate
FROM 
    creator_dimension c
JOIN 
    stream_sessions s ON c.creator_id = s.creator_id
GROUP BY 
    c.primary_category, c.creator_tier
ORDER BY 
    revenue_per_minute DESC;

-- 5. Creator Growth and Retention Analysis
WITH creator_cohorts AS (
    SELECT
        DATE_TRUNC('month', c.join_date)::DATE AS cohort_month,
        c.creator_id,
        c.creator_name,
        c.primary_category,
        c.creator_tier
    FROM
        creator_dimension c
),
creator_activity AS (
    SELECT
        c.creator_id,
        DATE_TRUNC('month', s.session_date)::DATE AS activity_month,
        COUNT(DISTINCT s.session_id) AS session_count
    FROM
        stream_sessions s
    JOIN
        creator_dimension c ON s.creator_id = c.creator_id
    GROUP BY
        c.creator_id, activity_month
),
cohort_retention AS (
    SELECT
        cc.cohort_month,
        cc.creator_tier,
        ca.activity_month,
        COUNT(DISTINCT cc.creator_id) AS active_creators,
        (EXTRACT(MONTH FROM AGE(ca.activity_month, cc.cohort_month)) + 
         EXTRACT(YEAR FROM AGE(ca.activity_month, cc.cohort_month)) * 12)::INTEGER AS months_since_join
    FROM
        creator_cohorts cc
    JOIN
        creator_activity ca ON cc.creator_id = ca.creator_id
    GROUP BY
        cc.cohort_month, cc.creator_tier, ca.activity_month, months_since_join
)
SELECT
    cohort_month,
    creator_tier,
    months_since_join,
    active_creators,
    FIRST_VALUE(active_creators) OVER (
        PARTITION BY cohort_month, creator_tier 
        ORDER BY months_since_join
    ) AS original_cohort_size,
    (active_creators * 100.0 / FIRST_VALUE(active_creators) OVER (
        PARTITION BY cohort_month, creator_tier 
        ORDER BY months_since_join
    ))::NUMERIC(5,2) AS retention_percentage
FROM
    cohort_retention
WHERE
    months_since_join <= 6 -- Looking at 6-month retention
ORDER BY
    cohort_month, creator_tier, months_since_join;

-- 6. Creator Performance Trends Over Time
SELECT
    DATE_TRUNC('month', s.session_date)::DATE AS month,
    c.creator_tier,
    COUNT(DISTINCT c.creator_id) AS active_creators,
    COUNT(DISTINCT s.session_id) AS total_sessions,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate
FROM
    creator_dimension c
JOIN
    stream_sessions s ON c.creator_id = s.creator_id
GROUP BY
    month, c.creator_tier
ORDER BY
    month, c.creator_tier;
