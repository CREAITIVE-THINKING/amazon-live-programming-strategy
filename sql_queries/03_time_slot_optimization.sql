-- SQL Query for Time Slot Optimization in Amazon Live
-- This query identifies optimal time slots for different categories and creator types

-- 1. Time Slot Performance Overview
SELECT
    s.time_slot,
    s.day_of_week,
    COUNT(DISTINCT s.session_id) AS session_count,
    COUNT(DISTINCT s.creator_id) AS creator_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.peak_viewers) AS avg_peak_viewers,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute
FROM 
    stream_sessions s
GROUP BY 
    s.time_slot, s.day_of_week
ORDER BY 
    revenue_per_minute DESC;

-- 2. Time Slot Performance by Category
SELECT
    s.primary_category,
    s.time_slot,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.peak_viewers) AS avg_peak_viewers,
    AVG(s.engagement_rate)::NUMERIC(5,2) AS avg_engagement_rate,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute
FROM 
    stream_sessions s
GROUP BY 
    s.primary_category, s.time_slot
ORDER BY 
    s.primary_category, revenue_per_minute DESC;

-- 3. Hourly Performance Analysis (Detailed Time Slots)
SELECT
    EXTRACT(HOUR FROM s.start_time) AS hour_of_day,
    s.day_of_week,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.peak_viewers) AS avg_peak_viewers,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute
FROM 
    stream_sessions s
GROUP BY 
    hour_of_day, s.day_of_week
ORDER BY 
    revenue_per_minute DESC;

-- 4. Creator Tier Performance by Time Slot
SELECT
    c.creator_tier,
    s.time_slot,
    s.day_of_week,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.peak_viewers) AS avg_peak_viewers,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute
FROM 
    stream_sessions s
JOIN
    creator_dimension c ON s.creator_id = c.creator_id
GROUP BY 
    c.creator_tier, s.time_slot, s.day_of_week
ORDER BY 
    c.creator_tier, revenue_per_minute DESC;

-- 5. Optimal Category-Time Slot Pairing
SELECT
    s.primary_category,
    s.time_slot,
    s.day_of_week,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.peak_viewers) AS avg_peak_viewers,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    RANK() OVER (
        PARTITION BY s.primary_category 
        ORDER BY (SUM(s.revenue) / SUM(s.view_minutes)) DESC
    ) AS time_slot_rank
FROM 
    stream_sessions s
GROUP BY 
    s.primary_category, s.time_slot, s.day_of_week
HAVING
    COUNT(DISTINCT s.session_id) >= 5 -- Minimum sessions for statistical significance
ORDER BY 
    s.primary_category, time_slot_rank;

-- 6. Optimal Creator-Time Slot Pairing
SELECT
    c.creator_id,
    c.creator_name,
    c.primary_category,
    s.time_slot,
    s.day_of_week,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.peak_viewers) AS avg_peak_viewers,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    RANK() OVER (
        PARTITION BY c.creator_id 
        ORDER BY (SUM(s.revenue) / SUM(s.view_minutes)) DESC
    ) AS time_slot_rank
FROM 
    stream_sessions s
JOIN
    creator_dimension c ON s.creator_id = c.creator_id
GROUP BY 
    c.creator_id, c.creator_name, c.primary_category, s.time_slot, s.day_of_week
HAVING
    COUNT(DISTINCT s.session_id) >= 3 -- Minimum sessions for statistical significance
ORDER BY 
    c.creator_id, time_slot_rank;

-- 7. Time Slot Competition Analysis
-- Identifies overlapping high-performing timeslots across categories
WITH category_time_performance AS (
    SELECT
        s.primary_category,
        s.time_slot,
        s.day_of_week,
        COUNT(DISTINCT s.session_id) AS session_count,
        SUM(s.view_minutes) AS total_view_minutes,
        SUM(s.revenue) AS total_revenue,
        (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
        RANK() OVER (
            PARTITION BY s.primary_category 
            ORDER BY (SUM(s.revenue) / SUM(s.view_minutes)) DESC
        ) AS time_slot_rank
    FROM 
        stream_sessions s
    GROUP BY 
        s.primary_category, s.time_slot, s.day_of_week
    HAVING
        COUNT(DISTINCT s.session_id) >= 5
)
SELECT
    time_slot,
    day_of_week,
    COUNT(DISTINCT primary_category) AS competing_categories,
    STRING_AGG(primary_category, ', ' ORDER BY revenue_per_minute DESC) AS categories,
    SUM(session_count) AS total_sessions,
    SUM(total_revenue) AS total_revenue
FROM
    category_time_performance
WHERE
    time_slot_rank <= 3 -- Top 3 time slots for each category
GROUP BY
    time_slot, day_of_week
ORDER BY
    competing_categories DESC, total_revenue DESC;
