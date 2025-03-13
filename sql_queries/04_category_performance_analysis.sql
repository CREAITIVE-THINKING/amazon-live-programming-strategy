-- SQL Query for Category Performance Analysis in Amazon Live
-- This query analyzes performance of different product categories and their optimization potential

-- 1. Overall Category Performance Summary
SELECT
    s.primary_category,
    COUNT(DISTINCT s.session_id) AS session_count,
    COUNT(DISTINCT s.creator_id) AS creator_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.product_showcased) AS total_products_showcased,
    SUM(s.revenue) AS total_revenue,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    (SUM(s.units_sold) / SUM(s.product_showcased))::NUMERIC(5,2) AS units_per_product
FROM 
    stream_sessions s
GROUP BY 
    s.primary_category
ORDER BY 
    revenue_per_minute DESC;

-- 2. Category Performance Trend Over Time
SELECT
    DATE_TRUNC('month', s.session_date)::DATE AS month,
    s.primary_category,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute
FROM 
    stream_sessions s
GROUP BY 
    month, s.primary_category
ORDER BY 
    s.primary_category, month;

-- 3. Category Performance by Creator Tier
SELECT
    s.primary_category,
    c.creator_tier,
    COUNT(DISTINCT s.session_id) AS session_count,
    COUNT(DISTINCT c.creator_id) AS creator_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    RANK() OVER (
        PARTITION BY s.primary_category 
        ORDER BY (SUM(s.revenue) / SUM(s.view_minutes)) DESC
    ) AS tier_rank
FROM 
    stream_sessions s
JOIN
    creator_dimension c ON s.creator_id = c.creator_id
GROUP BY 
    s.primary_category, c.creator_tier
ORDER BY 
    s.primary_category, tier_rank;

-- 4. Category Cross-Promotion Analysis
-- Identifies categories that perform well together for cross-promotion
WITH creator_categories AS (
    SELECT
        c.creator_id,
        c.creator_name,
        s.primary_category,
        SUM(s.revenue) AS category_revenue,
        SUM(s.view_minutes) AS category_view_minutes,
        (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS category_revenue_per_minute
    FROM 
        stream_sessions s
    JOIN
        creator_dimension c ON s.creator_id = c.creator_id
    GROUP BY 
        c.creator_id, c.creator_name, s.primary_category
    HAVING
        SUM(s.view_minutes) > 0 -- Avoid division by zero
)
SELECT
    a.primary_category AS category_1,
    b.primary_category AS category_2,
    COUNT(DISTINCT a.creator_id) AS common_creators,
    AVG(a.category_revenue_per_minute) AS avg_category_1_rpm,
    AVG(b.category_revenue_per_minute) AS avg_category_2_rpm,
    CORR(a.category_revenue_per_minute, b.category_revenue_per_minute)::NUMERIC(5,2) AS rpm_correlation
FROM
    creator_categories a
JOIN
    creator_categories b ON a.creator_id = b.creator_id AND a.primary_category < b.primary_category
GROUP BY
    a.primary_category, b.primary_category
HAVING
    COUNT(DISTINCT a.creator_id) >= 3 -- Minimum creators for statistical significance
ORDER BY
    rpm_correlation DESC, common_creators DESC;

-- 5. Category Time Slot Heatmap
-- Shows performance by category and time slot for programming optimization
SELECT
    s.primary_category,
    s.time_slot,
    s.day_of_week,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    -- Calculate percentile rank for this time slot within the category
    PERCENT_RANK() OVER (
        PARTITION BY s.primary_category 
        ORDER BY (SUM(s.revenue) / SUM(s.view_minutes))
    )::NUMERIC(5,2) AS percentile_rank
FROM 
    stream_sessions s
GROUP BY 
    s.primary_category, s.time_slot, s.day_of_week
HAVING
    COUNT(DISTINCT s.session_id) >= 3 -- Minimum sessions for statistical significance
ORDER BY 
    s.primary_category, revenue_per_minute DESC;

-- 6. Category Gap Analysis
-- Identifies underrepresented categories with growth potential
SELECT
    c.primary_category,
    COUNT(DISTINCT c.creator_id) AS total_creators,
    SUM(CASE WHEN c.creator_tier = 'Top' THEN 1 ELSE 0 END) AS top_tier_creators,
    SUM(CASE WHEN c.creator_tier = 'Mid' THEN 1 ELSE 0 END) AS mid_tier_creators,
    SUM(CASE WHEN c.creator_tier = 'Emerging' THEN 1 ELSE 0 END) AS emerging_creators,
    COUNT(DISTINCT s.session_id) AS total_sessions,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.revenue) AS total_revenue,
    (SUM(s.revenue) / NULLIF(SUM(s.view_minutes), 0))::NUMERIC(10,2) AS revenue_per_minute,
    
    -- Calculate relative representation
    (COUNT(DISTINCT c.creator_id) * 100.0 / SUM(COUNT(DISTINCT c.creator_id)) OVER ())::NUMERIC(5,2) AS creator_percentage,
    (SUM(s.revenue) * 100.0 / SUM(SUM(s.revenue)) OVER ())::NUMERIC(5,2) AS revenue_percentage,
    
    -- Calculate opportunity index: revenue percentage / creator percentage
    -- Values > 1 indicate underrepresented categories with good performance
    (
        (SUM(s.revenue) * 100.0 / SUM(SUM(s.revenue)) OVER ()) / 
        NULLIF((COUNT(DISTINCT c.creator_id) * 100.0 / SUM(COUNT(DISTINCT c.creator_id)) OVER ()), 0)
    )::NUMERIC(5,2) AS opportunity_index
FROM 
    creator_dimension c
LEFT JOIN
    stream_sessions s ON c.creator_id = s.creator_id
GROUP BY 
    c.primary_category
ORDER BY 
    opportunity_index DESC;

-- 7. Seasonal Category Performance
-- Identifies seasonal patterns in category performance
SELECT
    s.primary_category,
    EXTRACT(MONTH FROM s.session_date) AS month_number,
    TO_CHAR(s.session_date, 'Month') AS month_name,
    COUNT(DISTINCT s.session_id) AS session_count,
    SUM(s.view_minutes) AS total_view_minutes,
    SUM(s.units_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    AVG(s.conversion_rate)::NUMERIC(5,2) AS avg_conversion_rate,
    (SUM(s.revenue) / SUM(s.view_minutes))::NUMERIC(10,2) AS revenue_per_minute,
    -- Calculate percentile rank for this month within the category
    PERCENT_RANK() OVER (
        PARTITION BY s.primary_category 
        ORDER BY (SUM(s.revenue) / SUM(s.view_minutes))
    )::NUMERIC(5,2) AS monthly_percentile
FROM 
    stream_sessions s
GROUP BY 
    s.primary_category, month_number, month_name
ORDER BY 
    s.primary_category, month_number;
