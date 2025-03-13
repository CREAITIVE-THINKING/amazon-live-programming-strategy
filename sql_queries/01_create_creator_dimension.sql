-- SQL Query to Create a "Creator" Dimension for Amazon Live Simulation
-- This query maps product categories to fictional creators and assigns performance metrics

-- Create creator dimension table
CREATE TABLE IF NOT EXISTS creator_dimension (
    creator_id VARCHAR(50) PRIMARY KEY,
    creator_name VARCHAR(100) NOT NULL,
    primary_category VARCHAR(100) NOT NULL,
    secondary_category VARCHAR(100),
    creator_tier VARCHAR(20) NOT NULL, -- 'Top', 'Mid', 'Emerging'
    join_date DATE NOT NULL,
    avg_session_minutes NUMERIC(5,2),
    avg_engagement_rate NUMERIC(5,2)
);

-- Populate creator dimension table based on product categories
-- This assigns fictional creators to product categories based on performance
INSERT INTO creator_dimension (
    creator_id, 
    creator_name,
    primary_category,
    secondary_category,
    creator_tier,
    join_date,
    avg_session_minutes,
    avg_engagement_rate
)
SELECT
    'CR' || LPAD(ROW_NUMBER() OVER (ORDER BY category_name), 3, '0') AS creator_id,
    'Creator ' || SUBSTRING(category_name FROM 1 FOR 1) || LPAD(ROW_NUMBER() OVER (ORDER BY category_name), 2, '0') AS creator_name,
    p.category_name AS primary_category,
    CASE
        WHEN ROW_NUMBER() OVER (ORDER BY category_name) % 3 = 0 THEN (
            SELECT category_name 
            FROM product_categories 
            ORDER BY RANDOM() 
            LIMIT 1
        )
        ELSE NULL
    END AS secondary_category,
    CASE
        WHEN COUNT(*) OVER (PARTITION BY category_name) > 100 THEN 'Top'
        WHEN COUNT(*) OVER (PARTITION BY category_name) > 50 THEN 'Mid'
        ELSE 'Emerging'
    END AS creator_tier,
    CURRENT_DATE - (RANDOM() * 365 * 2)::INTEGER AS join_date,
    (RANDOM() * 30 + 15)::NUMERIC(5,2) AS avg_session_minutes,
    (RANDOM() * 5 + 1)::NUMERIC(5,2) AS avg_engagement_rate
FROM (
    SELECT DISTINCT category_name
    FROM product_categories
) p;

-- Create a stream session dimension that maps products to creators and time slots
CREATE TABLE IF NOT EXISTS stream_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    creator_id VARCHAR(50) REFERENCES creator_dimension(creator_id),
    session_date DATE NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    time_slot VARCHAR(20) NOT NULL, -- 'Morning', 'Afternoon', 'Evening', 'Night'
    primary_category VARCHAR(100) NOT NULL,
    view_minutes INTEGER NOT NULL,
    peak_viewers INTEGER NOT NULL,
    avg_viewers INTEGER NOT NULL,
    engagement_rate NUMERIC(5,2) NOT NULL,
    product_showcased INTEGER NOT NULL,
    units_sold INTEGER NOT NULL,
    revenue NUMERIC(10,2) NOT NULL,
    conversion_rate NUMERIC(5,2) NOT NULL
);

-- Populate stream sessions based on order data
-- This simulates Amazon Live streams using e-commerce transaction data
INSERT INTO stream_sessions (
    session_id,
    creator_id,
    session_date,
    start_time,
    end_time,
    day_of_week,
    time_slot,
    primary_category,
    view_minutes,
    peak_viewers,
    avg_viewers,
    engagement_rate,
    product_showcased,
    units_sold,
    revenue,
    conversion_rate
)
SELECT
    'SS' || LPAD(ROW_NUMBER() OVER (ORDER BY o.order_purchase_timestamp), 6, '0') AS session_id,
    c.creator_id,
    o.order_purchase_timestamp::DATE AS session_date,
    
    -- Create realistic time slots based on order patterns
    CASE
        WHEN EXTRACT(HOUR FROM o.order_purchase_timestamp) BETWEEN 6 AND 11 THEN 
            (EXTRACT(HOUR FROM o.order_purchase_timestamp) || ':00:00')::TIME
        WHEN EXTRACT(HOUR FROM o.order_purchase_timestamp) BETWEEN 12 AND 17 THEN 
            (EXTRACT(HOUR FROM o.order_purchase_timestamp) || ':00:00')::TIME
        WHEN EXTRACT(HOUR FROM o.order_purchase_timestamp) BETWEEN 18 AND 21 THEN 
            (EXTRACT(HOUR FROM o.order_purchase_timestamp) || ':00:00')::TIME
        ELSE (EXTRACT(HOUR FROM o.order_purchase_timestamp) || ':00:00')::TIME
    END AS start_time,
    
    -- End time is start time + avg_session_minutes
    (CASE
        WHEN EXTRACT(HOUR FROM o.order_purchase_timestamp) BETWEEN 6 AND 11 THEN 
            (EXTRACT(HOUR FROM o.order_purchase_timestamp) || ':00:00')::TIME
        WHEN EXTRACT(HOUR FROM o.order_purchase_timestamp) BETWEEN 12 AND 17 THEN 
            (EXTRACT(HOUR FROM o.order_purchase_timestamp) || ':00:00')::TIME
        WHEN EXTRACT(HOUR FROM o.order_purchase_timestamp) BETWEEN 18 AND 21 THEN 
            (EXTRACT(HOUR FROM o.order_purchase_timestamp) || ':00:00')::TIME
        ELSE (EXTRACT(HOUR FROM o.order_purchase_timestamp) || ':00:00')::TIME
    END + (c.avg_session_minutes || ' minutes')::INTERVAL) AS end_time,
    
    TO_CHAR(o.order_purchase_timestamp, 'Day') AS day_of_week,
    CASE
        WHEN EXTRACT(HOUR FROM o.order_purchase_timestamp) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM o.order_purchase_timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM o.order_purchase_timestamp) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_slot,
    
    c.primary_category,
    c.avg_session_minutes * 60 AS view_minutes, -- Converting to minutes
    (RANDOM() * 1000 + 500)::INTEGER AS peak_viewers,
    (RANDOM() * 800 + 300)::INTEGER AS avg_viewers,
    c.avg_engagement_rate + (RANDOM() * 2 - 1) AS engagement_rate, -- Base + variation
    
    COUNT(DISTINCT oi.product_id) AS product_showcased,
    SUM(oi.order_item_quantity) AS units_sold,
    SUM(oi.price) AS revenue,
    (SUM(oi.order_item_quantity) / (RANDOM() * 800 + 300) * 100)::NUMERIC(5,2) AS conversion_rate
    
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN creator_dimension c ON p.product_category_name = c.primary_category
GROUP BY
    o.order_purchase_timestamp,
    c.creator_id,
    c.primary_category,
    c.avg_session_minutes,
    c.avg_engagement_rate
ORDER BY o.order_purchase_timestamp
LIMIT 1000; -- Limiting to 1000 sessions for manageability
