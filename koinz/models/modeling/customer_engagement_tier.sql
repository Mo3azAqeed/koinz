{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id'
) }}

WITH customer_metrics AS (
    SELECT 
        customer_id,
        MAX(last_order_date) AS last_order_date,  -- Recency
        total_orders AS order_count,             -- Frequency
        total_gross_revenue                      -- Monetary
    FROM {{ ref('int_customers') }}
    GROUP BY customer_id, total_orders, total_gross_revenue

    {% if is_incremental() %}
    -- Process only customers with new data
    WHERE last_order_date > (SELECT MAX(last_order_date) FROM {{ this }})
    {% endif %}
),

rfm_percentiles AS (
    SELECT 
        customer_id,
        last_order_date,
        order_count,
        total_gross_revenue,

        -- Calculate Percentiles**
        PERCENT_RANK() OVER (ORDER BY last_order_date DESC) AS recency_rank,
        PERCENT_RANK() OVER (ORDER BY order_count) AS frequency_rank,
        PERCENT_RANK() OVER (ORDER BY total_gross_revenue) AS monetary_rank

    FROM customer_metrics
),

rfm_scores AS (
    SELECT 
        customer_id,
        last_order_date,
        order_count,
        total_gross_revenue,

        CASE 
            WHEN recency_rank >= 0.70 THEN 'Active'
            WHEN recency_rank BETWEEN 0.30 AND 0.70 THEN 'Dormant'
            ELSE 'Churned'
        END AS activity_status,

        CASE 
            WHEN frequency_rank >= 0.70 THEN 'Power Buyers'
            WHEN frequency_rank BETWEEN 0.30 AND 0.70 THEN 'Regular Buyers'
            ELSE 'One-Time Buyers'
        END AS frequency_segment,

        CASE 
            WHEN monetary_rank >= 0.70 THEN 'High-Value Customers'
            WHEN monetary_rank BETWEEN 0.30 AND 0.70 THEN 'Mid-Tier Customers'
            ELSE 'Bargain Shoppers'
        END AS monetary_segment

    FROM rfm_percentiles
)

SELECT 
    customer_id,
    last_order_date,
    order_count,
    total_gross_revenue,
    activity_status,
    frequency_segment,
    monetary_segment

FROM rfm_scores
