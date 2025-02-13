{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id'
) }}

WITH customer_running_metrics AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date DESC) AS row_num
    FROM {{ ref('stg_customer') }}

    {% if is_incremental() %}
    WHERE order_date > (SELECT MAX(last_order_date) FROM {{ this }})  -- Process only new data
    {% endif %}
)

SELECT 
    customer_id,

    -- Lifetime totals (from latest row)
    running_gross_revenue AS total_gross_revenue,
    running_cashback_earned AS total_cashback_earned,
    running_cashback_redeemed AS total_cashback_redeemed,
    running_discount_applied AS total_discount_applied,
    running_cltv AS total_cltv,
    running_total_orders AS total_orders,

    -- Acquisition and Engagement
    acquisition_campaign,
    last_campaign_engaged,

    -- Last Order Details
    order_date AS last_order_date,
    last_order_id,
    last_partner_id,

    -- Retention Insights
    days_since_last_order,

    -- Cashback Promo Usage
    cashback_promo_status,

    CURRENT_TIMESTAMP AS created_at
FROM customer_running_metrics
WHERE row_num = 1  -- Keep only the latest record per customer
