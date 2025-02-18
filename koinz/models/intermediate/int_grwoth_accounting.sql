{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='month_year'
) }}

WITH first_time_customers AS (
    -- Identify first-time buyers
    SELECT 
        customer_id,
        MIN(DATE_TRUNC('month', order_date)) AS first_order_month
    FROM swamp_staging.stg_orders
    GROUP BY customer_id
),

monthly_orders AS (
    -- Aggregate order data per month
    SELECT 
        DATE_TRUNC('month', order_date) AS month_year,
        COUNT(DISTINCT customer_id) AS active_customers,
        COUNT(order_id) AS total_orders,
        SUM(order_total) AS total_revenue,
        AVG(order_total) AS AOV,
        SUM(koinz_discount_share + koinz_cashback_share) AS total_koinz_spend,
        SUM(merchant_discount_share + merchant_cashback_share) AS total_merchant_spend,
        SUM(koinz_commission) AS koinz_gross_revenue,
        AVG(koinz_commission) AS average_sale_profit
    FROM swamp_staging.stg_orders
    {% if is_incremental() %}
    WHERE order_date >= (SELECT MIN(month_year) FROM {{ this }}) -- Only process new data
    {% endif %}
    GROUP BY month_year
),

new_customers_per_month AS (
    -- Count newly acquired customers per month
    SELECT 
        first_order_month AS month_year,
        COUNT(customer_id) AS new_customers
    FROM first_time_customers
    GROUP BY first_order_month
)

SELECT 
    o.month_year::DATE AS month,
    o.active_customers,
    COALESCE(n.new_customers, 0) AS new_customers,
    o.total_orders,
    o.total_revenue,
    o.total_koinz_spend,
    o.total_merchant_spend,
    o.koinz_gross_revenue,
    o.AOV::INT AS average_order_value,
    o.koinz_gross_revenue - o.total_koinz_spend AS koinz_net_revenue,
    o.average_sale_profit
FROM monthly_orders o
LEFT JOIN new_customers_per_month n ON o.month_year = n.month_year
