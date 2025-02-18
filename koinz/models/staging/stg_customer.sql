{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='append'
) }}

SELECT 
    customer_id,
    order_date,


    -- Running totals
    SUM(order_total) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_gross_revenue,
    SUM(cashback_gained) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cashback_earned,
    SUM(discount) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_applied,
    SUM(koinz_commission) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cltv,
    SUM(cashback_redeemed) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cashback_redeemed,
    SUM(koinz_discount_share) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_koinz_discount_share,
    SUM(merchant_discount_share) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_merchant_discount_share,
    SUM(koinz_cashback_share) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_koinz_cashback_share, 
    SUM(merchant_cashback_share) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_merchant_cashback_share,

    -- Running count metrics
    COUNT(order_id) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_orders,

    -- First and last campaign
    FIRST_VALUE(campaign_name) OVER (PARTITION BY customer_id ORDER BY order_date) AS acquisition_campaign,
    LAST_VALUE(campaign_name) OVER (PARTITION BY customer_id ORDER BY order_date) AS last_campaign_engaged,

    -- Last order ID
    LAST_VALUE(order_id) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_order_id,
    LAST_VALUE(partner_id) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_partner_id,
    MIN(order_date) OVER (PARTITION BY customer_id) AS first_order_date,
    MAX(order_date) OVER (PARTITION BY customer_id) AS last_order_date,
    -- Days since last order
    order_date - LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS days_since_last_order,

    -- Cashback promo usage
    CASE 
        WHEN cashback_gained IS NULL THEN 'No Cashback'
        WHEN cashback_remaining < cashback_gained THEN 'Used The Promo' 
        ELSE 'Not Used' 
    END AS cashback_promo_status,

    CURRENT_TIMESTAMP AS created_at

FROM {{ ref('source') }} 

{% if is_incremental() %}
-- Append only new records
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}

ORDER BY customer_id, order_date
