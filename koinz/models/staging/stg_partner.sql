{{ config(
    materialized='incremental',
    unique_key='partner_id',
    incremental_strategy='append',
    on_schema_change='sync_all_columns'

) }}

SELECT 
    partner_id,
    order_date,

    -- Running Totals (Cumulative Over Time)
    SUM(order_total) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_gross_revenue,  
    SUM(order_total - koinz_commission) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_partner_net_revenue,  
    SUM(koinz_commission) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_koinz_revenue,  
    SUM(cashback_redeemed) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cashback_redeemed,  
    SUM(discount) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_discounts,  
    COUNT(order_id) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_orders,  

    -- Running Totals for Discount & Cashback Shares
    SUM(koinz_discount_share) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_koinz_discount_cost,  
    SUM(merchant_discount_share) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_partner_discount_cost,  
    SUM(koinz_cashback_share) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_koinz_cashback_cost,  
    SUM(merchant_cashback_share) OVER (PARTITION BY partner_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_partner_cashback_cost  

FROM {{ ref('source') }}
{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
ORDER BY partner_id, order_date
