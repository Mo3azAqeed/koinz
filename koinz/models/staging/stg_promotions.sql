{{ config(
    materialized='incremental',
    unique_key=['campaign_name', 'order_date'],
    incremental_strategy='merge'
) }}

SELECT 
    DISTINCT campaign_name,
    order_date,

    COUNT(order_id) OVER (
        PARTITION BY campaign_name ORDER BY order_date 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_conversions, 

    SUM(order_total) OVER (
        PARTITION BY campaign_name ORDER BY order_date 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_revenue,  

    SUM(koinz_commission) OVER (
        PARTITION BY campaign_name ORDER BY order_date 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_ad_profit,  

    -- Running Spend
    SUM(discount) OVER (
        PARTITION BY campaign_name ORDER BY order_date 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_ad_spend,  

    SUM(cashback_gained) OVER (
        PARTITION BY campaign_name ORDER BY order_date 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_incentives,  

    SUM(discount + cashback_gained) OVER (
        PARTITION BY campaign_name ORDER BY order_date 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_campaign_cost  

FROM {{ ref('source') }} 
WHERE campaign_name IS NOT NULL 
AND campaign_name <> 'NO PROMO'

{% if is_incremental() %}

    AND order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}

ORDER BY order_date
