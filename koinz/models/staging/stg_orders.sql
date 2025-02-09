{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='staging',
    unique_key='order_id',
    on_schema_change='sync_all_columns'
) }}

SELECT 
    -- Order Details
    order_id,
    order_date,
    order_total,

    -- Customer & Partner Info
    customer_id,
    partner_id,
    zone,
    cuisine,

    -- Promotion & Discounts
    campaign_name,
    discount,
    discount_percentage,

    -- Cashback & Commission
    cashback_gained,
    cashback_remaining,
    koinz_commission

FROM {{ ref('source') }} 

{% if is_incremental() %}

WHERE order_date >= (SELECT COALESCE(MAX(order_date), '1900-01-01') FROM {{ this }})
{% endif %}
