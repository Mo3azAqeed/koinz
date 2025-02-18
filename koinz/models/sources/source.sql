{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='staging',
    unique_key='order_id',
    on_schema_change='sync_all_columns'
) }}

-- This model extracts and transforms data from the main table into an analytics-friendly format.
-- It is an incremental model that appends new data, reducing query time and computation costs.

WITH source AS (
    SELECT
        -- Unique Identifiers
        index_id,
        order_id,
        date::DATE AS order_date,  -- Order date extracted from the source
        
        -- Customer & Partner Info
        customer_id,
        zone,
        cuisine,
        store_name,
        branch_name,
        MD5(store_name || branch_name) AS partner_id,  -- Generating a hashed partner ID
        
        -- Promotion Details
        promo_id,
        promo_name AS campaign_name,
        CAST(NULLIF(cashback_expiration_date, '0') AS DATE) AS cashback_expiration_date, -- Handling invalid dates
        
        -- Financial Metrics
        gmv::NUMERIC AS order_total, -- Gross Merchandise Value
        cashback_gained::NUMERIC(10, 2) AS cashback_gained, -- Cashback earned by the user
        cashback_remaining_snapshot::NUMERIC(10,2) AS cashback_remaining, -- Remaining cashback balance
        cashback_redeemed::NUMERIC(10, 2), -- Cashback used by the user
        discount::NUMERIC(10, 2), -- Discount applied on the order

        -- Derived Metrics
        CASE 
            WHEN gmv > 0 THEN (discount * 100.0) / NULLIF(gmv, 0)  
            ELSE 0 
        END AS discount_percentage, -- Percentage of discount relative to GMV

        -- Commission & Cost Allocation
        koinz_commission::NUMERIC(10, 2) AS koinz_commission, -- Koinz's commission on the order
        koinz_discount_share::NUMERIC(10, 2), -- Koinz's share of the discount
        merchant_discount_share::NUMERIC(10, 2), -- Merchant's share of the discount
        koinz_cashback_share::NUMERIC(10, 2), -- Koinz's share of the cashback
        merchant_cashback_share::NUMERIC(10, 2), -- Merchant's share of the cashback
        
        -- Metadata
        CURRENT_TIMESTAMP AS created_at  -- Timestamp for tracking data freshness

    FROM {{ source('koinz', 'main') }}
    
    {% if is_incremental() %}
    WHERE date::DATE >= (SELECT COALESCE(MAX(order_date), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT * FROM source
