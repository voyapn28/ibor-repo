{{
    config(
        materialized='view',
        tags=['staging', 'securities']
    )
}}

-- Clean and type securities master data from raw source
with source as (
    select * from {{ source('raw_data', 'securities_master') }}
),

renamed as (
    select
        -- Primary key
        security_id,
        
        -- Identifiers
        isin,
        cusip,
        ticker,
        security_name,
        
        -- Classifications
        asset_class,
        sector,
        market_cap_category,
        
        -- Geographic
        country,
        exchange,
        
        -- Currency
        currency,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from source
)

select * from renamed