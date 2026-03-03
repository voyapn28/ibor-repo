{{
    config(
        materialized='view',
        tags=['staging', 'prices']
    )
}}

-- Clean and type market price data from raw source
with source as (
    select * from {{ source('raw_data', 'market_prices') }}
),

renamed as (
    select
        -- Primary key
        price_id,
        
        -- Foreign key
        security_id,
        
        -- Date
        price_date::date as price_date,
        
        -- OHLC prices
        open_price::number(18,4) as open_price,
        high_price::number(18,4) as high_price,
        low_price::number(18,4) as low_price,
        close_price::number(18,4) as close_price,
        
        -- Volume
        volume::number(18,0) as volume,
        
        -- Currency and source
        currency,
        source as price_source,
        
        -- Audit
        created_at::timestamp as created_at,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from source
)

select * from renamed
