	{{
    config(
        materialized='view',
        tags=['staging', 'fx']
    )
}}

-- Clean and type FX rates from raw source
with source as (
    select * from {{ source('raw_data', 'fx_rates') }}
),

renamed as (
    select
        -- Primary key
        rate_id,
        
        -- Currency pair
        from_currency,
        to_currency,
        
        -- Rate details
        rate_date::date as rate_date,
        exchange_rate::number(18,6) as exchange_rate,
        
        -- Source
        source as rate_source,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from source
)

select * from renamed