{{
    config(
        materialized='view',
        tags=['staging', 'transactions']
    )
}}

-- Clean and type transaction data from raw source
with source as (
    select * from {{ source('raw_data', 'transactions') }}
),

renamed as (
    select
        -- Primary key
        transaction_id,
        
        -- Foreign keys
        portfolio_id,
        security_id,
        
        -- Transaction details
        transaction_date::date as transaction_date,
        settlement_date::date as settlement_date,
        transaction_type,
        
        -- Quantities and amounts
        quantity::number(18,4) as quantity,
        price::number(18,4) as price,
        transaction_amount::number(18,2) as transaction_amount,
        currency,
        
        -- Execution details
        broker,
        trader_id,
        status,
        
        -- Audit timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from source
)

select * from renamed
