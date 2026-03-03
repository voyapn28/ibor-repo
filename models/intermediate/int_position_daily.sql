{{
    config(
        materialized='view',
        tags=['intermediate', 'positions']
    )
}}

-- =========================================================
-- INTERMEDIATE MODEL: Daily Position Calculations
-- =========================================================
-- PURPOSE:
-- Calculates cumulative positions for each portfolio/security
-- combination from transaction history.
-- Uses settlement_date (not transaction_date) because
-- positions legally change on settlement, not trade date.
--
-- BUSINESS LOGIC:
-- BUY  = positive quantity (+)
-- SELL = negative quantity (-)
-- Running total = current position
-- =========================================================

with transactions as (
    -- Only include settled transactions
    -- Pending/Cancelled don't affect positions
    select * from {{ ref('stg_transactions') }}
    where status = 'SETTLED'
),

position_changes as (
    select
        portfolio_id,
        security_id,
        settlement_date,
        currency,

        -- Convert BUY/SELL into +/- quantities
        case
            when transaction_type = 'BUY'  then quantity
            when transaction_type = 'SELL' then -quantity
        end as position_change,

        -- Convert BUY/SELL into +/- cost
        case
            when transaction_type = 'BUY'  then transaction_amount
            when transaction_type = 'SELL' then -transaction_amount
        end as cost_change

    from transactions
),

daily_positions as (
    select
        portfolio_id,
        security_id,
        settlement_date as position_date,
        currency,

        -- Running total of quantity (cumulative sum)
        -- This is the POSITION - how many units we own
        sum(position_change) over (
            partition by portfolio_id, security_id
            order by settlement_date
            rows between unbounded preceding and current row
        ) as cumulative_quantity,

        -- Running total of cost (cumulative sum)
        -- This is the COST BASIS - how much we paid
        sum(cost_change) over (
            partition by portfolio_id, security_id
            order by settlement_date
            rows between unbounded preceding and current row
        ) as cumulative_cost_basis

    from position_changes
),

final as (
    select
        portfolio_id,
        security_id,
        position_date,
        currency,
        cumulative_quantity,
        cumulative_cost_basis,

        -- Average cost per unit
        -- Used for P&L calculation on sells
        case
            when cumulative_quantity != 0
            then cumulative_cost_basis / cumulative_quantity
            else 0
        end as avg_cost_per_unit,

        current_timestamp() as dbt_loaded_at

    from daily_positions

    -- Only show open positions (quantity > 0)
    -- Closed positions (sold everything) are excluded
    where cumulative_quantity > 0
)

select * from final