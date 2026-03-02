{{
    config(
        materialized='view',
        tags=['intermediate', 'positions', 'valuations']
    )
}}

-- =========================================================
-- INTERMEDIATE MODEL: Current Positions with Market Value
-- =========================================================
-- PURPOSE:
-- Takes the latest position for each portfolio/security
-- and joins with current market prices to calculate:
-- 1. Current market value
-- 2. Unrealized P&L (absolute)
-- 3. Unrealized P&L (percentage)
--
-- BUSINESS LOGIC:
-- Market Value    = quantity × current_price
-- Unrealized P&L  = market_value - cost_basis
-- Unrealized P&L% = (unrealized_pnl / cost_basis) × 100
-- =========================================================

with latest_positions as (
    -- Get only the most recent position for each
    -- portfolio/security combination
    select
        portfolio_id,
        security_id,
        position_date,
        cumulative_quantity,
        cumulative_cost_basis,
        avg_cost_per_unit,
        currency,

        -- Rank by date descending to get latest
        row_number() over (
            partition by portfolio_id, security_id
            order by position_date desc
        ) as rn

    from {{ ref('int_position_daily') }}
),

current_positions as (
    -- Keep only the latest position record
    select
        portfolio_id,
        security_id,
        position_date,
        cumulative_quantity,
        cumulative_cost_basis,
        avg_cost_per_unit,
        currency
    from latest_positions
    where rn = 1
),

latest_prices as (
    -- Get only the most recent price for each security
    select
        security_id,
        price_date,
        close_price,
        currency as price_currency,

        -- Rank by date descending to get latest
        row_number() over (
            partition by security_id
            order by price_date desc
        ) as rn

    from {{ ref('stg_market_prices') }}
),

current_prices as (
    -- Keep only the latest price record
    select
        security_id,
        price_date as valuation_date,
        close_price as current_price,
        price_currency
    from latest_prices
    where rn = 1
),

positions_valued as (
    -- Join positions with current prices
    select
        -- Position info
        p.portfolio_id,
        p.security_id,
        p.position_date,
        p.cumulative_quantity        as quantity,
        p.cumulative_cost_basis      as cost_basis,
        p.avg_cost_per_unit,
        p.currency                   as position_currency,

        -- Price info
        pr.valuation_date,
        pr.current_price,
        pr.price_currency,

        -- Market value calculation
        -- How much is our position worth TODAY?
        p.cumulative_quantity * pr.current_price
            as market_value,

        -- Unrealized P&L calculation
        -- How much have we made/lost on paper?
        (p.cumulative_quantity * pr.current_price)
            - p.cumulative_cost_basis
            as unrealized_pnl,

        -- Unrealized P&L percentage
        -- What % return on our investment?
        case
            when p.cumulative_cost_basis != 0
            then (
                    (p.cumulative_quantity * pr.current_price)
                    - p.cumulative_cost_basis
                 )
                 / abs(p.cumulative_cost_basis) * 100
            else 0
        end as unrealized_pnl_pct

    from current_positions p
    left join current_prices pr
        on p.security_id = pr.security_id
)

select
    *,
    current_timestamp() as dbt_loaded_at
from positions_valued