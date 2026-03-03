{{
    config(
        materialized='table',
        tags=['marts', 'portfolio', 'holdings']
    )
}}

-- =========================================================
-- MART MODEL: Portfolio Holdings Report
-- =========================================================
-- PURPOSE:
-- Daily position-level report showing what each portfolio
-- owns, current market value, and P&L.
--
-- USERS:
-- - Portfolio Managers (daily P&L monitoring)
-- - Risk teams (concentration analysis)
-- - Client service (answering position queries)
--
-- BUSINESS QUESTIONS ANSWERED:
-- - What securities do we own?
-- - What's the current market value?
-- - Are we making or losing money?
-- - What % of the portfolio is each position?
-- =========================================================

with current_positions as (
    select * from {{ ref('int_current_positions') }}
),

securities as (
    select
        security_id,
        ticker,
        security_name,
        asset_class,
        sector,
        country
    from {{ ref('stg_securities') }}
),

portfolios as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_type,
        portfolio_manager,
        base_currency
    from {{ ref('stg_portfolios') }}
),

fx_rates as (
    select
        from_currency,
        to_currency,
        exchange_rate
    from {{ ref('stg_fx_rates') }}
    where rate_date = (select max(rate_date) from {{ ref('stg_fx_rates') }})
),

holdings_with_fx as (
    select
        -- Portfolio dimensions
        cp.portfolio_id,
        p.portfolio_name,
        p.portfolio_type,
        p.portfolio_manager,
        p.base_currency,
        
        -- Security dimensions
        cp.security_id,
        s.ticker,
        s.security_name,
        s.asset_class,
        s.sector,
        s.country,
        
        -- Position metrics
        cp.quantity,
        cp.avg_cost_per_unit,
        cp.cost_basis,
        
        -- Pricing
        cp.current_price,
        cp.valuation_date,
        cp.position_currency,
        
        -- FX conversion
        coalesce(fx.exchange_rate, 1.0) as fx_rate,
        
        -- Market value in position currency
        cp.market_value as market_value_local,
        
        -- Market value in portfolio base currency
        cp.market_value * coalesce(fx.exchange_rate, 1.0) as market_value_base,
        
        -- Cost basis in base currency
        cp.cost_basis * coalesce(fx.exchange_rate, 1.0) as cost_basis_base,
        
        -- Unrealized P&L in base currency
        (cp.market_value * coalesce(fx.exchange_rate, 1.0)) 
            - (cp.cost_basis * coalesce(fx.exchange_rate, 1.0)) 
            as unrealized_pnl_base,
        
        -- Unrealized P&L percentage
        cp.unrealized_pnl_pct

    from current_positions cp
    left join securities s 
        on cp.security_id = s.security_id
    left join portfolios p 
        on cp.portfolio_id = p.portfolio_id
    left join fx_rates fx 
        on cp.position_currency = fx.from_currency 
        and p.base_currency = fx.to_currency
),

portfolio_totals as (
    -- Calculate total portfolio value for weight calculations
    select
        portfolio_id,
        sum(market_value_base) as total_portfolio_value
    from holdings_with_fx
    group by portfolio_id
),

final as (
    select
        -- Portfolio info
        h.portfolio_id,
        h.portfolio_name,
        h.portfolio_type,
        h.portfolio_manager,
        h.base_currency,
        
        -- Security info
        h.security_id,
        h.ticker,
        h.security_name,
        h.asset_class,
        h.sector,
        h.country,
        
        -- Position details
        h.quantity,
        h.avg_cost_per_unit,
        h.current_price,
        h.valuation_date,
        
        -- Values in base currency
        h.cost_basis_base as cost_basis,
        h.market_value_base as market_value,
        h.unrealized_pnl_base as unrealized_pnl,
        h.unrealized_pnl_pct,
        
        -- Position weight (% of portfolio)
        round(
            (h.market_value_base / pt.total_portfolio_value) * 100, 
            2
        ) as position_weight_pct,
        
        -- FX info
        h.position_currency,
        h.fx_rate,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from holdings_with_fx h
    left join portfolio_totals pt 
        on h.portfolio_id = pt.portfolio_id
)

select * from final
order by 
    portfolio_id, 
    market_value desc