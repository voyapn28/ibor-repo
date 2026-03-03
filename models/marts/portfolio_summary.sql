{{
    config(
        materialized='table',
        tags=['marts', 'portfolio', 'summary']
    )
}}

-- =========================================================
-- MART MODEL: Portfolio Summary Dashboard
-- =========================================================
-- PURPOSE:
-- Executive-level portfolio metrics and performance summary.
-- One row per portfolio showing total value, P&L, and allocations.
--
-- USERS:
-- - CIO / Senior Management (portfolio oversight)
-- - Clients (monthly statements)
-- - Board reporting
--
-- BUSINESS QUESTIONS ANSWERED:
-- - What's the total portfolio value?
-- - How much are we up/down overall?
-- - What's our asset allocation?
-- - Which portfolio is performing best?
-- =========================================================

with holdings as (
    select * from {{ ref('portfolio_holdings') }}
),

portfolio_metrics as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_type,
        portfolio_manager,
        base_currency,
        
        -- Count metrics
        count(distinct security_id) as num_holdings,
        count(distinct sector) as num_sectors,
        count(distinct country) as num_countries,
        
        -- Value metrics (in base currency)
        sum(cost_basis) as total_cost_basis,
        sum(market_value) as total_market_value,
        sum(unrealized_pnl) as total_unrealized_pnl,
        
        -- Performance
        round(
            (sum(unrealized_pnl) / nullif(sum(cost_basis), 0)) * 100,
            2
        ) as total_return_pct,
        
        -- Largest position
        max(position_weight_pct) as largest_position_pct,
        
        -- Asset allocation
        sum(case when asset_class = 'Equity' then market_value else 0 end) 
            as equity_value,
        sum(case when asset_class = 'Fixed Income' then market_value else 0 end) 
            as fixed_income_value,
        
        -- Sector concentration (top sector %)
        max(sector_value) as top_sector_value
        
    from holdings
    left join (
        -- Calculate sector totals
        select
            portfolio_id,
            sector,
            sum(market_value) as sector_value
        from holdings
        group by portfolio_id, sector
    ) sector_totals using (portfolio_id)
    
    group by 
        portfolio_id,
        portfolio_name,
        portfolio_type,
        portfolio_manager,
        base_currency
),

asset_allocation as (
    select
        portfolio_id,
        
        -- Asset class percentages
        round(
            (equity_value / nullif(total_market_value, 0)) * 100,
            1
        ) as equity_allocation_pct,
        
        round(
            (fixed_income_value / nullif(total_market_value, 0)) * 100,
            1
        ) as fixed_income_allocation_pct,
        
        -- Concentration metrics
        round(
            (top_sector_value / nullif(total_market_value, 0)) * 100,
            1
        ) as top_sector_concentration_pct
        
    from portfolio_metrics
),

final as (
    select
        -- Portfolio identification
        pm.portfolio_id,
        pm.portfolio_name,
        pm.portfolio_type,
        pm.portfolio_manager,
        pm.base_currency,
        
        -- Portfolio size
        pm.total_cost_basis,
        pm.total_market_value,
        
        -- Performance
        pm.total_unrealized_pnl,
        pm.total_return_pct,
        
        -- Diversification
        pm.num_holdings,
        pm.num_sectors,
        pm.num_countries,
        pm.largest_position_pct,
        
        -- Asset allocation
        aa.equity_allocation_pct,
        aa.fixed_income_allocation_pct,
        aa.top_sector_concentration_pct,
        
        -- Risk indicators
        case
            when pm.largest_position_pct > 10 then 'High Concentration Risk'
            when pm.largest_position_pct > 5 then 'Medium Concentration Risk'
            else 'Diversified'
        end as concentration_risk_flag,
        
        case
            when pm.total_return_pct > 10 then 'Outperforming'
            when pm.total_return_pct > 0 then 'Positive'
            when pm.total_return_pct > -5 then 'Slight Loss'
            else 'Underperforming'
        end as performance_status,
        
        -- Metadata
        current_timestamp() as report_date,
        current_timestamp() as dbt_loaded_at
        
    from portfolio_metrics pm
    left join asset_allocation aa 
        on pm.portfolio_id = aa.portfolio_id
)

select * from final
order by total_market_value desc
