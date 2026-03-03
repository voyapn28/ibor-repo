{{
    config(
        materialized='incremental',
        unique_key='position_key',
        incremental_strategy='merge',
        tags=['intermediate', 'positions', 'incremental']
    )
}}

-- =========================================================
-- INCREMENTAL MODEL: Daily Position Snapshots
-- =========================================================
-- PURPOSE:
-- Store daily position snapshots for historical analysis.
-- In production, this table grows forever (multi-year history).
-- Incremental strategy ensures we only process NEW days.
--
-- INCREMENTAL LOGIC:
-- - First run: Process ALL historical positions
-- - Subsequent runs: Process only positions AFTER max date
-- - Merge strategy: Update if exists, insert if new
--
-- BUSINESS VALUE:
-- - Historical position reporting
-- - Point-in-time portfolio reconstruction
-- - Performance attribution over time
-- - Audit trail compliance
-- =========================================================

with daily_positions as (
    select * from {{ ref('int_position_daily') }}
    
    {% if is_incremental() %}
        -- On incremental runs, only process NEW dates
        -- This is the magic that makes it fast!
        where position_date > (select max(position_date) from {{ this }})
    {% endif %}
),

securities as (
    select
        security_id,
        ticker,
        security_name,
        asset_class,
        sector
    from {{ ref('stg_securities') }}
),

portfolios as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_manager
    from {{ ref('stg_portfolios') }}
),

enriched_positions as (
    select
        -- Composite unique key
        dp.portfolio_id || '_' || 
        dp.security_id || '_' || 
        to_char(dp.position_date, 'YYYYMMDD') as position_key,
        
        -- Date dimension
        dp.position_date,
        date_trunc('month', dp.position_date)::date as position_month,
        date_trunc('quarter', dp.position_date)::date as position_quarter,
        year(dp.position_date) as position_year,
        
        -- Portfolio dimension
        dp.portfolio_id,
        p.portfolio_name,
        p.portfolio_manager,
        
        -- Security dimension
        dp.security_id,
        s.ticker,
        s.security_name,
        s.asset_class,
        s.sector,
        
        -- Position metrics
        dp.cumulative_quantity as quantity,
        dp.cumulative_cost_basis as cost_basis,
        dp.avg_cost_per_unit,
        dp.currency,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at,
        
        -- Incremental tracking
        {% if is_incremental() %}
            'incremental_insert' as load_type
        {% else %}
            'full_refresh' as load_type
        {% endif %}
        
    from daily_positions dp
    left join securities s on dp.security_id = s.security_id
    left join portfolios p on dp.portfolio_id = p.portfolio_id
)

select * from enriched_positions