-- =========================================================
-- Model: sales_forecast
-- Layer: ML / Analytics
-- Purpose: Simple time-series forecast baseline for sales
-- =========================================================

with monthly as (

    select
        date_trunc('month', CONTRACT_DATE) as MONTH,
        sum(CONTRACT_PRICE) as TOTAL_SALES
    from {{ ref('stg_homebuilder_sales') }}
    group by 1
),

features as (

    select
        MONTH,
        TOTAL_SALES,
        row_number() over (order by MONTH) as TIME_INDEX
    from monthly
)

select

    MONTH,
    TOTAL_SALES,

    -- Simple linear trend projection (baseline forecast)
    avg(TOTAL_SALES) over (
        order by TIME_INDEX
        rows between 2 preceding and 0 preceding
    ) as MOVING_AVG_3M,

    -- Trend indicator
    TOTAL_SALES -
    lag(TOTAL_SALES) over (order by MONTH) as MOM_CHANGE

from features