-- =========================================================
-- Model: monthly_trends
-- Layer: Mart
-- Purpose: Time-series sales performance
-- =========================================================

select

    date_trunc('month', CONTRACT_DATE) as MONTH,

    count(*) as TOTAL_CONTRACTS,

    sum(CONTRACT_PRICE) as TOTAL_SALES,

    avg(CONTRACT_PRICE) as AVG_CONTRACT_PRICE

from {{ ref('stg_homebuilder_sales') }}

group by 1
order by 1