-- =========================================================
-- Model: dim_consultant
-- Layer: Mart (Dimension)
-- Purpose: Sales consultant performance tracking
-- =========================================================

select

    SALES_CONSULTANT,

    count(*) as TOTAL_CONTRACTS,

    sum(CONTRACT_PRICE) as TOTAL_SALES,

    avg(CONTRACT_PRICE) as AVG_CONTRACT_PRICE,

    avg(DAYS_TO_CLOSE) as AVG_DAYS_TO_CLOSE

from {{ ref('stg_homebuilder_sales') }}

group by SALES_CONSULTANT