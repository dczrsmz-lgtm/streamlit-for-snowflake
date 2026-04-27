-- =========================================================
-- Model: fct_sales
-- Layer: Fact
-- Purpose: Central sales fact table for analytics
-- =========================================================

select

    CONTRACT_ID,
    REGION,
    COMMUNITY,
    SALES_CONSULTANT,
    CONTRACT_DATE,
    CLOSE_DATE,
    STATUS,

    CONTRACT_PRICE,
    DAYS_TO_CLOSE,

    -- Derived fields for analysis
    date_trunc('month', CONTRACT_DATE) as CONTRACT_MONTH,

    CONTRACT_PRICE - BASE_PRICE as NET_UPLIFT

from db_Real_Estate.staging.stg_homebuilder_sales