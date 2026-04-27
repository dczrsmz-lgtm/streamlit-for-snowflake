-- =========================================================
-- Model: dim_region
-- Layer: Mart (Dimension)
-- Purpose: Enables slicing, targeting, and performance comparison
-- =========================================================

with sales as (
    select *
    from db_Real_Estate.staging.stg_homebuilder_sales
),

region_lookup as (
    select *
    from db_Real_Estate.staging.stg_regional_lookup
)

select

    -- =========================
    -- DIMENSION KEY
    -- =========================
    s.REGION,

    -- =========================
    -- CONTEXT (slice/dice fields)
    -- =========================
    r.REGIONAL_MANAGER,

    -- =========================
    -- ACTUAL PERFORMANCE
    -- =========================
    count(s.CONTRACT_ID) as TOTAL_CONTRACTS,
    sum(s.CONTRACT_PRICE) as TOTAL_SALES,
    avg(s.CONTRACT_PRICE) as AVG_SALES_PRICE,
    avg(s.DAYS_TO_CLOSE) as AVG_DAYS_TO_CLOSE,

    -- =========================
    -- TARGETS (FROM LOOKUP)
    -- =========================
    max(r.SALES_TARGET_UNITS) as SALES_TARGET_UNITS,
    max(r.MARGIN_TARGET_PCT) as MARGIN_TARGET_PCT,

    -- =========================
    -- PERFORMANCE GAP (VERY IMPORTANT)
    -- =========================
    count(s.CONTRACT_ID) - max(r.SALES_TARGET_UNITS) as CONTRACT_GAP

from sales s

left join region_lookup r
    on s.REGION = r.REGION

group by
    s.REGION,
    r.REGIONAL_MANAGER