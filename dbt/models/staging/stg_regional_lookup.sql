-- =========================================================
-- Model: stg_regional_lookup
-- Layer: Staging
-- Purpose: Clean regional dimension for slicing & KPI joins
-- =========================================================

select

    cast(REGION as varchar) as REGION,
    cast(REGIONAL_MANAGER as varchar) as REGIONAL_MANAGER,

    -- Targets (used for performance slicing)
    cast(SALES_TARGET_UNITS as integer) as SALES_TARGET_UNITS,
    cast(MARGIN_TARGET_PCT as number(5,2)) as MARGIN_TARGET_PCT

from DB_REAL_ESTATE.RAW.REGIONAL_MANAGER_LOOKUP