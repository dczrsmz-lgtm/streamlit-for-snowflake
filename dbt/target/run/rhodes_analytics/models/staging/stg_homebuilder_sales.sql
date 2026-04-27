
  create or replace   view db_Real_Estate.staging.stg_homebuilder_sales
  
  
  
  
  as (
    -- =========================================================
-- Model: stg_homebuilder_sales
-- Layer: Staging
-- Purpose: Clean and standardize raw sales data
-- =========================================================

select

    -- Primary Identifiers
    cast(CONTRACT_ID as varchar) as CONTRACT_ID,

    -- Location Attributes
    cast(COMMUNITY as varchar) as COMMUNITY,
    cast(CITY as varchar) as CITY,
    cast(REGION as varchar) as REGION,

    -- Product Attributes
    cast(PLAN_NAME as varchar) as PLAN_NAME,
    cast(SQFT as integer) as SQFT,
    cast(BEDROOMS as integer) as BEDROOMS,
    cast(BATHROOMS as number(3,1)) as BATHROOMS,

    -- Financials
    cast(BASE_PRICE as number(12,2)) as BASE_PRICE,
    cast(UPGRADE_AMOUNT as number(12,2)) as UPGRADE_AMOUNT,
    cast(INCENTIVE_AMOUNT as number(12,2)) as INCENTIVE_AMOUNT,
    cast(CONTRACT_PRICE as number(12,2)) as CONTRACT_PRICE,
    cast(AGENT_COMMISSION as number(12,2)) as AGENT_COMMISSION,

    -- Dates
    cast(CONTRACT_DATE as date) as CONTRACT_DATE,
    cast(CLOSE_DATE as date) as CLOSE_DATE,

    -- Performance Metrics
    cast(DAYS_TO_CLOSE as integer) as DAYS_TO_CLOSE,

    -- Status / Business Attributes
    cast(STATUS as varchar) as STATUS,
    cast(BUYER_SOURCE as varchar) as BUYER_SOURCE,
    cast(LOAN_TYPE as varchar) as LOAN_TYPE,
    cast(SALES_CONSULTANT as varchar) as SALES_CONSULTANT

from DB_REAL_ESTATE.RAW.HOMEBUILDER_SALES
  );

