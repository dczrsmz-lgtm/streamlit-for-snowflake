
  
    

create or replace transient table db_Real_Estate.staging_kpi.kpi_summary
    
    
    
    as (-- =========================================================
-- Model: kpi_summary
-- Layer: KPI Mart
-- Purpose: Executive-level aggregated KPIs
-- =========================================================

select

    count(*) as TOTAL_CONTRACTS,

    sum(CONTRACT_PRICE) as TOTAL_SALES,

    avg(CONTRACT_PRICE) as AVG_CONTRACT_PRICE,

    avg(DAYS_TO_CLOSE) as AVG_DAYS_TO_CLOSE,

    sum(AGENT_COMMISSION) as TOTAL_COMMISSION

from db_Real_Estate.staging.stg_homebuilder_sales
    )
;


  