
  
    

create or replace transient table db_Real_Estate.staging_marts.monthly_trends
    
    
    
    as (-- =========================================================
-- Model: monthly_trends
-- Layer: Mart
-- Purpose: Time-series sales performance
-- =========================================================

select

    date_trunc('month', CONTRACT_DATE) as MONTH,

    count(*) as TOTAL_CONTRACTS,

    sum(CONTRACT_PRICE) as TOTAL_SALES,

    avg(CONTRACT_PRICE) as AVG_CONTRACT_PRICE

from db_Real_Estate.staging.stg_homebuilder_sales

group by 1
order by 1
    )
;


  