-- =========================================================
-- Model: ai_kpi_context
-- Layer: AI / Semantic + Narrative
-- Purpose: Business-explainable AI grounding layer
-- =========================================================

select

    'TOTAL_SALES' as KPI_NAME,

    'Total revenue generated from all closed homebuilder contracts' as BUSINESS_DEFINITION,

    sum(CONTRACT_PRICE) as CURRENT_VALUE,

    'Primary revenue driver for company performance' as BUSINESS_IMPORTANCE,

    case
        when sum(CONTRACT_PRICE) > 10000000 then 'Strong performance'
        when sum(CONTRACT_PRICE) between 5000000 and 10000000 then 'Moderate performance'
        else 'Below expectations'
    end as PERFORMANCE_STATE

from {{ ref('stg_homebuilder_sales') }}

union all

select

    'AVG_DAYS_TO_CLOSE',
    'Average number of days required to close a home sale',
    avg(DAYS_TO_CLOSE),
    'Operational efficiency indicator',
    case
        when avg(DAYS_TO_CLOSE) < 30 then 'Efficient'
        when avg(DAYS_TO_CLOSE) between 30 and 60 then 'Normal'
        else 'Slow cycle'
    end

from {{ ref('stg_homebuilder_sales') }}

union all

select

    'AVG_CONTRACT_PRICE',
    'Average selling price per home contract across all regions',
    avg(CONTRACT_PRICE),
    'Measures pricing strength and product mix',
    case
        when avg(CONTRACT_PRICE) > 500000 then 'High value mix'
        when avg(CONTRACT_PRICE) between 300000 and 500000 then 'Mid-market mix'
        else 'Low value mix'
    end

from {{ ref('stg_homebuilder_sales') }}