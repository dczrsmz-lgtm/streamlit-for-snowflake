# DBT Project Overview

## Project Description
This project is a modern data analytics platform built using **Snowflake**, **DBT**, **Python ingestion scripts**, and **Streamlit (Streamlit for Snowflake)** for interactive dashboards and reporting.

---

## Project Structure

```
streamlit-for-snowflake/
│
├── data/                  # Source datasets, DBT analyses and ad-hoc files
│   ├── Homebuilder_Sales.csv
│   ├── Regional_Manager_Lookup.xlsx
│
├── DBT/                   # DBT models, transformations, and configurations
│   ├── analyses
│   ├── logs
│   ├── macros
│   ├── models
│   │   ├── kpi
│   │   │   └── kpi_summary.sql
│   │   ├── marts
│   │   │   ├── dim_consultant.sql
│   │   │   ├── dim_region.sql
│   │   │   ├── fct_sales.sql
│   │   │   └── monthly_trends.sql
│   │   ├── ml
│   │   │   └── sales_forecast.sql
│   │   ├── semantic
│   │   │   └── ai_kpi_context.sql
│   │   ├── staging
│   │   │   ├── stg_homebuilder_sales.sql
│   │   │   └── stg_regional_lookup.sql
│   │   └── schema.yml
│   ├── seeds
│   ├── target
│   ├── tests
│   └── dbt_project.yml
│
├── Ingestion/            # Data ingestion scripts
│   └── load_to_snowflake.py
│
├── Rhodes_app/           # Streamlit application
│   ├── app.py
│   └── deploy.py
│
└── requirements.txt      # Python dependencies
```

---

## Components

### 1. Ingestion Layer
- Location: `Ingestion/load_to_snowflake.py`
- Purpose:
  - Load raw CSV and Excel files into Snowflake
  - Prepare data for DBT transformations
- Technology: Python, Snowflake Connector / Snowpark

---

### 2. DBT Layer
- Location: `DBT/`
- Purpose:
  - Transform raw data into analytical models
  - Build dimensional models (facts & dimensions)
  - Support KPI reporting, forecasting, and semantic modeling

Includes:
- **Staging models** (raw data standardization)
- **Marts** (dimensional modeling)
- **KPI layer** (business metrics)
- **ML layer** (forecasting models)
- **Semantic layer** (AI-ready context)

---

### 3. Data Layer
- Location: `data/`
- Purpose:
  - Source datasets used for ingestion and DBT models
  - Supports sales and regional analysis

Files:
- Homebuilder sales transactions
- Regional manager lookup data

---

### 4. Streamlit Application
- Location: `Rhodes_app/`
- Purpose:
  - Interactive analytics dashboard
  - KPI visualization from Snowflake
  - Business reporting layer
  - Deployed via Streamlit for Snowflake

Files:
- `app.py` → main dashboard application
- `deploy.py` → deployment configuration or helper script

---

## Requirements

Install dependencies using:

```bash
pip install -r requirements.txt
```

### Key Dependencies
```
streamlit
snowflake-snowpark-python
pandas
sqlalchemy
dbt-core
dbt-snowflake
openpyxl
```

---

## Streamlit for Snowflake
This project leverages **Streamlit for Snowflake** to build interactive dashboards directly connected to Snowflake for real-time analytics.

---

## How to Run

### 1. Data Ingestion
```bash
python Ingestion/load_to_snowflake.py
```

### 2. Run DBT Models
```bash
dbt run
```

### 3. Launch Streamlit App
```bash
streamlit run Rhodes_app/app.py
```

---

## Architecture Overview
- **Ingestion Layer** → Loads raw files into Snowflake
- **DBT Layer** → Transforms data into analytics-ready models
- **Data Layer** → Source datasets for processing
- **Presentation Layer** → Streamlit dashboards

---

## Notes
- Use environment variables or secrets management for Snowflake credentials
- Maintain separation between ingestion, transformation, and presentation layers
- DBT models follow modular, scalable design (staging → marts → KPI)

---

## Author
Data Engineering & Analytics Project – Streamlit + DBT + Snowflake Pipeline

