import sys
print("RUNNING FROM:", sys.executable)
import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# =========================
# CONFIG
# =========================
SNOWFLAKE_CONFIG = {
    "user": "",
    "password": "",
    "account": "",
    "warehouse": "",
    "database": "",
    "schema": ""
}

#DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

# =========================
# CONNECTION
# =========================
def get_connection():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    conn.cursor().execute("USE DATABASE db_Real_Estate")
    conn.cursor().execute("USE SCHEMA RAW")
    return conn

# =========================
# CREATE TABLES
# =========================
def create_tables(cursor):

    cursor.execute("""
    CREATE OR REPLACE TABLE HOMEBUILDER_SALES (
        CONTRACT_ID STRING,
        COMMUNITY STRING,
        CITY STRING,
        REGION STRING,
        PLAN_NAME STRING,
        SQFT FLOAT,
        BEDROOMS INT,
        BATHROOMS FLOAT,
        BASE_PRICE FLOAT,
        UPGRADE_AMOUNT FLOAT,
        INCENTIVE_AMOUNT FLOAT,
        CONTRACT_PRICE FLOAT,
        CONTRACT_DATE DATE,
        CLOSE_DATE DATE,
        DAYS_TO_CLOSE INT,
        STATUS STRING,
        BUYER_SOURCE STRING,
        AGENT_COMMISSION FLOAT,
        LOAN_TYPE STRING,
        SALES_CONSULTANT STRING
    )
    """)

    cursor.execute("""
    CREATE OR REPLACE TABLE REGIONAL_MANAGER_LOOKUP (
        REGION STRING,
        REGIONAL_MANAGER STRING,
        SALES_TARGET_UNITS INT,
        MARGIN_TARGET_PCT FLOAT
    )
    """)

# =========================
# LOAD FILES
# =========================
def load_files():

    sales_path = os.path.join(DATA_DIR, "HOMEBUILDER_SALES.csv")
    lookup_path = os.path.join(DATA_DIR, "REGIONAL_MANAGER_LOOKUP.xlsx")

    sales_df = pd.read_csv(sales_path)
    lookup_df = pd.read_excel(lookup_path)

    # =========================
    # CLEAN DATA
    # =========================

    sales_df["Contract_Date"] = pd.to_datetime(sales_df["Contract_Date"], errors='coerce').dt.date
    sales_df["Close_Date"] = pd.to_datetime(sales_df["Close_Date"], errors='coerce').dt.date

    # Replace NaN / NaT with None (required for Snowflake)
    sales_df = sales_df.where(pd.notnull(sales_df), None)
    lookup_df = lookup_df.where(pd.notnull(lookup_df), None)

    # Ensure correct column order
    #sales_df = sales_df[
    #    [
    #        "Contract_ID","Community","City","Region","Plan_Name","Sqft",
    #        "Bedrooms","Bathrooms","Base_Price","Upgrade_Amount",
    #        "Incentive_Amount","Contract_Price","Contract_Date","Close_Date",
    #        "Days_to_Close","Status","Buyer_Source","Agent_Commission",
    #        "Loan_Type","Sales_Consultant"
    #    ]
    #]

    sales_df = sales_df.rename(columns={
        "Contract_ID": "CONTRACT_ID",
        "Community": "COMMUNITY",
        "City": "CITY",
        "Region": "REGION",
        "Plan_Name": "PLAN_NAME",
        "Sqft": "SQFT",
        "Bedrooms": "BEDROOMS",
        "Bathrooms": "BATHROOMS",
        "Base_Price": "BASE_PRICE",
        "Upgrade_Amount": "UPGRADE_AMOUNT",
        "Incentive_Amount": "INCENTIVE_AMOUNT",
        "Contract_Price": "CONTRACT_PRICE",
        "Contract_Date": "CONTRACT_DATE",
        "Close_Date": "CLOSE_DATE",
        "Days_to_Close": "DAYS_TO_CLOSE",
        "Status": "STATUS",
        "Buyer_Source": "BUYER_SOURCE",
        "Agent_Commission": "AGENT_COMMISSION",
        "Loan_Type": "LOAN_TYPE",
        "Sales_Consultant": "SALES_CONSULTANT"
    })

    #lookup_df = lookup_df[
    #    ["Region","Regional_Manager","Sales_Target_Units","Margin_Target_Pct"]
    #]

    lookup_df = lookup_df.rename(columns={
        "Region": "REGION",
        "Regional_Manager": "REGIONAL_MANAGER",
        "Sales_Target_Units": "SALES_TARGET_UNITS",
        "Margin_Target_Pct": "MARGIN_TARGET_PCT"
    })
    return sales_df, lookup_df

# =========================
# MAIN
# =========================
def main():

    print("Starting Snowflake Load Script...")

    conn = get_connection()
    cursor = conn.cursor()

    print("Connected to Snowflake")

    # Create tables
    create_tables(cursor)
    print("Tables created")

    # Load data
    sales_df, lookup_df = load_files()

    # =========================
    # LOAD TO SNOWFLAKE (FAST WAY)
    # =========================
    print("Loading HOMEBUILDER_SALES...")
    success, nchunks, nrows, _ = write_pandas(conn, sales_df, "HOMEBUILDER_SALES")
    print(f"HOMEBUILDER_SALES Loaded: {success}, Rows: {nrows}")

    print("Loading REGIONAL_MANAGER_LOOKUP...")
    success, nchunks, nrows, _ = write_pandas(conn, lookup_df, "REGIONAL_MANAGER_LOOKUP")
    print(f"REGIONAL_MANAGER_LOOKUP Loaded: {success}, Rows: {nrows}")

    # Verify
    cursor.execute("SELECT COUNT(*) FROM HOMEBUILDER_SALES")
    print("HOMEBUILDER_SALES COUNT:", cursor.fetchone())

    cursor.execute("SELECT COUNT(*) FROM REGIONAL_MANAGER_LOOKUP")
    print("REGIONAL_MANAGER_LOOKUP COUNT:", cursor.fetchone())

    conn.close()

    print("DONE - Data successfully loaded into Snowflake!")

# =========================
# RUN
# =========================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR OCCURRED:")
        print(e)