import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session

@st.cache_resource
def get_session():
    connection_parameters = {
        "account": st.secrets["SNOWFLAKE_ACCOUNT"],
        "user": st.secrets["SNOWFLAKE_USER"],
        "password": st.secrets["SNOWFLAKE_PASSWORD"],
        "role": st.secrets["SNOWFLAKE_ROLE"],
        "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
        "database": st.secrets["SNOWFLAKE_DATABASE"],
        "schema": st.secrets["SNOWFLAKE_SCHEMA"],
    }

    return Session.builder.configs(connection_parameters).create()

session = get_session()

# ============================
# PAGE CONFIG + THEME
# ============================
st.set_page_config(page_title="Rhodes Analytics", layout="wide")

st.markdown("""
    <style>
        /* MAIN APP BACKGROUND */
        .stApp {
            background-color: #f5f7fb;
            color: #111827;
        }

        /* SIDEBAR */
        section[data-testid="stSidebar"] {
            background-color: #ffffff;
            border-right: 1px solid #e5e7eb;
        }

        /* METRIC CARDS */
        .stMetric {
            background-color: #ffffff;
            padding: 14px;
            border-radius: 10px;
            border: 1px solid #e5e7eb;
            box-shadow: 0px 1px 3px rgba(0,0,0,0.05);
        }

        /* HEADERS */
        h1, h2, h3 {
            color: #111827;
        }

        /* EXPANDERS */
        .streamlit-expanderHeader {
            background-color: #ffffff;
        }

        /* DATAFRAMES */
        .dataframe {
            background-color: #ffffff;
        }
    </style>
""", unsafe_allow_html=True)

st.title("Rhodes Homebuilder Executive Dashboard")
st.caption("dbt Staging Layer + Snowflake + AI Analytics")
st.markdown("---")

# ============================
# SESSION
# ============================
@st.cache_resource
def get_session():
    return get_active_session()

@st.cache_data
def run_query(sql):
    return get_session().sql(sql).to_pandas()

# ============================
# LOAD STAGING DATA ONLY
# ============================
sales_df = run_query("""
    SELECT *
    FROM DB_REAL_ESTATE.STAGING.STG_HOMEBUILDER_SALES
""")

region_df = run_query("""
    SELECT *
    FROM DB_REAL_ESTATE.STAGING.STG_REGIONAL_LOOKUP
""")

sales_df["CONTRACT_DATE"] = pd.to_datetime(sales_df["CONTRACT_DATE"])
sales_df["MONTH"] = sales_df["CONTRACT_DATE"].dt.to_period("M").astype(str)

# ============================
# LAYOUT
# ============================
left, right = st.columns([3, 1])

# ============================
# FILTERS (RIGHT PANEL)
# ============================
with right:

    st.subheader("Filters")

    regions = st.multiselect("Region", sales_df["REGION"].dropna().unique())
    consultants = st.multiselect("Consultant", sales_df["SALES_CONSULTANT"].dropna().unique())

    filtered = sales_df.copy()

    if regions:
        filtered = filtered[filtered["REGION"].isin(regions)]

    if consultants:
        filtered = filtered[filtered["SALES_CONSULTANT"].isin(consultants)]

    st.markdown("---")

    # ============================
    # AI ASSISTANT
    # ============================
    st.subheader("AI Assistant")

    mode = st.selectbox(
        "Mode",
        ["Ask Question", "Forecast", "Anomaly Detection", "Summary"]
    )

    question = st.text_area("Ask a question", height=100)

    if st.button("Run Analysis"):

        session = get_session()

        prompt = f"""
        You are a business analyst.
        Use ONLY STG_HOMEBUILDER_SALES.
        Question: {question}
        """

        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large',
                $$ {prompt} $$
            )
        """).collect()[0][0]

        st.write(result)

# ============================
# LEFT DASHBOARD
# ============================
with left:

    # ============================
    # KPI GAUGES (NEW)
    # ============================
    st.subheader("Executive KPIs")

    total_sales = filtered["CONTRACT_PRICE"].sum()
    avg_price = filtered["CONTRACT_PRICE"].mean()
    total_contracts = len(filtered)
    avg_days = filtered["DAYS_TO_CLOSE"].mean()

    # Targets (from region lookup if available)
    target_sales = region_df["SALES_TARGET_UNITS"].sum()

    def gauge(label, value, target):
        pct = min(value / target, 1) if target > 0 else 0

        st.markdown(f"### {label}")
        st.progress(pct)
        st.write(f"{value:,.0f} / {target:,.0f} ({pct*100:.1f}%)")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Sales", f"${total_sales:,.0f}")
        gauge("Sales Target Progress", total_sales, target_sales)

    with col2:
        st.metric("Avg Price", f"${avg_price:,.0f}")

    with col3:
        st.metric("Total Contracts", total_contracts)

    with col4:
        st.metric("Avg Days to Close", round(avg_days, 1))

    st.markdown("---")

    # ============================
    # CHARTS
    # ============================
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Sales by Region")
        st.bar_chart(filtered.groupby("REGION")["CONTRACT_PRICE"].sum())

    with c2:
        st.subheader("Monthly Trend")
        st.line_chart(filtered.groupby("MONTH")["CONTRACT_PRICE"].sum())

    # ============================
    # CONSULTANTS
    # ============================
    st.subheader("Consultant Performance")

    st.bar_chart(
        filtered.groupby("SALES_CONSULTANT")["CONTRACT_PRICE"].sum()
    )

    # ============================
    # REGION TARGETS
    # ============================
    st.subheader("Region vs Target Performance")

    merged = filtered.groupby("REGION")["CONTRACT_PRICE"].sum().reset_index()
    merged = merged.merge(region_df, on="REGION", how="left")

    merged["GAP"] = merged["CONTRACT_PRICE"] - merged["SALES_TARGET_UNITS"].fillna(0)

    st.dataframe(merged, use_container_width=True)

    # ============================
    # RAW VIEW
    # ============================
    with st.expander("Filtered Data"):
        st.dataframe(filtered, use_container_width=True)