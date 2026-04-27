import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

# ============================
# PAGE CONFIG
# ============================
st.set_page_config(page_title="Rhodes Analytics", layout="wide")

# ============================
# SNOWFLAKE SESSION (TOML + CACHED)
# ============================
@st.cache_resource
def get_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()

session = get_session()

# ============================
# CACHED QUERY FUNCTION
# ============================
@st.cache_data(ttl=300)
def run_query(sql):
    return session.sql(sql).to_pandas()

# ============================
# LOAD DATA (DBT STAGING LAYER)
# ============================
sales_df = run_query("""
    SELECT *
    FROM DB_REAL_ESTATE.STAGING.STG_HOMEBUILDER_SALES
""")

region_df = run_query("""
    SELECT *
    FROM DB_REAL_ESTATE.STAGING.STG_REGIONAL_LOOKUP
""")

# ============================
# DATA PREP
# ============================
sales_df["CONTRACT_DATE"] = pd.to_datetime(sales_df["CONTRACT_DATE"])
sales_df["MONTH"] = sales_df["CONTRACT_DATE"].dt.to_period("M").astype(str)

# ============================
# THEME
# ============================
st.markdown("""
<style>
.stApp { background-color: #f5f7fb; }
</style>
""", unsafe_allow_html=True)

st.title("Rhodes Homebuilder Executive Dashboard")
st.caption("DBT + Snowflake + Streamlit Analytics Platform")
st.markdown("---")

# ============================
# LAYOUT
# ============================
left, right = st.columns([3, 1])

# ============================
# FILTERS
# ============================
with right:

    st.subheader("Filters")

    regions = st.multiselect(
        "Region",
        sales_df["REGION"].dropna().unique()
    )

    consultants = st.multiselect(
        "Consultant",
        sales_df["SALES_CONSULTANT"].dropna().unique()
    )

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

    question = st.text_area("Ask a question", height=100)

    if st.button("Run Analysis"):

        prompt = f"""
        You are a business analyst.
        Use ONLY STG_HOMEBUILDER_SALES.

        Question:
        {question}
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

    st.subheader("Executive KPIs")

    total_sales = filtered["CONTRACT_PRICE"].sum()
    avg_price = filtered["CONTRACT_PRICE"].mean()
    total_contracts = len(filtered)
    avg_days = filtered["DAYS_TO_CLOSE"].mean()

    target_sales = region_df["SALES_TARGET_UNITS"].sum()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Sales", f"${total_sales:,.0f}")
        st.progress(min(total_sales / target_sales, 1) if target_sales else 0)

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

    st.subheader("Consultant Performance")
    st.bar_chart(filtered.groupby("SALES_CONSULTANT")["CONTRACT_PRICE"].sum())

    # ============================
    # REGION PERFORMANCE
    # ============================
    st.subheader("Region vs Target Performance")

    merged = filtered.groupby("REGION")["CONTRACT_PRICE"].sum().reset_index()
    merged = merged.merge(region_df, on="REGION", how="left")

    merged["GAP"] = merged["CONTRACT_PRICE"] - merged["SALES_TARGET_UNITS"].fillna(0)

    st.dataframe(merged, use_container_width=True)

    # ============================
    # RAW DATA
    # ============================
    with st.expander("Filtered Data"):
        st.dataframe(filtered, use_container_width=True)