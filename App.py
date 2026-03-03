import streamlit as st
import pandas as pd
from pyathena import connect
import plotly.express as px
import boto3

REGION = "us-east-1"
S3_STAGING = "s3://covid19-data-lake-mmeli/athena-results/"
DB_NAME = "covid19_db"

# ============================
# ATHENA CONNECTION
# ============================
@st.cache_resource
def get_connection():
    return connect(
        s3_staging_dir=S3_STAGING,
        region_name=REGION,
        schema_name=DB_NAME
    )

conn = get_connection()
# Fetch countries dynamically from Athena
query_countries = "SELECT DISTINCT country_region FROM covid19_processed ORDER BY country_region;"
countries = pd.read_sql(query_countries, conn)["country_region"].tolist()

country = st.sidebar.selectbox("Select Country", countries)

# Date range filter
min_date = pd.read_sql("SELECT MIN(date) as min_date FROM covid19_processed;", conn)["min_date"][0]
max_date = pd.read_sql("SELECT MAX(date) as max_date FROM covid19_processed;", conn)["max_date"][0]
date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date])
start_date, end_date = date_range

st.title("COVID-19 Data Lake Dashboard")
st.markdown("Built with AWS S3 + Glue + Athena + Streamlit")

# Total Cases
query_total = "SELECT SUM(cases) as total_cases FROM covid19_processed;"
total_cases = pd.read_sql(query_total, conn)["total_cases"][0]

# Total Countries
total_countries = len(countries)

# Highest Daily Spike
query_peak = f"""
SELECT MAX(daily_cases) as peak
FROM (
    SELECT date, SUM(cases) as daily_cases
    FROM covid19_processed
    GROUP BY date
) t;
"""
peak_daily = pd.read_sql(query_peak, conn)["peak"][0]

st.metric("Total Cases (Global)", f"{total_cases:,}")
st.metric("Total Countries", f"{total_countries}")
st.metric("Highest Daily Cases", f"{peak_daily:,}")


query_top = """
SELECT country_region,
       SUM(cases) AS total_cases
FROM covid19_processed
GROUP BY country_region
ORDER BY total_cases DESC
LIMIT 10;
"""
df_top = pd.read_sql(query_top, conn)
st.subheader("Top 10 Countries by Total Cases")
st.plotly_chart(px.bar(df_top, x="country_region", y="total_cases"), use_container_width=True)

query_daily = f"""
SELECT date,
       SUM(cases) AS daily_cases
FROM covid19_processed
WHERE country_region = '{country}'
AND date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY date
ORDER BY date;
"""
df_daily = pd.read_sql(query_daily, conn)
st.subheader(f"Daily Cases - {country}")
st.plotly_chart(px.line(df_daily, x="date", y="daily_cases"), use_container_width=True)

query_monthly = f"""
SELECT year,
       month(date) AS month,
       SUM(cases) AS monthly_cases
FROM covid19_processed
WHERE date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY year, month(date)
ORDER BY year, month;
"""
df_monthly = pd.read_sql(query_monthly, conn)
df_monthly["month_year"] = df_monthly["year"].astype(str) + "-" + df_monthly["month"].astype(str)
st.subheader("Monthly Global Cases")
st.plotly_chart(px.bar(df_monthly, x="month_year", y="monthly_cases"), use_container_width=True)

st.success("Dashboard connected to Amazon Athena successfully.")

