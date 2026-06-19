"""
Serving-layer dashboard — reads the dbt marts from DuckDB and shows both
business metrics (GMV, orders, cancellation rate) and a data-quality scorecard
(duplicates collapsed, orphan rate, null rate, schema-drift coverage).

On first run (e.g. fresh Streamlit Cloud deploy) it bootstraps the warehouse
from the committed, deterministic CDC events: generate → load → dbt build.
"""

import os
import sys
import subprocess
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="CDC ELT Pipeline", page_icon="📊", layout="wide")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "warehouse.duckdb")


def bootstrap():
    """Build the warehouse from scratch if it doesn't exist yet."""
    env = {**os.environ, "DUCKDB_PATH": DB}
    py = sys.executable  # use the same interpreter Streamlit runs on
    subprocess.run([py, "generator/cdc_generator.py", "--events", "5000",
                    "--seed", "42", "--out", "raw_events"], cwd=ROOT, check=True)
    subprocess.run([py, "ingestion/load_raw.py", "--db", DB,
                    "--events-dir", "raw_events"], cwd=ROOT, check=True)
    subprocess.run([py, "-m", "dbt.cli.main", "deps", "--profiles-dir", "."],
                   cwd=f"{ROOT}/dbt", check=True, env=env)
    subprocess.run([py, "-m", "dbt.cli.main", "build", "--profiles-dir", "."],
                   cwd=f"{ROOT}/dbt", check=True, env=env)


if not os.path.exists(DB):
    with st.spinner("First run — building the warehouse (generate → load → dbt build)..."):
        try:
            bootstrap()
        except Exception as e:
            st.error(f"Bootstrap failed: {e}")
            st.stop()


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    con = duckdb.connect(DB, read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


st.title("📊 CDC → ELT Pipeline")
st.caption("Debezium-style CDC · DuckDB · dbt · Airflow · automated data-quality gates")

# ── Headline KPIs ─────────────────────────────────────────────────────────────
kpis = q("""
    select
        count(*) as orders,
        count(distinct customer_id) as customers,
        round(sum(case when is_completed then amount else 0 end), 0) as gmv,
        round(sum(case when is_lost then 1 else 0 end) * 100.0 / count(*), 1) as cancel_rate
    from main_marts.fct_orders
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Orders processed", f"{int(kpis.orders):,}")
c2.metric("Customers", f"{int(kpis.customers):,}")
c3.metric("GMV (completed)", f"₹{int(kpis.gmv):,}")
c4.metric("Cancellation rate", f"{kpis.cancel_rate}%")

# ── Data-quality scorecard ────────────────────────────────────────────────────
st.subheader("🔬 Data-Quality Scorecard")

raw_orders = q("select count(*) c, count(distinct event_id) d from raw.raw_orders_cdc").iloc[0]
raw_cust = q("select count(*) c, count(distinct event_id) d from raw.raw_customers_cdc").iloc[0]
orphan = q("select round(sum(case when is_orphan_order then 1 else 0 end)*100.0/count(*),2) p from main_marts.fct_orders").iloc[0].p
null_email = q("select round(sum(case when email is null then 1 else 0 end)*100.0/count(*),2) p from main_marts.dim_customers").iloc[0].p
drift = q("select round(sum(case when currency != 'INR' then 1 else 0 end)*100.0/count(*),1) p from main_marts.fct_orders").iloc[0].p
future = q("select count(*) c from main_marts.fct_orders where created_at > current_timestamp").iloc[0].c

d1, d2, d3, d4 = st.columns(4)
d1.metric("Orphan-order rate", f"{orphan}%", help="Orders whose customer never appeared — gate fails above 5%")
d2.metric("Null email rate", f"{null_email}%", help="Null-flood injected upstream; tracked as warn-level signal")
d3.metric("Future-dated orders", int(future), help="Hard DQ gate — must be 0")
d4.metric("Schema-drift coverage", f"{drift}%", help="Orders carrying the late-added `currency` field")

st.info(
    f"**Idempotent load:** collapsed "
    f"{int(raw_orders.c - raw_orders.d) + int(raw_cust.c - raw_cust.d)} redelivered CDC events "
    f"to a single row each. Raw events landed: {int(raw_orders.c + raw_cust.c):,}."
)

# ── Business charts ───────────────────────────────────────────────────────────
st.subheader("📈 Daily GMV")
daily = q("""
    select order_date, sum(gmv) as gmv, sum(total_orders) as orders
    from main_marts.agg_daily_revenue group by 1 order by 1
""")
st.line_chart(daily, x="order_date", y="gmv")

colA, colB = st.columns(2)
with colA:
    st.subheader("🌍 GMV by country")
    by_country = q("""
        select country, sum(gmv) as gmv from main_marts.agg_daily_revenue
        where country is not null group by 1 order by 2 desc
    """)
    st.bar_chart(by_country, x="country", y="gmv")

with colB:
    st.subheader("🏆 Top customers")
    top = q("""
        select full_name, tier, lifetime_orders, round(lifetime_gmv,0) as lifetime_gmv
        from main_marts.dim_customers order by lifetime_gmv desc limit 10
    """)
    st.dataframe(top, hide_index=True, use_container_width=True)

st.caption("Pipeline: generator → idempotent loader (DuckDB raw) → dbt (staging→marts, incremental, SCD2) → DQ gate → this dashboard")
