import os
import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="CDC ELT Pipeline", page_icon="📊", layout="wide")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "warehouse.duckdb")

# ── Theme ─────────────────────────────────────────────────────────────────────
ACCENT = "#f97316"
INK = "#0f172a"
GRID = "rgba(148,163,184,0.12)"
PALETTE = ["#f97316", "#6366f1", "#22c55e", "#eab308", "#ec4899", "#06b6d4", "#a855f7", "#ef4444"]

ALPHA2_TO_ISO3 = {
    "IN": "IND", "US": "USA", "GB": "GBR", "AE": "ARE", "SG": "SGP",
    "AU": "AUS", "DE": "DEU", "CA": "CAN",
}
ALPHA2_FLAG = {
    "IN": "🇮🇳", "US": "🇺🇸", "GB": "🇬🇧", "AE": "🇦🇪", "SG": "🇸🇬",
    "AU": "🇦🇺", "DE": "🇩🇪", "CA": "🇨🇦",
}

st.markdown(f"""
<style>
.stApp {{ background: {INK}; }}
#MainMenu, footer, header {{ visibility: hidden; }}
.block-container {{ padding-top: 2rem; max-width: 1400px; }}
.hero {{
    background: linear-gradient(135deg, #1e1b4b 0%, #0f172a 55%, #3b1108 100%);
    border: 1px solid rgba(249,115,22,0.25); border-radius: 18px;
    padding: 26px 30px; margin-bottom: 18px;
}}
.hero h1 {{ margin: 0; font-size: 2em; color: #fff;
    background: linear-gradient(135deg,#f97316,#fb923c,#fbbf24);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
.hero p {{ margin: 6px 0 0; color: #94a3b8; font-size: 0.95em; }}
.kpi {{
    background: linear-gradient(135deg,#1e293b,#162032);
    border: 1px solid rgba(255,255,255,0.06); border-radius: 14px;
    padding: 16px 18px; height: 100%;
}}
.kpi .lab {{ color:#64748b; font-size:0.72em; text-transform:uppercase; letter-spacing:0.06em; }}
.kpi .val {{ color:#fff; font-size:1.7em; font-weight:700; line-height:1.1; margin-top:4px; }}
.kpi .sub {{ color:#94a3b8; font-size:0.78em; margin-top:2px; }}
.pill {{ display:inline-block; padding:3px 10px; border-radius:20px; font-size:0.72em;
    background:rgba(34,197,94,0.15); color:#4ade80; border:1px solid rgba(34,197,94,0.3); }}
.stTabs [data-baseweb="tab-list"] {{ gap: 4px; }}
.stTabs [data-baseweb="tab"] {{ background:#1e293b; border-radius:8px 8px 0 0; padding:8px 16px; color:#94a3b8; }}
.stTabs [aria-selected="true"] {{ background:{ACCENT}; color:#fff; }}
</style>
""", unsafe_allow_html=True)

if not os.path.exists(DB):
    st.error("warehouse.duckdb not found. Build it: generate → load → dbt build.")
    st.stop()


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    con = duckdb.connect(DB, read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def style(fig, h=320, title=None):
    fig.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#cbd5e1", size=12),
        margin=dict(l=10, r=10, t=40 if title else 10, b=10), height=h,
        title=dict(text=title, font=dict(size=15, color="#fff")) if title else None,
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(gridcolor=GRID, zerolinecolor=GRID)
    fig.update_yaxes(gridcolor=GRID, zerolinecolor=GRID)
    return fig


def gauge(value, title, suffix="%", good_low=True, vmax=100, thresh=5):
    # color: green if within tolerance, red if breached
    if good_low:
        color = "#22c55e" if value <= thresh else ("#eab308" if value <= thresh * 2 else "#ef4444")
    else:
        color = "#22c55e" if value >= thresh else "#ef4444"
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        number={"suffix": suffix, "font": {"size": 26, "color": "#fff"}},
        title={"text": title, "font": {"size": 13, "color": "#94a3b8"}},
        gauge={
            "axis": {"range": [0, vmax], "tickcolor": "#475569"},
            "bar": {"color": color, "thickness": 0.75},
            "bgcolor": "#1e293b", "borderwidth": 0,
            "steps": [{"range": [0, vmax], "color": "#0f172a"}],
        },
    ))
    return style(fig, h=220)


# ── Data ──────────────────────────────────────────────────────────────────────
kpis = q("""select count(*) orders, count(distinct customer_id) customers,
    round(sum(case when is_completed then amount else 0 end),0) gmv,
    round(sum(case when is_lost then 1 else 0 end)*100.0/count(*),1) cancel_rate,
    round(avg(amount),0) aov from main_marts.fct_orders""").iloc[0]

raw_o = q("select count(*) c from raw.raw_orders_cdc").iloc[0].c
raw_c = q("select count(*) c from raw.raw_customers_cdc").iloc[0].c
orphan = q("select round(sum(case when is_orphan_order then 1 else 0 end)*100.0/count(*),2) p from main_marts.fct_orders").iloc[0].p
null_email = q("select round(sum(case when email is null then 1 else 0 end)*100.0/count(*),2) p from main_marts.dim_customers").iloc[0].p
future = int(q("select count(*) c from main_marts.fct_orders where created_at > current_timestamp").iloc[0].c)

# ── Hero ──────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="hero">
  <h1>📊 CDC → ELT Pipeline</h1>
  <p>Debezium-style CDC · DuckDB · dbt · Airflow · automated data-quality gates
     &nbsp;·&nbsp; <span class="pill">● pipeline healthy</span></p>
</div>
""", unsafe_allow_html=True)

# ── KPI row ───────────────────────────────────────────────────────────────────
cards = [
    ("Orders processed", f"{int(kpis.orders):,}", "current-state"),
    ("Customers", f"{int(kpis.customers):,}", "deduped"),
    ("GMV (completed)", f"₹{int(kpis.gmv):,}", "delivered only"),
    ("Avg order value", f"₹{int(kpis.aov):,}", "per order"),
    ("Cancellation rate", f"{kpis.cancel_rate}%", "cancelled+refunded"),
    ("Raw CDC events", f"{int(raw_o + raw_c):,}", "idempotent load"),
]
cols = st.columns(6)
for col, (lab, val, sub) in zip(cols, cards):
    col.markdown(f'<div class="kpi"><div class="lab">{lab}</div>'
                 f'<div class="val">{val}</div><div class="sub">{sub}</div></div>',
                 unsafe_allow_html=True)

st.write("")
tab1, tab2, tab3, tab4 = st.tabs(["📈 Overview", "💰 Business", "🔬 Data Quality", "🔧 Pipeline"])

# ════════════════════════════ OVERVIEW ════════════════════════════════════════
with tab1:
    daily = q("""select order_date, sum(gmv) gmv, sum(total_orders) orders,
        sum(completed_orders) completed, sum(lost_orders) lost
        from main_marts.agg_daily_revenue group by 1 order by 1""")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily.order_date, y=daily.gmv, fill="tozeroy",
        line=dict(color=ACCENT, width=2), fillcolor="rgba(249,115,22,0.15)", name="GMV"))
    st.plotly_chart(style(fig, 300, "Daily GMV (₹)"), use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        status = q("select status, count(*) n from main_marts.fct_orders group by 1")
        order_map = {s: i for i, s in enumerate(
            ["pending", "confirmed", "shipped", "delivered", "cancelled", "refunded"])}
        status["o"] = status.status.map(order_map)
        status = status.sort_values("o")
        fig = go.Figure(go.Bar(x=status.n, y=status.status, orientation="h",
            marker=dict(color=status.n, colorscale="Oranges"), text=status.n, textposition="auto"))
        st.plotly_chart(style(fig, 320, "Orders by status"), use_container_width=True)
    with c2:
        tier = q("select tier, count(*) n from main_marts.dim_customers group by 1")
        fig = go.Figure(go.Pie(labels=tier.tier, values=tier.n, hole=0.55,
            marker=dict(colors=PALETTE)))
        fig.update_traces(textinfo="label+percent")
        st.plotly_chart(style(fig, 320, "Customer tiers"), use_container_width=True)

    # daily orders completed vs lost
    fig = go.Figure()
    fig.add_trace(go.Bar(x=daily.order_date, y=daily.completed, name="Completed", marker_color="#22c55e"))
    fig.add_trace(go.Bar(x=daily.order_date, y=daily.lost, name="Lost", marker_color="#ef4444"))
    fig.update_layout(barmode="stack")
    st.plotly_chart(style(fig, 280, "Daily orders — completed vs lost"), use_container_width=True)

# ════════════════════════════ BUSINESS ════════════════════════════════════════
with tab2:
    by_country = q("""select country, sum(gmv) gmv, sum(total_orders) orders
        from main_marts.agg_daily_revenue where country is not null group by 1 order by 2 desc""")
    by_country["iso3"] = by_country.country.map(ALPHA2_TO_ISO3)
    by_country["flag"] = by_country.country.map(ALPHA2_FLAG).fillna("")

    c1, c2 = st.columns([3, 2])
    with c1:
        fig = px.choropleth(by_country.dropna(subset=["iso3"]), locations="iso3",
            color="gmv", color_continuous_scale="Oranges",
            projection="natural earth", hover_name="country")
        fig.update_geos(bgcolor="rgba(0,0,0,0)", lakecolor="rgba(0,0,0,0)",
            landcolor="#1e293b", showcountries=True, countrycolor="#334155")
        st.plotly_chart(style(fig, 360, "GMV by country (world projection)"), use_container_width=True)
    with c2:
        fig = go.Figure(go.Bar(
            x=by_country.gmv, y=by_country.flag + " " + by_country.country,
            orientation="h", marker=dict(color=by_country.gmv, colorscale="Oranges")))
        fig.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(style(fig, 360, "GMV ranking"), use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        trend = q("""select order_date, round(avg(cancellation_rate_pct),1) rate
            from main_marts.agg_daily_revenue group by 1 order by 1""")
        fig = go.Figure(go.Scatter(x=trend.order_date, y=trend.rate, mode="lines",
            line=dict(color="#ef4444", width=2), fill="tozeroy", fillcolor="rgba(239,68,68,0.1)"))
        st.plotly_chart(style(fig, 300, "Cancellation rate trend (%)"), use_container_width=True)
    with c4:
        top = q("""select full_name, tier, lifetime_orders, round(lifetime_gmv,0) gmv
            from main_marts.dim_customers order by lifetime_gmv desc limit 10""")
        fig = go.Figure(go.Bar(x=top.gmv, y=top.full_name, orientation="h",
            marker=dict(color=top.gmv, colorscale="Purples"), text=top.tier))
        fig.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(style(fig, 300, "Top 10 customers by lifetime GMV"), use_container_width=True)

# ════════════════════════════ DATA QUALITY ════════════════════════════════════
with tab3:
    st.markdown("##### Data-quality gates (green = within tolerance)")
    g1, g2, g3, g4 = st.columns(4)
    g1.plotly_chart(gauge(float(orphan), "Orphan-order rate", vmax=10, thresh=5), use_container_width=True)
    g2.plotly_chart(gauge(float(null_email), "Null email rate", vmax=10, thresh=5), use_container_width=True)
    g3.plotly_chart(gauge(float(future), "Future-dated orders", suffix="", vmax=10, thresh=0), use_container_width=True)
    drift = q("select round(sum(case when currency!='INR' then 1 else 0 end)*100.0/count(*),1) p from main_marts.fct_orders").iloc[0].p
    g4.plotly_chart(gauge(float(drift), "Schema-drift coverage", good_low=False, vmax=100, thresh=1), use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        ops = q("""select op, count(*) n from (
            select op from raw.raw_orders_cdc union all select op from raw.raw_customers_cdc) group by 1""")
        op_label = {"c": "Create", "u": "Update", "d": "Delete", "r": "Snapshot"}
        ops["label"] = ops.op.map(op_label).fillna(ops.op)
        fig = go.Figure(go.Pie(labels=ops.label, values=ops.n, hole=0.5,
            marker=dict(colors=["#22c55e", "#6366f1", "#ef4444", "#eab308"])))
        fig.update_traces(textinfo="label+value")
        st.plotly_chart(style(fig, 320, "CDC events by operation type"), use_container_width=True)
    with c2:
        drift_ts = q("""select cast(to_timestamp(ts_ms/1000) as date) d,
            round(avg(case when json_extract_string(after_json,'$.currency') is not null
                then 1.0 else 0.0 end)*100,1) pct
            from raw.raw_orders_cdc where op='c' group by 1 order by 1""")
        fig = go.Figure(go.Scatter(x=drift_ts.d, y=drift_ts.pct, mode="lines",
            line=dict(color="#a855f7", width=2), fill="tozeroy", fillcolor="rgba(168,85,247,0.12)"))
        st.plotly_chart(style(fig, 320, "Schema drift onset — % of orders carrying `currency`"),
                        use_container_width=True)

    cdc_vol = q("""select cast(to_timestamp(ts_ms/1000) as date) d, count(*) n
        from raw.raw_orders_cdc group by 1 order by 1""")
    fig = go.Figure(go.Bar(x=cdc_vol.d, y=cdc_vol.n, marker_color="#06b6d4"))
    st.plotly_chart(style(fig, 260, "Raw CDC event volume per day (orders)"), use_container_width=True)

# ════════════════════════════ PIPELINE ════════════════════════════════════════
with tab4:
    st.markdown("##### Pipeline lineage & row counts")
    counts = {
        "raw.orders": int(raw_o), "raw.customers": int(raw_c),
        "stg_orders": int(q("select count(*) c from main_staging.stg_orders").iloc[0].c),
        "stg_customers": int(q("select count(*) c from main_staging.stg_customers").iloc[0].c),
        "int_orders": int(q("select count(*) c from main_intermediate.int_orders_enriched").iloc[0].c),
        "fct_orders": int(kpis.orders),
        "dim_customers": int(kpis.customers),
        "agg_daily": int(q("select count(*) c from main_marts.agg_daily_revenue").iloc[0].c),
    }
    labels = list(counts.keys())
    idx = {k: i for i, k in enumerate(labels)}
    node_colors = ["#475569", "#475569", "#6366f1", "#6366f1", "#eab308", "#f97316", "#f97316", "#22c55e"]
    links = [
        ("raw.orders", "stg_orders"), ("raw.customers", "stg_customers"),
        ("stg_orders", "int_orders"), ("stg_customers", "int_orders"),
        ("int_orders", "fct_orders"), ("stg_customers", "dim_customers"),
        ("int_orders", "dim_customers"), ("fct_orders", "agg_daily"),
    ]
    fig = go.Figure(go.Sankey(
        node=dict(label=[f"{k} ({counts[k]:,})" for k in labels], color=node_colors,
                  pad=18, thickness=18, line=dict(width=0)),
        link=dict(
            source=[idx[a] for a, _ in links], target=[idx[b] for _, b in links],
            value=[counts[a] for a, _ in links],
            color="rgba(148,163,184,0.2)"),
    ))
    st.plotly_chart(style(fig, 380, "raw (bronze) → staging (silver) → marts (gold)"),
                    use_container_width=True)

    st.markdown("""
    | Layer | Models | What happens |
    |---|---|---|
    | **Bronze (raw)** | `raw_orders_cdc`, `raw_customers_cdc` | Append-only CDC events; deduped on `event_id` at load |
    | **Silver (staging)** | `stg_orders`, `stg_customers` | Collapse CDC log → latest state; handle soft-deletes, type & schema drift |
    | **Silver (intermediate)** | `int_orders_enriched` | Join orders↔customers, flag orphans |
    | **Gold (marts)** | `fct_orders` (incremental), `dim_customers`, `agg_daily_revenue` | Business-ready facts, dims & aggregates |
    | **History** | `customers_snapshot` | SCD Type-2 tier history |
    """)

st.caption("Built with DuckDB · dbt · Airflow · Plotly · Streamlit  ·  "
           "[GitHub](https://github.com/Riiyansh/cdc-elt-pipeline)")
