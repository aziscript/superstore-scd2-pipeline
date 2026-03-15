#!/usr/bin/env python3
"""
SuperStore SCD2 Analytics Dashboard
=====================================
Multi-page Streamlit dashboard for the Superstore data warehouse.
Pages: Overview · Products · Customers · SCD2 Explorer
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text

# ── Config ─────────────────────────────────────────────────────────────────────
DB_URL          = "postgresql://postgres:postgres123@localhost:5432/superstore_warehouse"
TEMPLATE        = "plotly_dark"
BG              = "rgba(0,0,0,0)"
GRID_COLOR      = "#2d3550"
BLUE            = "#00D4FF"
GREEN           = "#00FF87"
RED             = "#FF6B6B"
YELLOW          = "#FFD93D"
PROFIT_SCALE    = [RED, YELLOW, GREEN]

# ── Page setup ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SuperStore Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
/* ── Base ── */
.stApp                        { background-color: #0e1117; }
[data-testid="stSidebar"]     { background-color: #080b14;
                                 border-right: 1px solid #1e2640; }

/* ── KPI cards ── */
.kpi-card {
    background: linear-gradient(135deg, #141929, #1c2238);
    border: 1px solid #2a3354;
    border-radius: 14px;
    padding: 22px 16px;
    text-align: center;
    margin-bottom: 4px;
}
.kpi-value  { font-size: 2rem;  font-weight: 800; color: #00D4FF; margin: 0; }
.kpi-label  { font-size: .78rem; color: #7a86a3; margin: 0;
               text-transform: uppercase; letter-spacing: .07em; }

/* ── Version badges ── */
.badge-current {
    background:#00FF87; color:#0e1117;
    padding:2px 10px; border-radius:12px;
    font-size:.72rem; font-weight:700;
}
.badge-expired {
    background:#323552; color:#7a86a3;
    padding:2px 10px; border-radius:12px;
    font-size:.72rem;
}

/* ── Change diff cells ── */
.cell-changed {
    background:rgba(255,107,107,.12);
    border-left:3px solid #FF6B6B;
    border-radius:0 6px 6px 0;
    padding:6px 10px;
}
.cell-same { color:#7a86a3; padding:6px 10px; }
.cell-field { font-size:.72rem; color:#7a86a3; margin-bottom:2px; }
.cell-val   { font-size:.92rem; color:#e0e6ff; }
.cell-was   { font-size:.72rem; color:#FF6B6B; margin-top:2px; }

/* ── Section divider ── */
hr.dim { border:none; border-top:1px solid #1e2640; margin:14px 0; }
</style>
""", unsafe_allow_html=True)


# ── Database helpers ───────────────────────────────────────────────────────────
@st.cache_resource
def _engine():
    return create_engine(DB_URL, echo=False, pool_pre_ping=True)


@st.cache_data(ttl=300, show_spinner="Querying warehouse…")
def q(sql: str) -> pd.DataFrame:
    with _engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


# ── Shared chart defaults ──────────────────────────────────────────────────────
def _base_layout(**kw) -> dict:
    return dict(
        template=TEMPLATE,
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        margin=dict(t=24, b=24, l=8, r=8),
        **kw,
    )


def _grid(fig, axis="both"):
    if axis in ("x", "both"):
        fig.update_xaxes(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR)
    if axis in ("y", "both"):
        fig.update_yaxes(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR)
    return fig


# ── KPI card helper ────────────────────────────────────────────────────────────
def kpi(col, label: str, value, fmt: str = "$"):
    if fmt == "$":
        disp = f"${value:,.0f}"
    elif fmt == "%":
        disp = f"{value:.1f}%"
    elif fmt == "n":
        disp = f"{value:,.0f}"
    else:
        disp = str(value)
    with col:
        st.markdown(
            f'<div class="kpi-card">'
            f'<p class="kpi-label">{label}</p>'
            f'<p class="kpi-value">{disp}</p>'
            f'</div>',
            unsafe_allow_html=True,
        )


# ── Sidebar navigation ─────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 SuperStore")
    st.markdown("*Analytics Dashboard*")
    st.markdown("---")
    PAGE = st.radio(
        "nav",
        ["🏠  Overview", "📦  Products", "👥  Customers", "🔄  SCD2 Explorer"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown(
        "<small style='color:#4a5580'>Superstore Warehouse<br>SCD2 ETL Pipeline</small>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if PAGE == "🏠  Overview":
    st.title("🏠 Overview")
    st.caption("Business-level KPIs, trends, and market breakdown")

    # KPIs ────────────────────────────────────────────────────────────────────
    k = q("""
        SELECT
            SUM(sales)                              AS total_sales,
            SUM(profit)                             AS total_profit,
            COUNT(DISTINCT order_id)                AS total_orders,
            SUM(profit)/NULLIF(SUM(sales),0)*100   AS margin_pct,
            COUNT(DISTINCT customer_key)            AS customers
        FROM fact_orders
    """).iloc[0]

    c1, c2, c3, c4, c5 = st.columns(5)
    kpi(c1, "Total Sales",    k["total_sales"])
    kpi(c2, "Total Profit",   k["total_profit"])
    kpi(c3, "Orders",         k["total_orders"], "n")
    kpi(c4, "Profit Margin",  k["margin_pct"],   "%")
    kpi(c5, "Customers",      k["customers"],    "n")

    st.markdown("<br>", unsafe_allow_html=True)

    # Yearly Sales + Profit Trend ─────────────────────────────────────────────
    trend = q("""
        SELECT d.year, SUM(f.sales) AS sales, SUM(f.profit) AS profit
        FROM fact_orders f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year ORDER BY d.year
    """)

    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.subheader("Yearly Sales & Profit Trend")
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(x=trend["year"], y=trend["sales"],
                   name="Sales", marker_color=BLUE, opacity=0.8),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(x=trend["year"], y=trend["profit"],
                       name="Profit", line=dict(color=GREEN, width=3),
                       mode="lines+markers", marker=dict(size=9, color=GREEN)),
            secondary_y=True,
        )
        fig.update_layout(**_base_layout(height=360, legend=dict(orientation="h", y=1.08)))
        fig.update_yaxes(title_text="Sales ($)",  secondary_y=False, gridcolor=GRID_COLOR)
        fig.update_yaxes(title_text="Profit ($)", secondary_y=True,  gridcolor=GRID_COLOR)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Sales by Market")
        mkt = q("""
            SELECT c.market, SUM(f.sales) AS sales
            FROM fact_orders f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.market ORDER BY sales DESC
        """)
        fig2 = px.pie(
            mkt, values="sales", names="market", hole=0.48,
            color_discrete_sequence=["#00D4FF", "#00FF87", "#FFD93D",
                                     "#FF6B6B", "#a78bfa", "#fb923c"],
            template=TEMPLATE,
        )
        fig2.update_traces(textposition="outside", textinfo="percent+label",
                           pull=[0.04] + [0] * (len(mkt) - 1))
        fig2.update_layout(**_base_layout(height=360, showlegend=False))
        st.plotly_chart(fig2, use_container_width=True)

    # Year × Quarter heatmap ──────────────────────────────────────────────────
    st.subheader("Sales Heatmap — Year × Quarter")
    heat = q("""
        SELECT d.year, d.quarter, SUM(f.sales) AS sales
        FROM fact_orders f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.quarter ORDER BY d.year, d.quarter
    """)
    pivot = heat.pivot(index="year", columns="quarter", values="sales")
    pivot.columns = [f"Q{c}" for c in pivot.columns]

    fig3 = px.imshow(
        pivot, color_continuous_scale="Blues",
        text_auto="$,.0f", aspect="auto", template=TEMPLATE,
    )
    fig3.update_layout(**_base_layout(height=230, coloraxis_showscale=False))
    fig3.update_traces(textfont_size=12)
    st.plotly_chart(fig3, use_container_width=True)

    # Shipping mode mix ───────────────────────────────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Orders by Ship Mode")
        ship = q("""
            SELECT ship_mode, COUNT(DISTINCT order_id) AS orders, SUM(sales) AS sales
            FROM fact_orders GROUP BY ship_mode ORDER BY orders DESC
        """)
        fig4 = px.bar(
            ship, x="ship_mode", y="orders", color="sales",
            color_continuous_scale="Blues", template=TEMPLATE,
            labels={"ship_mode": "", "orders": "Orders", "sales": "Sales ($)"},
            text_auto=True,
        )
        fig4.update_layout(**_base_layout(height=300, coloraxis_showscale=False))
        _grid(fig4, "y")
        st.plotly_chart(fig4, use_container_width=True)

    with col_b:
        st.subheader("Monthly Sales Distribution")
        monthly = q("""
            SELECT d.month_name, d.month, SUM(f.sales) AS sales
            FROM fact_orders f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.month_name, d.month ORDER BY d.month
        """)
        fig5 = px.bar(
            monthly, x="month_name", y="sales",
            color="sales", color_continuous_scale="Blues",
            template=TEMPLATE,
            labels={"month_name": "", "sales": "Sales ($)"},
        )
        fig5.update_layout(**_base_layout(height=300, coloraxis_showscale=False))
        _grid(fig5, "y")
        st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE 2 — PRODUCTS
# ══════════════════════════════════════════════════════════════════════════════
elif PAGE == "📦  Products":
    st.title("📦 Products")
    st.caption("Sales performance, category mix, and profitability analysis")

    # Top N products ──────────────────────────────────────────────────────────
    top_n = st.slider("Top N products", 10, 30, 15, key="prod_top_n")
    top = q(f"""
        SELECT p.product_name, p.sub_category, p.category,
               SUM(f.sales) AS sales, SUM(f.profit) AS profit,
               SUM(f.quantity) AS units
        FROM fact_orders f
        JOIN dim_product_scd2 p ON f.product_key = p.product_key
        WHERE p.is_current = TRUE
        GROUP BY p.product_name, p.sub_category, p.category
        ORDER BY sales DESC LIMIT {top_n}
    """)

    col1, col2 = st.columns([3, 2])

    with col1:
        st.subheader(f"Top {top_n} Products by Sales")
        fig = px.bar(
            top.sort_values("sales"), x="sales", y="product_name",
            color="profit", color_continuous_scale=PROFIT_SCALE,
            orientation="h", template=TEMPLATE,
            hover_data={"sub_category": True, "units": True, "category": True},
            labels={"sales": "Sales ($)", "product_name": "", "profit": "Profit ($)"},
        )
        fig.update_layout(**_base_layout(
            height=max(380, top_n * 26),
            coloraxis_colorbar=dict(title="Profit"),
        ))
        _grid(fig, "x")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Profit Margin by Category")
        cat = q("""
            SELECT p.category,
                   SUM(f.sales) AS sales, SUM(f.profit) AS profit,
                   SUM(f.profit)/NULLIF(SUM(f.sales),0)*100 AS margin_pct
            FROM fact_orders f
            JOIN dim_product_scd2 p ON f.product_key = p.product_key
            GROUP BY p.category ORDER BY margin_pct DESC
        """)
        fig2 = px.bar(
            cat, x="category", y="margin_pct",
            color="margin_pct", color_continuous_scale=PROFIT_SCALE,
            template=TEMPLATE, text_auto=".1f",
            labels={"margin_pct": "Margin %", "category": ""},
        )
        fig2.update_traces(texttemplate="%{text}%", textposition="outside")
        fig2.update_layout(**_base_layout(height=380, coloraxis_showscale=False))
        _grid(fig2, "y")
        st.plotly_chart(fig2, use_container_width=True)

    # Treemap ─────────────────────────────────────────────────────────────────
    st.subheader("Sales Treemap — Category → Sub-Category")
    tree = q("""
        SELECT p.category, p.sub_category,
               SUM(f.sales) AS sales, SUM(f.profit) AS profit
        FROM fact_orders f
        JOIN dim_product_scd2 p ON f.product_key = p.product_key
        WHERE p.is_current = TRUE
        GROUP BY p.category, p.sub_category
    """)
    fig3 = px.treemap(
        tree, path=["category", "sub_category"],
        values="sales", color="profit",
        color_continuous_scale=[RED, "#1c2238", GREEN],
        color_continuous_midpoint=0,
        template=TEMPLATE,
        hover_data={"sales": ":$,.0f", "profit": ":$,.0f"},
    )
    fig3.update_traces(
        texttemplate="<b>%{label}</b><br>$%{value:,.0f}",
        root_color="#0e1117",
    )
    fig3.update_layout(**_base_layout(
        height=460,
        coloraxis_colorbar=dict(title="Profit ($)"),
    ))
    st.plotly_chart(fig3, use_container_width=True)

    # Sub-category scatter ────────────────────────────────────────────────────
    st.subheader("Sub-Category: Sales vs Profit (bubble = units sold)")
    subcat = q("""
        SELECT p.sub_category, p.category,
               SUM(f.sales) AS sales, SUM(f.profit) AS profit,
               SUM(f.quantity) AS units
        FROM fact_orders f
        JOIN dim_product_scd2 p ON f.product_key = p.product_key
        WHERE p.is_current = TRUE
        GROUP BY p.sub_category, p.category
    """)
    fig4 = px.scatter(
        subcat, x="sales", y="profit",
        size="units", color="category", hover_name="sub_category",
        size_max=50, template=TEMPLATE,
        color_discrete_sequence=[BLUE, GREEN, YELLOW],
        labels={"sales": "Total Sales ($)", "profit": "Total Profit ($)"},
    )
    fig4.add_hline(y=0, line_color="#8892b0", line_dash="dash", line_width=1)
    fig4.update_layout(**_base_layout(height=400))
    _grid(fig4)
    st.plotly_chart(fig4, use_container_width=True)

    # Loss-making products ────────────────────────────────────────────────────
    st.subheader("Loss-Making Products (current version, all-time)")
    loss = q("""
        SELECT p.product_name, p.category, p.sub_category,
               SUM(f.sales) AS sales, SUM(f.profit) AS profit,
               SUM(f.quantity) AS units,
               SUM(f.profit)/NULLIF(SUM(f.sales),0)*100 AS margin_pct
        FROM fact_orders f
        JOIN dim_product_scd2 p ON f.product_key = p.product_key
        WHERE p.is_current = TRUE
        GROUP BY p.product_name, p.category, p.sub_category
        HAVING SUM(f.profit) < 0
        ORDER BY profit ASC LIMIT 25
    """)
    if loss.empty:
        st.success("No loss-making products found among current versions.")
    else:
        st.caption(f"{len(loss)} products are unprofitable in aggregate")
        fig5 = px.bar(
            loss.sort_values("profit"), x="product_name", y="profit",
            color="category", template=TEMPLATE,
            color_discrete_sequence=[RED, YELLOW, BLUE],
            labels={"profit": "Total Profit ($)", "product_name": ""},
            hover_data={"margin_pct": ":.1f"},
        )
        fig5.add_hline(y=0, line_color="#8892b0", line_dash="dash")
        fig5.update_layout(**_base_layout(height=380))
        fig5.update_xaxes(tickangle=-40)
        _grid(fig5, "y")
        st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE 3 — CUSTOMERS
# ══════════════════════════════════════════════════════════════════════════════
elif PAGE == "👥  Customers":
    st.title("👥 Customers")
    st.caption("Segment performance, top spenders, and geographic distribution")

    # Segment overview ────────────────────────────────────────────────────────
    seg = q("""
        SELECT c.segment,
               SUM(f.sales)   AS sales,
               SUM(f.profit)  AS profit,
               COUNT(DISTINCT f.order_id) AS orders,
               COUNT(DISTINCT c.customer_key) AS customers,
               SUM(f.profit)/NULLIF(SUM(f.sales),0)*100 AS margin_pct
        FROM fact_orders f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.segment ORDER BY sales DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue & Profit by Segment")
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Sales",  x=seg["segment"], y=seg["sales"],
                             marker_color=BLUE,  opacity=0.85))
        fig.add_trace(go.Bar(name="Profit", x=seg["segment"], y=seg["profit"],
                             marker_color=GREEN, opacity=0.85))
        fig.update_layout(
            **_base_layout(height=360, barmode="group",
                           legend=dict(orientation="h", y=1.06)),
        )
        _grid(fig, "y")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Orders & Margin by Segment")
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_trace(
            go.Bar(x=seg["segment"], y=seg["orders"],
                   name="Orders", marker_color=YELLOW, opacity=0.85),
            secondary_y=False,
        )
        fig2.add_trace(
            go.Scatter(x=seg["segment"], y=seg["margin_pct"],
                       name="Margin %", mode="markers",
                       marker=dict(color=GREEN, size=16, symbol="diamond")),
            secondary_y=True,
        )
        fig2.update_layout(**_base_layout(height=360,
                                          legend=dict(orientation="h", y=1.06)))
        fig2.update_yaxes(title_text="Orders",    secondary_y=False, gridcolor=GRID_COLOR)
        fig2.update_yaxes(title_text="Margin %",  secondary_y=True,  gridcolor=GRID_COLOR)
        st.plotly_chart(fig2, use_container_width=True)

    # Top customers scatter ────────────────────────────────────────────────────
    top_cn = st.slider("Top N customers to display", 10, 40, 20, key="cust_top")
    top_cust = q(f"""
        SELECT c.customer_name, c.segment, c.market,
               SUM(f.sales) AS sales, SUM(f.profit) AS profit,
               COUNT(DISTINCT f.order_id) AS orders
        FROM fact_orders f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.customer_name, c.segment, c.market
        ORDER BY profit DESC LIMIT {top_cn}
    """)

    st.subheader(f"Top {top_cn} Customers by Profit (bubble = order count)")
    fig3 = px.scatter(
        top_cust, x="sales", y="profit",
        size="orders", color="segment", hover_name="customer_name",
        hover_data={"market": True, "orders": True},
        template=TEMPLATE,
        color_discrete_sequence=[BLUE, RED, YELLOW],
        size_max=30,
        labels={"sales": "Total Sales ($)", "profit": "Total Profit ($)"},
    )
    fig3.update_layout(**_base_layout(height=420))
    _grid(fig3)
    st.plotly_chart(fig3, use_container_width=True)

    # Region + Country ────────────────────────────────────────────────────────
    col3, col4 = st.columns(2)

    region = q("""
        SELECT c.region, SUM(f.sales) AS sales, SUM(f.profit) AS profit
        FROM fact_orders f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.region ORDER BY sales DESC
    """)
    with col3:
        st.subheader("Sales & Profit by Region")
        fig4 = px.bar(
            region.sort_values("sales"), x="sales", y="region",
            color="profit", color_continuous_scale=PROFIT_SCALE,
            orientation="h", template=TEMPLATE,
            labels={"sales": "Sales ($)", "region": "",
                    "profit": "Profit ($)"},
        )
        fig4.update_layout(**_base_layout(height=420,
                                          coloraxis_colorbar=dict(title="Profit")))
        _grid(fig4, "x")
        st.plotly_chart(fig4, use_container_width=True)

    country = q("""
        SELECT c.country, SUM(f.sales) AS sales, SUM(f.profit) AS profit
        FROM fact_orders f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.country ORDER BY sales DESC LIMIT 20
    """)
    with col4:
        st.subheader("Top 20 Countries by Sales")
        fig5 = px.bar(
            country.sort_values("sales"), x="sales", y="country",
            color="profit", color_continuous_scale=PROFIT_SCALE,
            orientation="h", template=TEMPLATE,
            labels={"sales": "Sales ($)", "country": "",
                    "profit": "Profit ($)"},
        )
        fig5.update_layout(**_base_layout(height=520,
                                          coloraxis_colorbar=dict(title="Profit")))
        _grid(fig5, "x")
        st.plotly_chart(fig5, use_container_width=True)

    # Segment × Market heatmap ────────────────────────────────────────────────
    st.subheader("Sales Heatmap — Segment × Market")
    seg_mkt = q("""
        SELECT c.segment, c.market, SUM(f.sales) AS sales
        FROM fact_orders f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.segment, c.market
    """)
    pivot = seg_mkt.pivot(index="segment", columns="market", values="sales").fillna(0)
    fig6 = px.imshow(
        pivot, color_continuous_scale="Blues",
        text_auto="$,.0f", aspect="auto", template=TEMPLATE,
    )
    fig6.update_layout(**_base_layout(height=260, coloraxis_showscale=False))
    st.plotly_chart(fig6, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE 4 — SCD2 EXPLORER
# ══════════════════════════════════════════════════════════════════════════════
elif PAGE == "🔄  SCD2 Explorer":
    st.title("🔄 SCD2 Explorer")
    st.caption(
        "Inspect every product's version history as tracked by the "
        "Slowly Changing Dimension Type 2 pipeline"
    )

    with st.expander("ℹ️  How SCD2 works in this pipeline"):
        st.markdown("""
**Slowly Changing Dimension Type 2 (SCD2)** preserves historical versions of product attributes.

Whenever a product's `product_name`, `category`, or `sub_category` changes in the source data:

| Action | What happens |
|--------|-------------|
| **Expire old version** | `end_date` is set to the day *before* the change; `is_current` → `FALSE` |
| **Open new version** | A new row is inserted with the updated attributes, a fresh `start_date`, and `is_current = TRUE` |

Every row in `fact_orders` points to the **exact product version** active at the time of purchase —
meaning historical reports stay accurate even if a product moves category years later.
        """)

    # ── Summary stats ─────────────────────────────────────────────────────────
    stats = q("""
        SELECT
            COUNT(DISTINCT product_id)                              AS total_products,
            COUNT(DISTINCT product_id) FILTER (WHERE version_count > 1) AS changed_products,
            MAX(version_count)                                      AS max_versions,
            SUM(version_count) - COUNT(DISTINCT product_id)        AS total_extra_versions
        FROM (
            SELECT product_id, COUNT(*) AS version_count
            FROM dim_product_scd2 GROUP BY product_id
        ) t
    """).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    kpi(c1, "Total Products",          stats["total_products"],        "n")
    kpi(c2, "Products with Changes",   stats["changed_products"],      "n")
    kpi(c3, "Max Versions (1 product)",stats["max_versions"],          "n")
    kpi(c4, "Total Extra Versions",    stats["total_extra_versions"],  "n")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Version distribution bar chart ────────────────────────────────────────
    st.subheader("How Many Products Have N Versions?")
    ver_dist = q("""
        SELECT version_count, COUNT(*) AS num_products
        FROM (
            SELECT product_id, COUNT(*) AS version_count
            FROM dim_product_scd2 GROUP BY product_id
        ) t
        GROUP BY version_count ORDER BY version_count
    """)
    fig_dist = px.bar(
        ver_dist, x="version_count", y="num_products",
        color="num_products", color_continuous_scale="Blues",
        template=TEMPLATE, text_auto=True,
        labels={"version_count": "Number of Versions",
                "num_products": "Number of Products"},
    )
    fig_dist.update_layout(**_base_layout(height=300, coloraxis_showscale=False))
    fig_dist.update_xaxes(dtick=1)
    _grid(fig_dist, "y")
    st.plotly_chart(fig_dist, use_container_width=True)

    st.markdown("---")

    # ── Product selector ──────────────────────────────────────────────────────
    st.subheader("Product Version History Explorer")

    multi = q("""
        SELECT product_id, COUNT(*) AS version_count,
               MIN(start_date) AS first_seen
        FROM dim_product_scd2
        GROUP BY product_id HAVING COUNT(*) > 1
        ORDER BY version_count DESC, product_id
    """)

    if multi.empty:
        st.info("No products with multiple versions found in the warehouse.")
        st.stop()

    col_search, col_sort = st.columns([3, 1])
    with col_search:
        search = st.text_input("Filter by product ID", placeholder="e.g. FUR-", key="scd2_filter")
    with col_sort:
        sort_by = st.selectbox("Sort by", ["Most versions", "Alphabetical"], key="scd2_sort")

    filtered = multi.copy()
    if search:
        filtered = filtered[filtered["product_id"].str.contains(search, case=False, na=False)]

    if sort_by == "Alphabetical":
        filtered = filtered.sort_values("product_id")

    if filtered.empty:
        st.warning("No products match your filter.")
        st.stop()

    selected_id = st.selectbox(
        f"Select product ({len(filtered)} with changes shown)",
        filtered["product_id"].tolist(),
        format_func=lambda pid: (
            f"{pid}  —  "
            f"{filtered.loc[filtered['product_id']==pid, 'version_count'].iloc[0]} versions"
        ),
        key="scd2_select",
    )

    # ── Load version history ──────────────────────────────────────────────────
    history = q(f"""
        SELECT
            product_key, product_id, product_name, category, sub_category,
            start_date, end_date, is_current,
            CASE
                WHEN end_date IS NULL THEN CURRENT_DATE - start_date
                ELSE end_date - start_date + 1
            END AS days_active
        FROM dim_product_scd2
        WHERE product_id = '{selected_id}'
        ORDER BY start_date
    """).reset_index(drop=True)

    n_versions = len(history)
    st.markdown(
        f"**{selected_id}** · **{n_versions} version{'s' if n_versions > 1 else ''}**"
    )

    # ── Gantt timeline ────────────────────────────────────────────────────────
    st.markdown("#### Version Timeline")

    today_str = str(pd.Timestamp.today().date())
    gantt_rows = []
    for i, row in history.iterrows():
        end_str = today_str if row["end_date"] is None else str(row["end_date"])
        gantt_rows.append({
            "Version":      f"v{i+1}",
            "Start":        str(row["start_date"]),
            "End":          end_str,
            "Product Name": row["product_name"],
            "Category":     row["category"],
            "Sub-Category": row["sub_category"],
            "Status":       "Current" if row["is_current"] else "Expired",
            "Days Active":  int(row["days_active"]),
        })
    gantt_df = pd.DataFrame(gantt_rows)

    fig_gantt = px.timeline(
        gantt_df,
        x_start="Start", x_end="End", y="Version",
        color="Status",
        color_discrete_map={"Current": GREEN, "Expired": "#3a3d5c"},
        hover_data={
            "Product Name": True, "Category": True,
            "Sub-Category": True, "Days Active": True,
        },
        text="Product Name",
        template=TEMPLATE,
    )
    fig_gantt.update_yaxes(autorange="reversed")
    fig_gantt.update_traces(textposition="inside", insidetextanchor="start",
                            textfont=dict(color="#0e1117", size=11))
    fig_gantt.update_layout(
        **_base_layout(
            height=max(220, n_versions * 64 + 80),
            legend=dict(orientation="h", y=1.1),
        )
    )
    _grid(fig_gantt, "x")
    st.plotly_chart(fig_gantt, use_container_width=True)

    # ── Version cards with diff highlighting ──────────────────────────────────
    st.markdown("#### Version Details & Change Log")

    ATTR_COLS = ["product_name", "category", "sub_category"]
    ATTR_LABELS = {"product_name": "Product Name",
                   "category": "Category",
                   "sub_category": "Sub-Category"}

    for i, row in history.iterrows():
        v_num    = i + 1
        is_cur   = row["is_current"]
        end_disp = "Present" if row["end_date"] is None else str(row["end_date"])
        badge    = (
            '<span class="badge-current">● CURRENT</span>'
            if is_cur
            else '<span class="badge-expired">○ EXPIRED</span>'
        )

        prev = history.iloc[v_num - 2] if v_num > 1 else None

        st.markdown(
            f"**Version {v_num}** &nbsp; {badge} &nbsp; "
            f"<small style='color:#7a86a3'>"
            f"{row['start_date']} → {end_disp} &nbsp; ({int(row['days_active'])} days)"
            f"</small>",
            unsafe_allow_html=True,
        )

        attr_cols = st.columns(3)
        for col_widget, attr in zip(attr_cols, ATTR_COLS):
            val     = row[attr]
            label   = ATTR_LABELS[attr]
            changed = prev is not None and val != prev[attr]
            with col_widget:
                if changed:
                    st.markdown(
                        f'<div class="cell-changed">'
                        f'<div class="cell-field">▲ {label} (changed)</div>'
                        f'<div class="cell-val">{val}</div>'
                        f'<div class="cell-was">← was: {prev[attr]}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="cell-same">'
                        f'<div class="cell-field">{label}</div>'
                        f'<div class="cell-val">{val}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

        # Revenue metrics for this specific version
        rev = q(f"""
            SELECT SUM(sales) AS sales, SUM(profit) AS profit,
                   COUNT(DISTINCT order_id) AS orders
            FROM fact_orders
            WHERE product_key = {int(row['product_key'])}
        """).iloc[0]

        m1, m2, m3, _ = st.columns([1, 1, 1, 1])
        m1.metric("Revenue (this version)", f"${rev['sales']:,.0f}" if rev["sales"] else "$0")
        m2.metric("Profit (this version)",  f"${rev['profit']:,.0f}" if rev["profit"] else "$0")
        m3.metric("Orders (this version)",  f"{int(rev['orders'])}"  if rev["orders"] else "0")

        st.markdown('<hr class="dim">', unsafe_allow_html=True)

    # ── Orders over time, split by version ────────────────────────────────────
    st.markdown("#### Monthly Sales — All Versions Overlaid")

    oot = q(f"""
        SELECT
            d.year, d.month,
            f.product_key,
            SUM(f.sales)  AS sales,
            SUM(f.profit) AS profit
        FROM fact_orders f
        JOIN dim_product_scd2 p ON f.product_key = p.product_key
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE p.product_id = '{selected_id}'
        GROUP BY d.year, d.month, f.product_key
        ORDER BY d.year, d.month
    """)

    if not oot.empty:
        # Map product_key → version label
        key_to_label = {
            row["product_key"]: f"v{i+1} (from {row['start_date']})"
            for i, row in history.iterrows()
        }
        oot["version"] = oot["product_key"].map(key_to_label)
        oot["period"]  = (
            oot["year"].astype(str) + "-"
            + oot["month"].astype(str).str.zfill(2)
        )

        fig_oot = px.line(
            oot, x="period", y="sales", color="version",
            markers=True, template=TEMPLATE,
            labels={"period": "Month", "sales": "Sales ($)", "version": "Version"},
            color_discrete_sequence=[BLUE, GREEN, YELLOW, RED, "#a78bfa", "#fb923c"],
        )
        fig_oot.update_layout(
            **_base_layout(height=380, legend=dict(orientation="h", y=1.1))
        )
        fig_oot.update_xaxes(tickangle=-45)
        _grid(fig_oot)
        st.plotly_chart(fig_oot, use_container_width=True)
    else:
        st.info("No order history found for this product across any version.")

    # ── Raw version table ─────────────────────────────────────────────────────
    with st.expander("📋 Raw version table"):
        display = history.drop(columns=["product_id"]).copy()
        display["end_date"] = display["end_date"].fillna("Present")
        display.index = [f"v{i+1}" for i in display.index]
        st.dataframe(display, use_container_width=True)
