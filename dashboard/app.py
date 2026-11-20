"""Pelivahti dashboard: GGR and tax, responsible gaming alerts, player drilldown, data quality.

Run locally with `streamlit run dashboard/app.py`. If no warehouse exists yet (for example on
Streamlit Community Cloud) the app builds one first with a smaller synthetic dataset.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = Path(os.environ.get("PELIVAHTI_DATA", ROOT / "data"))
WAREHOUSE = DATA / "warehouse.duckdb"

# Reference palette (validated): categorical slots 1-2 and the fixed status colors.
PRODUCT_COLORS = {"casino": "#2a78d6", "sports": "#eb6834"}
SEVERITY_COLORS = {"critical": "#d03b3b", "high": "#ec835a", "medium": "#fab219", "low": "#8a8986"}
SEVERITY_ORDER = ["critical", "high", "medium", "low"]
RULE_LABELS = {
    "SELF_EXCLUSION_BREACH": "Played while self-excluded",
    "DAILY_LOSS_LIMIT_BREACH": "Daily loss limit breached",
    "MONTHLY_LOSS_LIMIT_BREACH": "Monthly loss limit breached",
    "LOSS_CHASING": "Loss chasing",
    "DEPOSIT_VELOCITY": "Rapid deposits",
    "NIGHT_MARATHON_SESSION": "Night marathon session",
    "DAILY_LOSS_LIMIT_80PCT": "80% of daily limit",
    "STALE_ODDS_ACCEPTED": "Bet accepted at stale odds",
}

st.set_page_config(page_title="Pelivahti", page_icon="🎯", layout="wide")


def build_warehouse() -> None:
    from pelivahti.generator import GeneratorConfig, generate
    from pelivahti.lakehouse import land
    from pelivahti.stream.runner import run_stream

    events = DATA / "events.jsonl"
    generate(events, GeneratorConfig(seed=42, players=400, days=3))
    land(DATA / "lake", file=events)
    run_stream(DATA / "realtime.duckdb", file=events, log=lambda *_: None)
    env = dict(
        os.environ,
        PELIVAHTI_LAKE=str(DATA / "lake"),
        PELIVAHTI_REALTIME=str(DATA / "realtime.duckdb"),
        PELIVAHTI_WAREHOUSE=str(WAREHOUSE),
        DBT_PROFILES_DIR=str(ROOT / "dbt"),
    )
    dbt = Path(sys.executable).with_name("dbt")
    subprocess.run([str(dbt) if dbt.exists() else "dbt", "build", "--quiet"], cwd=ROOT / "dbt", env=env, check=True)


@st.cache_resource(show_spinner=False)
def ensure_warehouse() -> Path:
    if not WAREHOUSE.exists():
        with st.spinner("First start: generating synthetic data and building the warehouse (about a minute)..."):
            build_warehouse()
    return WAREHOUSE


@st.cache_data(show_spinner=False, ttl=300)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    # short-lived read-only connections, so `dbt build` can rebuild the warehouse while the app is open
    with duckdb.connect(str(ensure_warehouse()), read_only=True) as con:
        return con.execute(sql, list(params)).df()


def style(fig: go.Figure, height: int = 340) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=56, b=8),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="right", x=1, title=None),
        hoverlabel=dict(bgcolor="white"),
        font=dict(size=13),
    )
    fig.update_xaxes(showgrid=False, title=None)
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.18)", zeroline=False, title=None)
    return fig


def eur(x: float) -> str:
    return f"€{x:,.0f}".replace(",", " ")


# ---------------------------------------------------------------------------------------------- header
st.title("Pelivahti")
st.caption(
    "Real-time betting settlement and responsible gaming monitoring for a regulated gambling operator. "
    "All players, bets and results are synthetic."
)

ggr = q("select * from marts.fct_ggr_daily order by business_date, product")
ggr["business_date"] = pd.to_datetime(ggr["business_date"]).dt.strftime("%a %d %b")
dates = sorted(ggr["business_date"].unique())
products = st.multiselect("Products", ["casino", "sports"], default=["casino", "sports"])
ggr_f = ggr[ggr["product"].isin(products)]

tab_overview, tab_rg, tab_player, tab_dq, tab_odds = st.tabs(
    ["Revenue and tax", "Responsible gaming", "Player drilldown", "Data quality", "Odds and matches"]
)

# ---------------------------------------------------------------------------------------------- overview
with tab_overview:
    tax = q("select * from marts.fct_gambling_tax_monthly")
    tax["tax_month"] = pd.to_datetime(tax["tax_month"]).dt.strftime("%Y-%m")
    tax_f = tax[tax["product"].isin(products)]
    alerts_total = q("select count(*) as n from marts.fct_rg_alerts where category = 'responsible_gaming'")["n"][0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Turnover", eur(ggr_f["turnover_eur"].astype(float).sum()))
    c2.metric("Gross gaming revenue", eur(ggr_f["ggr_eur"].astype(float).sum()))
    c3.metric("Estimated gambling tax (22%)", eur(tax_f["tax_eur"].astype(float).sum()))
    c4.metric("Active players", f"{q('select count(distinct player_id) n from marts.fct_player_daily')['n'][0]:,}")
    c5.metric("Responsible gaming alerts", f"{alerts_total:,}")

    left, right = st.columns(2)
    with left:
        fig = px.bar(
            ggr_f,
            x="business_date",
            y="ggr_eur",
            color="product",
            barmode="group",
            color_discrete_map=PRODUCT_COLORS,
            title="GGR per business day (EUR)",
            hover_data={"turnover_eur": ":,.2f", "winnings_eur": ":,.2f", "hold_pct": ":.1%"},
        )
        fig.update_traces(marker_line_width=0)
        fig.update_layout(bargap=0.35, bargroupgap=0.08)
        st.plotly_chart(style(fig), width="stretch")
    with right:
        fig = px.line(
            ggr_f,
            x="business_date",
            y="hold_pct",
            color="product",
            markers=True,
            color_discrete_map=PRODUCT_COLORS,
            title="Hold % (GGR / turnover)",
        )
        fig.update_traces(line_width=2, marker_size=8)
        fig.update_yaxes(tickformat=".0%")
        fig.update_xaxes(type="category")
        st.plotly_chart(style(fig), width="stretch")

    st.subheader("Gambling tax estimate")
    st.caption(
        "Licensed betting and online casino operators typically pay tax on the gambling margin "
        "(stakes minus winnings). Modelled here as 22% of positive monthly GGR per product; the rate is a dbt "
        "variable. Illustrative only, not tax advice."
    )
    st.dataframe(tax_f, width="stretch", hide_index=True)

# ---------------------------------------------------------------------------------------------- RG
with tab_rg:
    alerts = q("select * from marts.fct_rg_alerts")
    sev = st.multiselect("Severity", SEVERITY_ORDER, default=SEVERITY_ORDER)
    a = alerts[alerts["severity"].isin(sev)].copy()
    a["rule_label"] = a["rule"].map(RULE_LABELS).fillna(a["rule"])
    a["label"] = a["rule_label"] + "  (" + a["severity"] + ")"

    left, right = st.columns([3, 2])
    with left:
        by_rule = a.groupby(["label", "severity"], as_index=False).size().sort_values("size")
        fig = px.bar(
            by_rule,
            x="size",
            y="label",
            color="severity",
            orientation="h",
            color_discrete_map=SEVERITY_COLORS,
            category_orders={"severity": SEVERITY_ORDER},
            title="Alerts by rule",
        )
        fig.update_traces(marker_line_width=0)
        fig.update_layout(bargap=0.3, showlegend=False)
        st.plotly_chart(style(fig, 380), width="stretch")
    with right:
        tiers = q("select risk_tier, count(*) as players from marts.dim_players group by 1")
        tiers["risk_tier"] = pd.Categorical(tiers["risk_tier"], ["red", "amber", "green"], ordered=True)
        tiers = tiers.sort_values("risk_tier")
        fig = px.bar(
            tiers,
            x="risk_tier",
            y="players",
            color="risk_tier",
            title="Players by risk tier",
            color_discrete_map={"red": "#d03b3b", "amber": "#fab219", "green": "#0ca30c"},
            text="players",
        )
        fig.update_traces(marker_line_width=0, textposition="outside")
        fig.update_layout(showlegend=False, bargap=0.45)
        st.plotly_chart(style(fig, 380), width="stretch")

    a["hour"] = pd.to_datetime(a["triggered_at_local"]).dt.floor("h")
    hourly = a.groupby("hour", as_index=False).size()
    fig = px.line(hourly, x="hour", y="size", title="Alerts per hour (local time)")
    fig.update_traces(line_width=2, line_color="#2a78d6")
    st.plotly_chart(style(fig, 280), width="stretch")

    st.subheader("Players to contact first")
    top = q("""
        select player_id, risk_tier, rg_alerts_critical as critical, rg_alerts_high as high,
               rg_alerts_total as total, daily_loss_limit_eur, has_self_excluded, age_band, region
        from marts.dim_players where risk_tier = 'red'
        order by rg_alerts_critical desc, rg_alerts_high desc, rg_alerts_total desc limit 25""")
    st.dataframe(top, width="stretch", hide_index=True)

# ---------------------------------------------------------------------------------------------- player
with tab_player:
    players = q("""select player_id from marts.dim_players
                   order by rg_alerts_critical desc, rg_alerts_high desc, rg_alerts_total desc, player_id""")
    pid = st.selectbox("Player", players["player_id"])
    info = q("select * from marts.dim_players where player_id = ?", (pid,)).iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Risk tier", info["risk_tier"].upper())
    c2.metric("Daily loss limit", eur(float(info["daily_loss_limit_eur"])))
    c3.metric("Monthly loss limit", eur(float(info["monthly_loss_limit_eur"])))
    c4.metric("Self-excluded", "Yes" if info["has_self_excluded"] else "No")

    daily = q("select * from marts.fct_player_daily where player_id = ? order by business_date", (pid,))
    if not daily.empty:
        daily["business_date"] = pd.to_datetime(daily["business_date"]).dt.strftime("%a %d %b")
        fig = go.Figure()
        fig.add_bar(
            x=daily["business_date"],
            y=daily["net_loss_eur"].astype(float),
            name="Net loss",
            marker_color="#2a78d6",
            marker_line_width=0,
        )
        fig.add_scatter(
            x=daily["business_date"],
            y=daily["daily_loss_limit_eur"].astype(float),
            name="Daily limit",
            mode="lines+markers",
            line=dict(color="#52514e", width=2, dash="dash"),
            marker_size=8,
        )
        fig.update_layout(title="Net loss per day vs. daily loss limit (EUR)", bargap=0.5)
        st.plotly_chart(style(fig, 300), width="stretch")

    left, right = st.columns(2)
    with left:
        st.markdown("**Alerts**")
        st.dataframe(
            q(
                """select triggered_at_local, rule, severity, details from marts.fct_rg_alerts
                          where player_id = ? order by triggered_at_local""",
                (pid,),
            ),
            width="stretch",
            hide_index=True,
        )
    with right:
        st.markdown("**Bets**")
        st.dataframe(
            q(
                """select placed_at_local, fixture, selection, odds, stake_eur, status, return_eur
                          from marts.fct_sports_bets where player_id = ? order by placed_at_utc""",
                (pid,),
            ),
            width="stretch",
            hide_index=True,
        )

# ---------------------------------------------------------------------------------------------- data quality
with tab_dq:
    dq = q("select * from reports.rpt_data_quality")
    vals = dict(zip(dq["metric"], dq["value"]))
    oob = int(vals.get("ledger_players_out_of_balance", 0))
    if oob == 0:
        st.success("✅ Reconciled: the real-time ledger and the batch rebuild agree to the cent for every player.")
    else:
        st.error(f"❌ {oob} players are out of balance between the stream and the batch layer.")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events in bronze", f"{vals.get('bronze_events', 0):,}")
    c2.metric("Dead-lettered (contract)", f"{vals.get('bronze_dlq', 0):,}")
    c3.metric("Duplicates dropped", f"{vals.get('stream_duplicates_dropped', 0):,}")
    c4.metric("Late events", f"{vals.get('stream_late_events', 0):,}")

    left, right = st.columns(2)
    with left:
        late = q("select event_type, lateness_seconds / 60 as minutes_late from reports.rpt_late_events")
        fig = px.histogram(
            late, x="minutes_late", nbins=40, title="How late were late events? (minutes past watermark)"
        )
        fig.update_traces(marker_color="#2a78d6", marker_line_width=0)
        fig.update_layout(bargap=0.08)
        st.plotly_chart(style(fig, 300), width="stretch")
    with right:
        gap = q("""select business_date, sum(late_data_gap_eur) as gap_eur,
                          count(*) filter (where late_data_gap_eur <> 0) as player_days
                   from reports.rpt_realtime_vs_batch_daily group by 1 order by 1""")
        gap["business_date"] = pd.to_datetime(gap["business_date"]).dt.strftime("%a %d %b")
        fig = px.bar(
            gap,
            x="business_date",
            y="gap_eur",
            title="Net loss the real-time windows missed (EUR)",
            hover_data=["player_days"],
        )
        fig.update_traces(marker_color="#eb6834", marker_line_width=0)
        fig.update_layout(bargap=0.5)
        st.plotly_chart(style(fig, 300), width="stretch")

    st.markdown("**Dead-letter queue**")
    dlq = q("""select regexp_extract(error, '^[a-z_]+') as error_class, error, count(*) as messages
               from bronze.dlq group by all order by messages desc""")
    st.dataframe(dlq, width="stretch", hide_index=True)

    batches = q("select * from reports.rpt_stream_batches where records > 0 order by batch_no")
    fig = px.line(batches, x="batch_no", y="records_per_second", title="Stream throughput (records per second)")
    fig.update_traces(line_width=2, line_color="#2a78d6")
    st.plotly_chart(style(fig, 260), width="stretch")

# ---------------------------------------------------------------------------------------------- odds
with tab_odds:
    matches = q("select * from marts.dim_matches order by kickoff_utc")
    st.dataframe(
        matches[
            [
                "fixture",
                "league",
                "kickoff_local",
                "final_outcome",
                "result_corrected",
                "n_bets",
                "turnover_eur",
                "paid_out_eur",
            ]
        ],
        width="stretch",
        hide_index=True,
    )
    moves = q("""select m.fixture, o.selection, o.opening_odds, o.closing_odds, o.drift_pct, o.n_bets, o.stakes_eur
                 from marts.fct_odds_movement o join marts.dim_matches m using (match_id)
                 order by abs(o.drift_pct) desc""")
    st.markdown("**Biggest odds moves before kickoff**")
    st.dataframe(moves.head(30), width="stretch", hide_index=True)
