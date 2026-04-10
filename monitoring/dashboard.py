"""
Phase 1 Monitoring Dashboard — Streamlit app.

Run with:  streamlit run monitoring/dashboard.py --server.port 8502

Shows:
  - Live producer stats (from logs/producer_stats.json)
  - Kafka topic message counts (via kafka-python AdminClient)
  - Recent records preview (live API sample)
  - Data quality metrics (valid vs DLQ rate)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from producers.nyc_dot_client import NYCDOTClient

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Traffic Pipeline — Phase 1 Monitor",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

STATS_FILE = os.path.join(os.path.dirname(__file__), "..", "logs", "producer_stats.json")
REFRESH_INTERVAL = 15   # seconds

# ── Header ───────────────────────────────────────────────────────────────────
st.title("🚦 NYC Traffic Pipeline — Phase 1 Live Monitor")
st.caption(f"Data source: NYC DOT Traffic Speeds NBE (dataset i4gi-tjb9) · "
           f"Auto-refreshes every {REFRESH_INTERVAL}s")

col_status, col_refresh = st.columns([4, 1])
with col_refresh:
    if st.button("↻ Refresh now"):
        st.rerun()


# ── Helper: load producer stats ──────────────────────────────────────────────
def load_stats() -> dict:
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE) as f:
            return json.load(f)
    return {}


# ── Section 1: Producer Stats ─────────────────────────────────────────────────
st.subheader("Producer Status")
stats = load_stats()

if stats:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Fetched",    stats.get("fetched", 0))
    c2.metric("Published",  stats.get("published", 0),
              delta=f"{stats.get('rate_per_min', 0)} rec/min")
    c3.metric("Invalid → DLQ", stats.get("invalid", 0))
    c4.metric("Duplicates skipped", stats.get("duplicates", 0))
    c5.metric("Errors",     stats.get("errors", 0))

    ts = stats.get("timestamp", "")
    st.caption(f"Last poll: {ts}")

    # Borough breakdown bar chart
    by_borough = stats.get("by_borough", {})
    if by_borough:
        df_borough = pd.DataFrame(
            list(by_borough.items()), columns=["Borough", "Records"]
        ).sort_values("Records", ascending=False)

        fig = px.bar(
            df_borough, x="Borough", y="Records",
            title="Records Published — by Borough (last poll)",
            color="Borough",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_layout(showlegend=False, height=300)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No producer stats yet. Start the producer: "
            "`python producers/traffic_producer.py`")


# ── Section 2: Live API Sample ───────────────────────────────────────────────
st.subheader("Live API Sample — Latest 50 Records")

@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_sample():
    client = NYCDOTClient()
    return client.fetch_latest(limit=50)

with st.spinner("Fetching from NYC DOT API …"):
    try:
        records = fetch_sample()
        if records:
            df = pd.DataFrame(records)

            # Cast numeric columns
            for col in ["speed", "travel_time"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # ── Speed distribution ───────────────────────────────────────────
            cola, colb = st.columns(2)

            with cola:
                if "speed" in df.columns:
                    fig_hist = px.histogram(
                        df.dropna(subset=["speed"]),
                        x="speed",
                        nbins=20,
                        title="Speed Distribution (mph)",
                        color_discrete_sequence=["#1f77b4"],
                    )
                    fig_hist.update_layout(height=300)
                    st.plotly_chart(fig_hist, use_container_width=True)

            with colb:
                if "borough" in df.columns and "speed" in df.columns:
                    fig_box = px.box(
                        df.dropna(subset=["speed", "borough"]),
                        x="borough", y="speed",
                        title="Speed by Borough",
                        color="borough",
                        color_discrete_sequence=px.colors.qualitative.Set2,
                    )
                    fig_box.update_layout(showlegend=False, height=300)
                    st.plotly_chart(fig_box, use_container_width=True)

            # ── Data quality summary ─────────────────────────────────────────
            st.subheader("Data Quality — Current Batch")
            total = len(df)
            null_speed    = df["speed"].isna().sum() if "speed" in df.columns else 0
            null_borough  = df["borough"].isna().sum() if "borough" in df.columns else 0
            valid_pct     = round((total - null_speed) / max(total, 1) * 100, 1)

            qc1, qc2, qc3, qc4 = st.columns(4)
            qc1.metric("Total records",    total)
            qc2.metric("Valid speed",      f"{valid_pct}%")
            qc3.metric("Null speed",       null_speed)
            qc4.metric("Missing borough",  null_borough)

            # ── Raw data table ───────────────────────────────────────────────
            st.subheader("Raw Records Preview")
            display_cols = [c for c in
                ["link_name", "borough", "speed", "travel_time",
                 "status", "data_as_of", "link_id"]
                if c in df.columns]
            st.dataframe(
                df[display_cols].head(20),
                use_container_width=True,
                height=400,
            )
        else:
            st.warning("No records returned from the API. "
                       "Check connectivity or app token.")
    except Exception as exc:
        st.error(f"API error: {exc}")


# ── Section 3: Pipeline Health ───────────────────────────────────────────────
st.subheader("Pipeline Health Checklist")

checks = {
    "NYC DOT API reachable": bool(records) if "records" in dir() else False,
    "Producer stats file exists": os.path.exists(STATS_FILE),
    "Kafka bootstrap servers configured":
        bool(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")),
}

for check, result in checks.items():
    icon = "✅" if result else "⚠️"
    st.write(f"{icon}  {check}")

# ── Auto-refresh ─────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("Dashboard auto-refreshes every 15 seconds via query param.")
st.markdown(
    f'<meta http-equiv="refresh" content="{REFRESH_INTERVAL}">',
    unsafe_allow_html=True,
)
