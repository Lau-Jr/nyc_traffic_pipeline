"""
Phase 4 – NYC Traffic Analytics Dashboard

Multi-tab Streamlit dashboard integrating all pipeline phases:
  Tab 1 – Overview      : KPIs, pipeline health, congestion summary
  Tab 2 – Live Traffic  : Real-time map, speed feed, congestion distribution
  Tab 3 – ML Insights   : Predictions, anomaly alerts, model info
  Tab 4 – Historical    : Hourly trends, borough heatmaps, aggregations
  Tab 5 – Data Quality  : Quality scores, null rates, batch health

Run:
    streamlit run monitoring/dashboard.py
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Traffic Analytics",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants ─────────────────────────────────────────────────────────────────
MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB       = os.getenv("MONGO_DB",  "nyc_traffic")
STATS_FILE     = Path(__file__).parent.parent / "logs" / "producer_stats.json"
REFRESH_SECS   = 10

CONGESTION_COLORS = {
    "FREE_FLOW":  "#2ecc71",
    "MODERATE":   "#f39c12",
    "CONGESTED":  "#e67e22",
    "SEVERE":     "#e74c3c",
    "UNKNOWN":    "#95a5a6",
}

BOROUGH_LIST = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]


# ── Data helpers ──────────────────────────────────────────────────────────────

@st.cache_resource
def get_mongo():
    """Cached MongoDB client (one per session)."""
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)


def mongo_ok() -> bool:
    try:
        get_mongo().admin.command("ping")
        return True
    except Exception:
        return False


@st.cache_data(ttl=REFRESH_SECS)
def fetch_recent_records(hours: int = 1, boroughs: list | None = None, limit: int = 2000) -> pd.DataFrame:
    # Use naive UTC datetime — processed_at is stored as naive datetime by Spark
    since = datetime.utcnow() - timedelta(hours=hours)
    filt: dict = {"processed_at": {"$gte": since}}
    if boroughs:
        filt["borough"] = {"$in": boroughs}
    proj = {
        "_id": 0, "borough": 1, "link_name": 1, "speed": 1, "travel_time": 1,
        "congestion_level": 1, "congestion_score": 1, "hour_of_day": 1,
        "day_of_week": 1, "is_peak_hour": 1, "time_bucket": 1,
        "latitude": 1, "longitude": 1, "processed_at": 1,
    }
    try:
        docs = list(
            get_mongo()[MONGO_DB]["traffic_processed"]
            .find(filt, proj)
            .sort("processed_at", -1)
            .limit(limit)
        )
        df = pd.DataFrame(docs)
        if not df.empty:
            df["speed"]            = pd.to_numeric(df["speed"],            errors="coerce")
            df["travel_time"]      = pd.to_numeric(df["travel_time"],      errors="coerce")
            df["congestion_score"] = pd.to_numeric(df["congestion_score"], errors="coerce")
            df["latitude"]         = pd.to_numeric(df["latitude"],         errors="coerce")
            df["longitude"]        = pd.to_numeric(df["longitude"],        errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def fetch_aggregations(hours: int = 24, boroughs: list | None = None) -> pd.DataFrame:
    # window_start is stored as an ISO string — filter using string comparison
    since_str = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
    filt: dict = {"window_start": {"$gte": since_str}}
    if boroughs:
        filt["borough"] = {"$in": boroughs}
    try:
        docs = list(
            get_mongo()[MONGO_DB]["traffic_aggregated"]
            .find(filt, {"_id": 0})
            .sort("window_start", 1)
            .limit(5000)
        )
        # Fallback: if no docs in window, return all available data
        if not docs:
            docs = list(
                get_mongo()[MONGO_DB]["traffic_aggregated"]
                .find({} if not boroughs else {"borough": {"$in": boroughs}}, {"_id": 0})
                .sort("window_start", 1)
                .limit(5000)
            )
        df = pd.DataFrame(docs)
        if not df.empty:
            # Unpack nested speed_stats dict → flat columns
            if "speed_stats" in df.columns:
                speed_df = df["speed_stats"].apply(
                    lambda x: x if isinstance(x, dict) else {}
                ).apply(pd.Series)
                for sub, target in [("avg", "avg_speed"), ("min", "min_speed"),
                                    ("max", "max_speed"), ("median", "median_speed")]:
                    if sub in speed_df.columns:
                        df[target] = pd.to_numeric(speed_df[sub], errors="coerce")

            for col in ["avg_speed", "min_speed", "max_speed", "avg_congestion_score",
                        "record_count", "avg_travel_time"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            if "window_start" in df.columns:
                df["window_start"] = pd.to_datetime(df["window_start"], utc=True, errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def fetch_quality_metrics(hours: int = 24) -> pd.DataFrame:
    # recorded_at is stored as an ISO string — fetch recent docs and filter in Python
    since_str = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        docs = list(
            get_mongo()[MONGO_DB]["data_quality_metrics"]
            .find({}, {"_id": 0})
            .sort("recorded_at", -1)
            .limit(500)
        )
        df = pd.DataFrame(docs)
        if not df.empty:
            for col in ["quality_score", "total_records", "valid_records"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            # Filter to requested window using string comparison (ISO format sorts correctly)
            if "recorded_at" in df.columns:
                df = df[df["recorded_at"] >= since_str]
                df = df.sort_values("recorded_at")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_resource
def load_predictor():
    """Load and cache the TrafficPredictor once across all rerenders."""
    try:
        from analytics.predictor import TrafficPredictor
        p = TrafficPredictor()
        if p.models_exist():
            p.load()
            return p
    except Exception:
        pass
    return None


def load_producer_stats() -> dict:
    if STATS_FILE.exists():
        try:
            with open(STATS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🚦 NYC Traffic")
    st.caption("Real-Time Analytics Pipeline")
    st.divider()

    st.subheader("Filters")
    selected_boroughs = st.multiselect(
        "Borough", BOROUGH_LIST, default=BOROUGH_LIST,
        help="Filter all views by borough"
    )
    time_window = st.selectbox(
        "Time Window", ["Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days"],
        index=1,
    )
    time_hours = {"Last 1 hour": 1, "Last 6 hours": 6,
                  "Last 24 hours": 24, "Last 7 days": 168}[time_window]

    st.divider()
    auto_refresh = st.toggle("Auto-refresh (10s)", value=True)
    if st.button("↻ Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.caption(f"Connected to: `{MONGO_DB}`")
    db_status = "🟢 MongoDB connected" if mongo_ok() else "🔴 MongoDB offline"
    st.caption(db_status)
    st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🚦 NYC Traffic Analytics Pipeline")
st.caption("Real-time traffic monitoring · Spark streaming · ML predictions · NYC DOT data")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "🗺️ Live Traffic",
    "🤖 ML Insights",
    "📈 Historical",
    "✅ Data Quality",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    df = fetch_recent_records(hours=time_hours, boroughs=selected_boroughs or None)
    stats = load_producer_stats()

    # ── KPI row ───────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)

    total_records = len(df)
    avg_speed     = round(df["speed"].mean(), 1) if not df.empty and "speed" in df.columns else 0
    anomaly_count = 0
    quality_score = 0.0

    dq = fetch_quality_metrics(hours=time_hours)
    if not dq.empty and "quality_score" in dq.columns:
        quality_score = round(dq["quality_score"].mean(), 1)

    # Try anomaly count using cached predictor
    try:
        pred = load_predictor()
        if pred is not None and not df.empty:
            sample = df.head(100).to_dict(orient="records")
            preds  = pred.predict_batch(sample)
            anomaly_count = sum(1 for p in preds if p.get("is_anomaly"))
    except Exception:
        pass

    published = stats.get("published", 0)
    k1.metric("Records (window)", f"{total_records:,}")
    k2.metric("Avg Speed",        f"{avg_speed} mph")
    k3.metric("Quality Score",    f"{quality_score}%")
    k4.metric("Anomalies",        anomaly_count,
              delta=None if anomaly_count == 0 else f"{anomaly_count} flagged",
              delta_color="inverse")
    k5.metric("Total Published",  f"{published:,}",
              delta=f"{stats.get('rate_per_min', 0)} rec/min")

    st.divider()

    # ── Congestion distribution + Borough speed ───────────────────────────────
    col_l, col_r = st.columns(2)

    with col_l:
        if not df.empty and "congestion_level" in df.columns:
            cong_counts = df["congestion_level"].value_counts().reset_index()
            cong_counts.columns = ["Congestion Level", "Count"]
            ordered = ["FREE_FLOW", "MODERATE", "CONGESTED", "SEVERE", "UNKNOWN"]
            cong_counts["Congestion Level"] = pd.Categorical(
                cong_counts["Congestion Level"], categories=ordered, ordered=True
            )
            cong_counts = cong_counts.sort_values("Congestion Level")
            fig = px.pie(
                cong_counts, names="Congestion Level", values="Count",
                title="Congestion Level Distribution",
                color="Congestion Level",
                color_discrete_map=CONGESTION_COLORS,
                hole=0.4,
            )
            fig.update_layout(height=350, margin=dict(t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No congestion data in selected window.")

    with col_r:
        if not df.empty and "borough" in df.columns and "speed" in df.columns:
            borough_speed = (
                df.dropna(subset=["speed", "borough"])
                .groupby("borough")["speed"]
                .mean().reset_index()
                .rename(columns={"speed": "avg_speed"})
                .sort_values("avg_speed")
            )
            fig = px.bar(
                borough_speed, x="avg_speed", y="borough",
                orientation="h",
                title=f"Average Speed by Borough ({time_window})",
                color="avg_speed",
                color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                labels={"avg_speed": "Avg Speed (mph)", "borough": "Borough"},
            )
            fig.update_layout(height=350, coloraxis_showscale=False, margin=dict(t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No speed data for selected filters.")

    # ── Pipeline health ───────────────────────────────────────────────────────
    st.subheader("Pipeline Health")
    ph1, ph2, ph3, ph4 = st.columns(4)

    models_dir = Path(__file__).parent.parent / "models"
    models_ready = all(
        (models_dir / f"{n}.joblib").exists()
        for n in ["congestion_classifier", "speed_predictor", "anomaly_detector"]
    )

    ph1.metric("Kafka Producer",   "🟢 Running" if stats else "⚪ Unknown")
    ph2.metric("MongoDB",          "🟢 Online"  if mongo_ok() else "🔴 Offline")
    ph3.metric("ML Models",        "🟢 Loaded"  if models_ready else "⚠️ Not trained")
    ph4.metric("Stream Processor", "🟢 Active"  if not df.empty else "⚪ No data")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — LIVE TRAFFIC
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    df = fetch_recent_records(hours=min(time_hours, 6), boroughs=selected_boroughs or None,
                              limit=3000)

    if df.empty:
        st.info("No traffic records in the selected window. Ensure the Spark stream processor is running.")
    else:
        # ── Map ───────────────────────────────────────────────────────────────
        df_map = df.dropna(subset=["latitude", "longitude", "speed"])
        df_map = df_map[
            df_map["latitude"].between(40.4, 40.9) &
            df_map["longitude"].between(-74.3, -73.7)
        ]

        if not df_map.empty:
            fig_map = px.scatter_mapbox(
                df_map.sample(min(1500, len(df_map))),
                lat="latitude", lon="longitude",
                color="congestion_level",
                color_discrete_map=CONGESTION_COLORS,
                size_max=8,
                zoom=10,
                center={"lat": 40.7128, "lon": -74.0060},
                mapbox_style="open-street-map",
                title="Live Traffic Congestion Map — NYC",
                hover_data={"latitude": False, "longitude": False,
                            "speed": True, "borough": True,
                            "link_name": True, "congestion_level": True},
                labels={"congestion_level": "Congestion"},
                opacity=0.7,
            )
            fig_map.update_layout(height=500, margin=dict(t=40, r=0, l=0, b=0))
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.warning("No valid lat/lon data for map rendering.")

        # ── Speed histogram + Congestion by time bucket ───────────────────────
        col_a, col_b = st.columns(2)

        with col_a:
            fig_hist = px.histogram(
                df.dropna(subset=["speed"]),
                x="speed", nbins=30,
                color="congestion_level",
                color_discrete_map=CONGESTION_COLORS,
                title="Speed Distribution (mph)",
                labels={"speed": "Speed (mph)", "count": "Records"},
                barmode="overlay",
            )
            fig_hist.update_layout(height=320, margin=dict(t=40, b=0))
            st.plotly_chart(fig_hist, use_container_width=True)

        with col_b:
            if "time_bucket" in df.columns:
                bucket_order = ["OVERNIGHT", "MORNING_PEAK", "OFF_PEAK", "EVENING_PEAK"]
                bucket_speed = (
                    df.dropna(subset=["speed", "time_bucket"])
                    .groupby("time_bucket")["speed"].mean()
                    .reindex(bucket_order).reset_index()
                    .rename(columns={"speed": "avg_speed"})
                )
                fig_bucket = px.bar(
                    bucket_speed, x="time_bucket", y="avg_speed",
                    title="Avg Speed by Time Bucket",
                    color="avg_speed",
                    color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                    labels={"time_bucket": "Time Bucket", "avg_speed": "Avg Speed (mph)"},
                )
                fig_bucket.update_layout(height=320, coloraxis_showscale=False,
                                         margin=dict(t=40, b=0))
                st.plotly_chart(fig_bucket, use_container_width=True)

        # ── Live records table ────────────────────────────────────────────────
        st.subheader("Latest Records")
        display_cols = [c for c in
            ["link_name", "borough", "speed", "travel_time",
             "congestion_level", "time_bucket", "processed_at"]
            if c in df.columns]
        st.dataframe(
            df[display_cols].head(50),
            use_container_width=True, height=300,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ML INSIGHTS
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    models_dir = Path(__file__).parent.parent / "models"
    models_ready = all(
        (models_dir / f"{n}.joblib").exists()
        for n in ["congestion_classifier", "speed_predictor", "anomaly_detector"]
    )

    if not models_ready:
        st.warning("ML models not found. Run `python -m analytics.train` first.")
    else:
        predictor = load_predictor()
        if predictor is None:
            st.error("Failed to load ML models.")
        else:
            try:
                df_raw = fetch_recent_records(
                    hours=min(time_hours, 3),
                    boroughs=selected_boroughs or None,
                    limit=300,
                )

                if df_raw.empty:
                    st.info("No recent records to score. Ensure the stream processor is running.")
                else:
                    with st.spinner("Running ML predictions…"):
                        df_pred = predictor.predict_dataframe(df_raw.head(200).copy())

                    # ── ML KPIs ───────────────────────────────────────────────
                    m1, m2, m3, m4 = st.columns(4)
                    n_anomalies  = int(df_pred["is_anomaly"].sum()) if "is_anomaly" in df_pred.columns else 0
                    avg_conf     = df_pred["congestion_confidence"].mean() if "congestion_confidence" in df_pred.columns else 0
                    avg_pred_spd = df_pred["predicted_speed"].mean() if "predicted_speed" in df_pred.columns else 0
                    anom_pct     = round(n_anomalies / max(len(df_pred), 1) * 100, 1)

                    m1.metric("Records Scored",           f"{len(df_pred):,}")
                    m2.metric("Anomalies Detected",       f"{n_anomalies}",
                              delta=f"{anom_pct}% of records",
                              delta_color="inverse" if n_anomalies > 0 else "off")
                    m3.metric("Avg Prediction Confidence", f"{avg_conf*100:.1f}%")
                    m4.metric("Avg Predicted Speed",       f"{avg_pred_spd:.1f} mph")

                    st.divider()
                    col_l, col_r = st.columns(2)

                    # ── Predicted vs Actual congestion ────────────────────────
                    with col_l:
                        if "predicted_congestion" in df_pred.columns and "congestion_level" in df_pred.columns:
                            comparison = (
                                df_pred.groupby(["congestion_level", "predicted_congestion"])
                                .size().reset_index(name="count")
                            )
                            fig_cmp = px.bar(
                                comparison,
                                x="congestion_level", y="count",
                                color="predicted_congestion",
                                color_discrete_map=CONGESTION_COLORS,
                                title="Actual vs Predicted Congestion",
                                labels={"congestion_level": "Actual", "count": "Records",
                                        "predicted_congestion": "Predicted"},
                                barmode="group",
                            )
                            fig_cmp.update_layout(height=360, margin=dict(t=40, b=0))
                            st.plotly_chart(fig_cmp, use_container_width=True)

                    # ── Anomaly score distribution ────────────────────────────
                    with col_r:
                        if "anomaly_score" in df_pred.columns:
                            fig_anom = px.histogram(
                                df_pred.dropna(subset=["anomaly_score"]),
                                x="anomaly_score",
                                color="is_anomaly",
                                nbins=40,
                                color_discrete_map={True: "#e74c3c", False: "#2ecc71"},
                                title="Anomaly Score Distribution",
                                labels={"anomaly_score": "Anomaly Score",
                                        "is_anomaly": "Is Anomaly"},
                                barmode="overlay",
                            )
                            fig_anom.add_vline(x=0, line_dash="dash", line_color="gray",
                                               annotation_text="threshold")
                            fig_anom.update_layout(height=360, margin=dict(t=40, b=0))
                            st.plotly_chart(fig_anom, use_container_width=True)

                    # ── Predicted vs Actual speed scatter ─────────────────────
                    if "predicted_speed" in df_pred.columns and "speed" in df_pred.columns:
                        df_scatter = df_pred.dropna(subset=["speed", "predicted_speed"])
                        if not df_scatter.empty:
                            fig_scatter = px.scatter(
                                df_scatter,
                                x="speed", y="predicted_speed",
                                color="congestion_level",
                                color_discrete_map=CONGESTION_COLORS,
                                title="Actual vs Predicted Speed",
                                labels={"speed": "Actual Speed (mph)",
                                        "predicted_speed": "Predicted Speed (mph)"},
                                opacity=0.6,
                            )
                            max_s = float(df_scatter[["speed", "predicted_speed"]].max().max())
                            fig_scatter.add_shape(
                                type="line", x0=0, y0=0, x1=max_s, y1=max_s,
                                line=dict(dash="dash", color="gray"),
                            )
                            fig_scatter.update_layout(height=380, margin=dict(t=40, b=0))
                            st.plotly_chart(fig_scatter, use_container_width=True)

                    # ── Anomaly records table ─────────────────────────────────
                    if n_anomalies > 0:
                        st.subheader(f"⚠️ Anomaly Alerts ({n_anomalies} records)")
                        anom_cols = [c for c in ["link_name", "borough", "speed",
                                                 "congestion_level", "anomaly_score",
                                                 "processed_at"]
                                     if c in df_pred.columns]
                        st.dataframe(
                            df_pred[df_pred["is_anomaly"] == True][anom_cols].head(20),
                            use_container_width=True, height=250,
                        )

                    # ── Model metadata ────────────────────────────────────────
                    with st.expander("Model Details"):
                        for name in ["congestion_classifier", "speed_predictor", "anomaly_detector"]:
                            meta_path = models_dir / f"{name}_metadata.json"
                            if meta_path.exists():
                                with open(meta_path) as f:
                                    meta = json.load(f)
                                st.write(f"**{name}**")
                                st.json(meta)

            except Exception as exc:
                st.error(f"ML prediction error: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — HISTORICAL
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    df_agg = fetch_aggregations(hours=time_hours, boroughs=selected_boroughs or None)

    if df_agg.empty:
        st.info("No aggregation data yet. Run `python -m processors.batch_processor` to generate aggregations.")
    else:
        # ── Speed trend over time ─────────────────────────────────────────────
        if "window_start" in df_agg.columns and "avg_speed" in df_agg.columns:
            df_trend = df_agg.dropna(subset=["window_start", "avg_speed", "borough"])
            fig_trend = px.line(
                df_trend,
                x="window_start", y="avg_speed",
                color="borough",
                title=f"Average Speed Trend by Borough ({time_window})",
                labels={"window_start": "Time", "avg_speed": "Avg Speed (mph)",
                        "borough": "Borough"},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_trend.update_layout(height=380, margin=dict(t=40, b=0))
            st.plotly_chart(fig_trend, use_container_width=True)

        col_a, col_b = st.columns(2)

        # ── Record count over time ─────────────────────────────────────────────
        with col_a:
            if "window_start" in df_agg.columns and "record_count" in df_agg.columns:
                fig_vol = px.area(
                    df_agg.dropna(subset=["window_start", "record_count", "borough"]),
                    x="window_start", y="record_count",
                    color="borough",
                    title="Record Volume by Borough",
                    labels={"window_start": "Time", "record_count": "Records",
                            "borough": "Borough"},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_vol.update_layout(height=340, margin=dict(t=40, b=0))
                st.plotly_chart(fig_vol, use_container_width=True)

        # ── Congestion score heatmap ───────────────────────────────────────────
        with col_b:
            df_raw_h = fetch_recent_records(hours=time_hours,
                                            boroughs=selected_boroughs or None,
                                            limit=5000)
            if not df_raw_h.empty and "hour_of_day" in df_raw_h.columns:
                df_heat = (
                    df_raw_h.dropna(subset=["hour_of_day", "day_of_week", "congestion_score"])
                    .groupby(["hour_of_day", "day_of_week"])["congestion_score"]
                    .mean().reset_index()
                )
                if not df_heat.empty:
                    pivot = df_heat.pivot(
                        index="hour_of_day", columns="day_of_week",
                        values="congestion_score"
                    )
                    day_labels = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed",
                                  5: "Thu", 6: "Fri", 7: "Sat"}
                    pivot.columns = [day_labels.get(c, str(c)) for c in pivot.columns]

                    fig_heat = go.Figure(data=go.Heatmap(
                        z=pivot.values,
                        x=pivot.columns.tolist(),
                        y=pivot.index.tolist(),
                        colorscale="RdYlGn_r",
                        colorbar=dict(title="Congestion Score"),
                        zmin=0, zmax=1,
                    ))
                    fig_heat.update_layout(
                        title="Congestion Heatmap — Hour × Day of Week",
                        xaxis_title="Day of Week",
                        yaxis_title="Hour of Day",
                        height=340,
                        margin=dict(t=40, b=0),
                    )
                    st.plotly_chart(fig_heat, use_container_width=True)

        # ── Borough comparison table ───────────────────────────────────────────
        st.subheader("Borough Performance Summary")
        if not df_agg.empty and "borough" in df_agg.columns:
            agg_spec = {}
            rename_map = {"borough": "Borough"}
            if "avg_speed" in df_agg.columns:
                agg_spec["avg_speed"]        = ("avg_speed", "mean")
                rename_map["avg_speed"]       = "Avg Speed (mph)"
            if "min_speed" in df_agg.columns:
                agg_spec["min_speed"]        = ("min_speed", "min")
                rename_map["min_speed"]       = "Min Speed (mph)"
            if "max_speed" in df_agg.columns:
                agg_spec["max_speed"]        = ("max_speed", "max")
                rename_map["max_speed"]       = "Max Speed (mph)"
            if "avg_congestion_score" in df_agg.columns:
                agg_spec["avg_congestion"]   = ("avg_congestion_score", "mean")
                rename_map["avg_congestion"]  = "Avg Congestion Score"
            if "avg_travel_time" in df_agg.columns:
                agg_spec["avg_travel_time"]  = ("avg_travel_time", "mean")
                rename_map["avg_travel_time"] = "Avg Travel Time (s)"
            if "record_count" in df_agg.columns:
                agg_spec["total_records"]    = ("record_count", "sum")
                rename_map["total_records"]   = "Total Records"

            if agg_spec:
                summary = (
                    df_agg.groupby("borough")
                    .agg(**agg_spec)
                    .round(2)
                    .reset_index()
                    .rename(columns=rename_map)
                )
                st.dataframe(summary, use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_agg.groupby("borough").size().reset_index(name="Batches"),
                             use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — DATA QUALITY
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    dq = fetch_quality_metrics(hours=time_hours)

    if dq.empty:
        st.info("No quality metrics yet. The stream processor records these automatically.")
    else:
        # ── Quality KPIs ──────────────────────────────────────────────────────
        q1, q2, q3, q4 = st.columns(4)
        avg_qs = round(dq["quality_score"].mean(), 2) if "quality_score" in dq.columns else 0
        min_qs = round(dq["quality_score"].min(), 2)  if "quality_score" in dq.columns else 0
        total  = int(dq["total_records"].sum())        if "total_records" in dq.columns else 0
        valid  = int(dq["valid_records"].sum())        if "valid_records" in dq.columns else 0

        q1.metric("Avg Quality Score", f"{avg_qs}%")
        q2.metric("Min Quality Score", f"{min_qs}%")
        q3.metric("Total Records",     f"{total:,}")
        q4.metric("Valid Records",     f"{valid:,}",
                  delta=f"{round(valid/max(total,1)*100,1)}% valid")

        st.divider()
        col_a, col_b = st.columns(2)

        # ── Quality score over time ───────────────────────────────────────────
        with col_a:
            if "recorded_at" in dq.columns and "quality_score" in dq.columns:
                fig_qs = px.line(
                    dq.dropna(subset=["recorded_at", "quality_score"]),
                    x="recorded_at", y="quality_score",
                    title="Quality Score Over Time (%)",
                    labels={"recorded_at": "Time", "quality_score": "Quality Score (%)"},
                    color_discrete_sequence=["#2ecc71"],
                )
                fig_qs.add_hline(y=99, line_dash="dash", line_color="orange",
                                 annotation_text="99% target")
                fig_qs.update_layout(height=340, margin=dict(t=40, b=0), yaxis_range=[0, 101])
                st.plotly_chart(fig_qs, use_container_width=True)

        # ── Valid vs invalid records ──────────────────────────────────────────
        with col_b:
            if "total_records" in dq.columns and "valid_records" in dq.columns:
                dq["invalid_records"] = dq["total_records"] - dq["valid_records"]
                fig_valid = px.area(
                    dq.dropna(subset=["recorded_at"]),
                    x="recorded_at",
                    y=["valid_records", "invalid_records"],
                    title="Valid vs Invalid Records per Batch",
                    labels={"recorded_at": "Time", "value": "Records",
                            "variable": "Type"},
                    color_discrete_map={
                        "valid_records":   "#2ecc71",
                        "invalid_records": "#e74c3c",
                    },
                )
                fig_valid.update_layout(height=340, margin=dict(t=40, b=0))
                st.plotly_chart(fig_valid, use_container_width=True)

        # ── Null counts ───────────────────────────────────────────────────────
        if "null_counts" in dq.columns:
            st.subheader("Null Field Counts (latest 20 batches)")
            try:
                null_expanded = pd.json_normalize(dq["null_counts"].dropna().tolist())
                null_expanded["batch"] = range(len(null_expanded))
                if not null_expanded.empty:
                    fig_null = px.bar(
                        null_expanded.tail(20),
                        x="batch",
                        y=null_expanded.columns.drop("batch").tolist(),
                        title="Null Counts per Batch",
                        labels={"batch": "Batch", "value": "Null Count", "variable": "Field"},
                        barmode="group",
                    )
                    fig_null.update_layout(height=300, margin=dict(t=40, b=0))
                    st.plotly_chart(fig_null, use_container_width=True)
            except Exception:
                pass

        # ── Recent quality batches table ──────────────────────────────────────
        st.subheader("Recent Quality Batches")
        display_cols = [c for c in
            ["batch_id", "recorded_at", "total_records", "valid_records",
             "quality_score", "source"]
            if c in dq.columns]
        st.dataframe(dq[display_cols].tail(20), use_container_width=True,
                     hide_index=True, height=280)


# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    st.markdown(
        f'<meta http-equiv="refresh" content="{REFRESH_SECS}">',
        unsafe_allow_html=True,
    )
    st.sidebar.caption(f"Next refresh in ~{REFRESH_SECS}s")
