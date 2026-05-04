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
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants ─────────────────────────────────────────────────────────────────
MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB       = os.getenv("MONGO_DB",  "nyc_traffic")
STATS_FILE     = Path(__file__).parent.parent / "logs" / "producer_stats.json"
REFRESH_SECS   = 60

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


@st.cache_data(ttl=10)
def mongo_ok() -> bool:
    """Ping MongoDB — cached for 10s so we don't ping on every Streamlit re-render."""
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
        "_id": 0, "borough": 1, "link_id": 1, "link_name": 1,
        "speed": 1, "travel_time": 1,
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
    # window_start was previously stored as ISO strings; new writes use BSON
    # datetime.  Use $or so the query matches both formats during the
    # transition period (until old records age out).
    since_dt  = datetime.utcnow() - timedelta(hours=hours)
    since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%S")
    filt: dict = {
        "$or": [
            {"window_start": {"$gte": since_dt}},
            {"window_start": {"$gte": since_str}},
        ]
    }
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
    """
    recorded_at is stored as datetime in new writes, ISO string in old ones.
    Fetch the most recent N docs and filter in Python by coercing to datetime.
    """
    since_dt = datetime.utcnow() - timedelta(hours=hours)
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
            if "recorded_at" in df.columns:
                # Coerce mixed string/datetime to datetime for comparison
                df["recorded_at"] = pd.to_datetime(df["recorded_at"], errors="coerce", utc=True)
                df = df[df["recorded_at"] >= pd.Timestamp(since_dt, tz="UTC")]
                df = df.sort_values("recorded_at")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def fetch_lineage_entries(limit: int = 100) -> pd.DataFrame:
    """Fetch recent data_lineage entries — used by the new Lineage tab."""
    try:
        docs = list(
            get_mongo()[MONGO_DB]["data_lineage"]
            .find({}, {"_id": 0})
            .sort("started_at", -1)
            .limit(limit)
        )
        df = pd.DataFrame(docs)
        if not df.empty:
            for col in ["started_at", "completed_at"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            if "duration_ms" in df.columns:
                df["duration_ms"] = pd.to_numeric(df["duration_ms"], errors="coerce")
            if "record_count" in df.columns:
                df["record_count"] = pd.to_numeric(df["record_count"], errors="coerce")
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


@st.cache_data(ttl=10)
def run_predictions(_predictor, df: pd.DataFrame) -> pd.DataFrame:
    """Cache ML predictions — recomputes only when input data changes.

    The leading underscore on _predictor tells Streamlit not to hash it
    (TrafficPredictor is not hashable). The df argument IS hashed, so
    predictions are reused for the same records within the 10-second TTL.
    """
    return _predictor.predict_dataframe(df.head(200).copy())


def load_producer_stats() -> dict:
    if STATS_FILE.exists():
        try:
            with open(STATS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


@st.cache_data(ttl=30)
def fetch_baseline_stats(boroughs: list | None = None) -> dict:
    """
    Compute a 7-day baseline (avg speed, congestion rate) used as a reference
    point for the executive summary.  Without a baseline, "current avg speed
    is 25 mph" tells you nothing.
    """
    since = datetime.utcnow() - timedelta(days=7)
    filt = {"processed_at": {"$gte": since}}
    if boroughs:
        filt["borough"] = {"$in": boroughs}
    try:
        pipeline = [
            {"$match": filt},
            {"$group": {
                "_id": None,
                "avg_speed":      {"$avg": "$speed"},
                "median_speed":   {"$avg": "$speed"},   # placeholder
                "total":          {"$sum": 1},
                "severe_count":   {"$sum": {"$cond": [{"$eq": ["$congestion_level", "SEVERE"]},   1, 0]}},
                "free_flow_count":{"$sum": {"$cond": [{"$eq": ["$congestion_level", "FREE_FLOW"]},1, 0]}},
            }},
        ]
        agg = list(get_mongo()[MONGO_DB]["traffic_processed"].aggregate(pipeline))
        if not agg:
            return {}
        a = agg[0]
        total = a.get("total") or 0
        return {
            "avg_speed":         round(float(a.get("avg_speed") or 0), 1),
            "severe_pct":        round((a.get("severe_count")    or 0) / max(total, 1) * 100, 2),
            "free_flow_pct":     round((a.get("free_flow_count") or 0) / max(total, 1) * 100, 2),
            "total_records":     total,
        }
    except Exception:
        return {}


# ── Pipeline Health & Status ───────────────────────────────────────────────────

def compute_health_status(df, dq, baseline) -> dict:
    """
    Return a composite health status:
        - level:  OPERATIONAL / DEGRADED / CRITICAL
        - color:  green / yellow / red
        - issues: list of human-readable problem descriptions
        - data_age_minutes: how stale is the most recent record
    """
    issues: list[str] = []
    level = "OPERATIONAL"

    # 1. MongoDB reachable?
    if not mongo_ok():
        return {
            "level": "CRITICAL", "color": "red",
            "issues": ["MongoDB unreachable - cannot read from storage layer"],
            "data_age_minutes": None,
        }

    # 2. Records present?
    if df is None or df.empty:
        issues.append("No records in current window")
        level = "DEGRADED"
        data_age = None
    else:
        # Data freshness: how long since the most recent record?
        if "processed_at" in df.columns:
            latest = pd.to_datetime(df["processed_at"], errors="coerce", utc=True).max()
            if pd.notnull(latest):
                now = pd.Timestamp.utcnow()
                data_age = max(0, int((now - latest).total_seconds() / 60))
                if data_age > 60:
                    issues.append(f"No new data for {data_age} minutes (stream processor may be down)")
                    level = "DEGRADED" if data_age < 240 else "CRITICAL"
            else:
                data_age = None
        else:
            data_age = None

    # 3. Quality score
    if not dq.empty and "quality_score" in dq.columns:
        recent_quality = dq["quality_score"].tail(5).mean()
        if pd.notnull(recent_quality):
            if recent_quality < 80:
                issues.append(f"Data quality at {recent_quality:.1f}% (target >= 95%)")
                level = "CRITICAL"
            elif recent_quality < 95:
                issues.append(f"Data quality at {recent_quality:.1f}% (target >= 95%)")
                if level == "OPERATIONAL":
                    level = "DEGRADED"

    color = {"OPERATIONAL": "green", "DEGRADED": "orange", "CRITICAL": "red"}[level]
    return {
        "level": level, "color": color,
        "issues": issues,
        "data_age_minutes": data_age,
    }


def compute_executive_summary(df, baseline) -> dict:
    """Build the executive summary card content."""
    if df is None or df.empty:
        return {}

    avg_speed_now = round(df["speed"].mean(), 1) if "speed" in df.columns else None
    baseline_speed = baseline.get("avg_speed") if baseline else None

    delta_speed = None
    delta_pct = None
    if avg_speed_now and baseline_speed:
        delta_speed = round(avg_speed_now - baseline_speed, 1)
        delta_pct   = round((avg_speed_now - baseline_speed) / baseline_speed * 100, 1)

    # Worst-performing borough now
    worst_borough = None
    worst_speed = None
    if "borough" in df.columns and "speed" in df.columns:
        bs = df.groupby("borough")["speed"].mean().sort_values()
        if not bs.empty:
            worst_borough = bs.index[0]
            worst_speed   = round(float(bs.iloc[0]), 1)

    # Severe congestion %
    severe_pct = None
    if "congestion_level" in df.columns and len(df) > 0:
        severe_pct = round(
            (df["congestion_level"] == "SEVERE").sum() / len(df) * 100, 1
        )

    return {
        "avg_speed_now":   avg_speed_now,
        "baseline_speed":  baseline_speed,
        "delta_speed":     delta_speed,
        "delta_pct":       delta_pct,
        "worst_borough":   worst_borough,
        "worst_speed":     worst_speed,
        "severe_pct":      severe_pct,
    }


def health_banner(status: dict):
    """Render a coloured banner at the top of the page reflecting overall health."""
    level  = status["level"]
    issues = status["issues"]
    age    = status.get("data_age_minutes")

    age_str = f"{age} min ago" if age is not None else "unknown"

    if level == "OPERATIONAL":
        st.success(f"🟢 **OPERATIONAL** — All systems healthy · Latest data: {age_str}")
    elif level == "DEGRADED":
        st.warning(
            f"🟡 **DEGRADED** — {len(issues)} issue(s) detected · Latest data: {age_str}\n\n"
            + "\n".join(f"- {i}" for i in issues)
        )
    else:  # CRITICAL
        st.error(
            f"🔴 **CRITICAL** — {len(issues)} issue(s) require attention · Latest data: {age_str}\n\n"
            + "\n".join(f"- {i}" for i in issues)
        )


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("NYC Traffic")
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


# ── Custom CSS for visual polish ──────────────────────────────────────────────
st.markdown("""
<style>
    /* Tighter top padding */
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }

    /* KPI metric cards: subtle border + background for visual grouping */
    [data-testid="stMetric"] {
        background-color: rgba(28, 131, 225, 0.04);
        border: 1px solid rgba(28, 131, 225, 0.12);
        padding: 12px 16px;
        border-radius: 8px;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
    }
    [data-testid="stMetricValue"] { font-size: 1.6rem; font-weight: 600; }
    [data-testid="stMetricLabel"] { font-size: 0.85rem; color: #555; }

    /* Tab styling — a bit more breathing room */
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        font-size: 1rem; font-weight: 500;
        padding: 8px 16px;
    }

    /* Section headers */
    h2 { margin-top: 1.5rem; padding-top: 0.5rem; border-top: 1px solid #eee; }
    h3 { color: #1f77b4; margin-top: 1rem; }
</style>
""", unsafe_allow_html=True)


# ── Header ────────────────────────────────────────────────────────────────────
st.title("NYC Traffic Analytics Pipeline")
st.caption("Real-time traffic monitoring · Spark streaming · ML predictions · NYC DOT data")


# ── Health banner (computed once, used across tabs) ───────────────────────────
_df_window  = fetch_recent_records(hours=time_hours, boroughs=selected_boroughs or None)
_dq_window  = fetch_quality_metrics(hours=time_hours)
_baseline   = fetch_baseline_stats(boroughs=selected_boroughs or None)
_health     = compute_health_status(_df_window, _dq_window, _baseline)
_exec_sum   = compute_executive_summary(_df_window, _baseline)

health_banner(_health)


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Overview",
    "Live Traffic",
    "ML Insights",
    "Historical",
    "Data Quality",
    "Lineage & Health",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    # Reuse data fetched for the health banner so we don't query MongoDB twice
    df    = _df_window
    stats = load_producer_stats()

    # ── Executive Summary (synthesized findings) ──────────────────────────────
    if _exec_sum and _exec_sum.get("avg_speed_now") is not None:
        st.subheader("Executive Summary")

        e1, e2, e3 = st.columns(3)

        # Card 1: speed vs baseline
        avg_now    = _exec_sum.get("avg_speed_now")
        delta_pct  = _exec_sum.get("delta_pct")
        delta_str  = ""
        if delta_pct is not None:
            sign = "+" if delta_pct >= 0 else ""
            delta_str = f"{sign}{delta_pct}% vs 7-day baseline ({_exec_sum.get('baseline_speed')} mph)"
        e1.metric(
            "Current Avg Speed",
            f"{avg_now} mph",
            delta=delta_str if delta_str else None,
            delta_color="normal" if (delta_pct or 0) >= 0 else "inverse",
        )

        # Card 2: worst borough
        worst_b = _exec_sum.get("worst_borough")
        worst_s = _exec_sum.get("worst_speed")
        if worst_b:
            e2.metric(
                "Slowest Borough Right Now",
                f"{worst_b}",
                delta=f"{worst_s} mph average",
                delta_color="off",
            )
        else:
            e2.metric("Slowest Borough", "—")

        # Card 3: severe congestion
        severe_pct = _exec_sum.get("severe_pct")
        baseline_severe = _baseline.get("severe_pct") if _baseline else None
        if severe_pct is not None and baseline_severe is not None:
            delta_severe = round(severe_pct - baseline_severe, 1)
            sign = "+" if delta_severe >= 0 else ""
            e3.metric(
                "Severe Congestion",
                f"{severe_pct}% of records",
                delta=f"{sign}{delta_severe} pp vs baseline",
                delta_color="inverse" if delta_severe > 0 else "normal",
            )
        else:
            e3.metric("Severe Congestion", f"{severe_pct or 0}% of records")

        # Narrative interpretation
        if avg_now and _exec_sum.get("baseline_speed"):
            if delta_pct and delta_pct < -5:
                st.info(f"Traffic is **{abs(delta_pct)}% slower** than the 7-day average. "
                        f"Worst-affected borough: **{worst_b}** ({worst_s} mph).")
            elif delta_pct and delta_pct > 5:
                st.success(f"Traffic is **{delta_pct}% faster** than the 7-day average — flowing well.")
            else:
                st.caption(f"Traffic is consistent with the 7-day baseline of {_exec_sum.get('baseline_speed')} mph.")

        st.divider()

    # ── KPI row ───────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)

    total_records = len(df)
    avg_speed     = round(df["speed"].mean(), 1) if not df.empty and "speed" in df.columns else 0
    anomaly_count = 0
    quality_score = 0.0

    dq = fetch_quality_metrics(hours=time_hours)
    if not dq.empty and "quality_score" in dq.columns:
        quality_score = round(dq["quality_score"].mean(), 1)

    # Try anomaly count using cached predictions
    try:
        pred = load_predictor()
        if pred is not None and not df.empty:
            df_scored = run_predictions(pred, df.head(100))
            if "is_anomaly" in df_scored.columns:
                anomaly_count = int(df_scored["is_anomaly"].sum())
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
            # Stratified sample: preserve borough / link diversity rather than
            # random-sampling (which can leave dense areas blank on the map).
            sample_size = min(1500, len(df_map))
            if "link_id" in df_map.columns and len(df_map) > sample_size:
                # One latest record per link, then sample if still too many
                df_map_sample = (
                    df_map.sort_values("processed_at", ascending=False)
                    .drop_duplicates(subset="link_id")
                )
                if len(df_map_sample) > sample_size:
                    df_map_sample = df_map_sample.sample(sample_size, random_state=42)
            else:
                df_map_sample = df_map
            fig_map = px.scatter_mapbox(
                df_map_sample,
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
                        df_pred = run_predictions(predictor, df_raw)

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

                    # ── Real-time alerting ─────────────────────────────────────
                    if anom_pct >= 10:
                        st.error(
                            f"🚨 **ALERT** — anomaly rate at {anom_pct}% of recent records "
                            f"({n_anomalies}/{len(df_pred)}). Investigate stream or sensor health."
                        )
                    elif anom_pct >= 6:
                        st.warning(
                            f"⚠️ Elevated anomaly rate ({anom_pct}%). Watch for further increase."
                        )

                    # ── Anomaly records table ─────────────────────────────────
                    if n_anomalies > 0:
                        st.subheader(f"⚠️ Anomaly Alerts ({n_anomalies} records)")
                        anom_cols = [c for c in ["link_name", "borough", "speed",
                                                 "congestion_level", "anomaly_score",
                                                 "processed_at"]
                                     if c in df_pred.columns]
                        anom_df = df_pred[df_pred["is_anomaly"] == True][anom_cols].head(20)
                        st.dataframe(anom_df, use_container_width=True, height=250)
                        # CSV download
                        st.download_button(
                            "Download anomalies (CSV)",
                            data=anom_df.to_csv(index=False).encode("utf-8"),
                            file_name=f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                        )

                    st.divider()

                    # ════════════════════════════════════════════════════════════
                    # MODEL EXPLAINABILITY (Master's-level Explainable AI)
                    # ════════════════════════════════════════════════════════════
                    st.subheader("Model Explainability")
                    st.caption(
                        "How and why the models make their predictions — feature "
                        "importances from training, per-prediction breakdown, and "
                        "calibration diagnostics."
                    )

                    expl_tab1, expl_tab2, expl_tab3 = st.tabs([
                        "Feature Importance",
                        "Per-Prediction Breakdown",
                        "Calibration & Residuals",
                    ])

                    # ── Feature importance from saved model metadata ──────────
                    with expl_tab1:
                        cols = st.columns(2)
                        for col, name, label in [
                            (cols[0], "congestion_classifier", "Congestion Classifier"),
                            (cols[1], "speed_predictor",       "Speed Predictor"),
                        ]:
                            with col:
                                meta_path = models_dir / f"{name}_metadata.json"
                                if not meta_path.exists():
                                    st.info(f"No metadata for {label}")
                                    continue
                                with open(meta_path) as f:
                                    meta = json.load(f)
                                imps = meta.get("feature_importances", {})
                                if not imps:
                                    st.info(f"No feature importances recorded for {label}")
                                    continue
                                df_imp = (
                                    pd.DataFrame(list(imps.items()),
                                                 columns=["feature", "importance"])
                                    .sort_values("importance", ascending=True)
                                )
                                fig = px.bar(
                                    df_imp, x="importance", y="feature",
                                    orientation="h",
                                    title=f"{label} — Feature Importance",
                                    color="importance",
                                    color_continuous_scale="Blues",
                                    labels={"importance": "Importance", "feature": ""},
                                )
                                fig.update_layout(height=340, coloraxis_showscale=False,
                                                  margin=dict(t=40, b=0))
                                st.plotly_chart(fig, use_container_width=True)

                                # Interpretation note for the reviewer
                                top = df_imp.iloc[-1]
                                st.caption(
                                    f"Top feature: **{top['feature']}** "
                                    f"(importance {top['importance']:.3f}). "
                                    f"Latitude/longitude dominating means each road segment "
                                    f"has a characteristic baseline speed the model has learned."
                                )

                    # ── Per-prediction breakdown ──────────────────────────────
                    with expl_tab2:
                        st.markdown(
                            "Pick one record to see exactly what the models predicted "
                            "and the feature values that drove that prediction."
                        )

                        # Build a label for each record
                        df_choices = df_pred.copy()
                        df_choices["__label"] = (
                            df_choices.get("link_name", "?").astype(str).str.slice(0, 35)
                            + " | " + df_choices.get("borough", "?").astype(str)
                            + " | " + df_choices.get("speed", 0).round(1).astype(str) + " mph"
                        )
                        sel_idx = st.selectbox(
                            "Record",
                            options=df_choices.index,
                            format_func=lambda i: df_choices.loc[i, "__label"],
                            key="prediction_selector",
                        )
                        rec = df_choices.loc[sel_idx].to_dict()

                        c1, c2, c3 = st.columns(3)
                        c1.metric("Actual Speed",    f"{rec.get('speed', 0):.1f} mph")
                        c2.metric("Predicted Speed", f"{rec.get('predicted_speed', 0):.1f} mph")
                        actual_cong = rec.get("congestion_level", "—")
                        pred_cong   = rec.get("predicted_congestion", "—")
                        match = "correct" if actual_cong == pred_cong else "⚠️ differs"
                        c3.metric(
                            "Predicted Congestion",
                            pred_cong,
                            delta=f"{rec.get('congestion_confidence', 0)*100:.0f}% confidence — {match}",
                            delta_color="off",
                        )

                        # Anomaly badge
                        if rec.get("is_anomaly"):
                            st.error(f"Flagged as ANOMALY · score = {rec.get('anomaly_score', 0):.3f}")
                        else:
                            st.success(f"✓ Normal record · anomaly score = {rec.get('anomaly_score', 0):.3f}")

                        # Feature contribution: weight feature value by its importance
                        meta_path = models_dir / "congestion_classifier_metadata.json"
                        if meta_path.exists():
                            with open(meta_path) as f:
                                meta = json.load(f)
                            imps = meta.get("feature_importances", {})
                            rows = []
                            for feat, importance in imps.items():
                                val = rec.get(feat)
                                rows.append({
                                    "feature":       feat,
                                    "value":         val,
                                    "importance":    importance,
                                    "contribution":  abs(importance) * 100,
                                })
                            df_contrib = pd.DataFrame(rows).sort_values("contribution", ascending=True)
                            fig = px.bar(
                                df_contrib, x="contribution", y="feature",
                                orientation="h",
                                hover_data=["value"],
                                title="Feature contributions to this prediction",
                                color="contribution",
                                color_continuous_scale="Blues",
                                labels={"contribution": "Contribution score", "feature": ""},
                            )
                            fig.update_layout(height=320, coloraxis_showscale=False,
                                              margin=dict(t=40, b=0))
                            st.plotly_chart(fig, use_container_width=True)
                            st.caption(
                                "Contribution = |feature importance| × 100. Hover to see "
                                "the actual feature value used for this record."
                            )

                    # ── Calibration plot + residuals ──────────────────────────
                    with expl_tab3:
                        if "predicted_speed" in df_pred.columns and "speed" in df_pred.columns:
                            df_calib = df_pred.dropna(subset=["speed", "predicted_speed"]).copy()
                            df_calib["residual"] = df_calib["speed"] - df_calib["predicted_speed"]
                            df_calib["abs_residual"] = df_calib["residual"].abs()

                            mae_live = df_calib["abs_residual"].mean()
                            bias     = df_calib["residual"].mean()

                            mc1, mc2, mc3 = st.columns(3)
                            mc1.metric("Live MAE",  f"{mae_live:.2f} mph")
                            mc2.metric("Bias",      f"{bias:+.2f} mph",
                                       delta="positive = under-predicting", delta_color="off")
                            mc3.metric("Max abs error", f"{df_calib['abs_residual'].max():.1f} mph")

                            cc1, cc2 = st.columns(2)
                            with cc1:
                                # Calibration scatter (binned)
                                df_calib["bin"] = pd.cut(df_calib["predicted_speed"],
                                                         bins=10).apply(lambda x: x.mid)
                                bin_stats = df_calib.groupby("bin").agg(
                                    pred_mean=("predicted_speed", "mean"),
                                    actual_mean=("speed", "mean"),
                                    n=("speed", "count"),
                                ).reset_index()
                                fig_cal = px.scatter(
                                    bin_stats, x="pred_mean", y="actual_mean",
                                    size="n",
                                    title="Calibration: Predicted vs Actual (binned)",
                                    labels={"pred_mean": "Predicted (mph)",
                                            "actual_mean": "Actual mean (mph)",
                                            "n": "Count"},
                                )
                                # Perfect calibration line
                                if not bin_stats.empty:
                                    m = float(max(bin_stats["pred_mean"].max(),
                                                  bin_stats["actual_mean"].max()))
                                    fig_cal.add_shape(
                                        type="line", x0=0, y0=0, x1=m, y1=m,
                                        line=dict(dash="dash", color="gray"),
                                    )
                                fig_cal.update_layout(height=340, margin=dict(t=40, b=0))
                                st.plotly_chart(fig_cal, use_container_width=True)
                                st.caption("Points should hug the diagonal. Points above = under-prediction; below = over-prediction.")

                            with cc2:
                                # Residual histogram
                                fig_res = px.histogram(
                                    df_calib, x="residual", nbins=40,
                                    title="Residuals (actual - predicted)",
                                    color_discrete_sequence=["#1f77b4"],
                                )
                                fig_res.add_vline(x=0, line_dash="dash", line_color="black")
                                fig_res.update_layout(height=340, margin=dict(t=40, b=0))
                                st.plotly_chart(fig_res, use_container_width=True)
                                st.caption("Should be centred on zero (no bias) and roughly bell-shaped.")
                        else:
                            st.info("No predicted_speed available for calibration analysis.")

                    # ── Model versioning / metadata ───────────────────────────
                    with st.expander("Model Versioning & Training Metadata"):
                        for name, label in [
                            ("congestion_classifier", "Congestion Classifier"),
                            ("speed_predictor",       "Speed Predictor"),
                            ("anomaly_detector",      "Anomaly Detector"),
                        ]:
                            meta_path = models_dir / f"{name}_metadata.json"
                            if not meta_path.exists():
                                continue
                            with open(meta_path) as f:
                                meta = json.load(f)
                            st.markdown(f"### {label}")
                            mv1, mv2, mv3, mv4 = st.columns(4)
                            mv1.metric("Code version",   meta.get("code_version", "?"))
                            mv2.metric("Git commit",     (meta.get("git_commit", "?") or "?")[:8])
                            mv3.metric("Train size",     f"{meta.get('train_size', '?'):,}"
                                       if isinstance(meta.get('train_size'), int) else "?")
                            mv4.metric("Trained at",     (meta.get("trained_at", "?") or "?")[:10])
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
                    # Detect encoding: Spark uses 1-7 (Sun-Sat); Python weekday is 0-6 (Mon-Sun)
                    cols = sorted([int(c) for c in pivot.columns if pd.notnull(c)])
                    if cols and min(cols) == 0:           # 0-6 Monday-Sunday (Python)
                        day_labels = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu",
                                      4: "Fri", 5: "Sat", 6: "Sun"}
                    else:                                  # 1-7 Sunday-Saturday (Spark)
                        day_labels = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed",
                                      5: "Thu", 6: "Fri", 7: "Sat"}
                    pivot.columns = [day_labels.get(int(c), str(c)) for c in pivot.columns]

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
        valid_pct = round(valid / total * 100, 1) if total > 0 else 0.0
        q4.metric("Valid Records",     f"{valid:,}",
                  delta=f"{valid_pct}% valid")

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
            except Exception as exc:
                st.caption(f"⚠️ Could not render null-count chart: {exc}")

        # ── Recent quality batches table ──────────────────────────────────────
        st.subheader("Recent Quality Batches")
        display_cols = [c for c in
            ["batch_id", "recorded_at", "total_records", "valid_records",
             "quality_score", "source"]
            if c in dq.columns]
        st.dataframe(dq[display_cols].tail(20), use_container_width=True,
                     hide_index=True, height=280)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — LINEAGE & PIPELINE HEALTH
# ══════════════════════════════════════════════════════════════════════════════
# Master's-level data provenance: every batch processed by Spark or the
# batch_processor records a structured lineage entry with source (Kafka
# offsets / MongoDB collection), destination, transformations applied,
# duration, and status. This tab makes that auditable.
with tab6:
    lineage_df = fetch_lineage_entries(limit=200)

    if lineage_df.empty:
        st.info(
            "No lineage entries yet. Run the Spark stream processor or batch "
            "processor — every batch records its provenance to the "
            "`data_lineage` collection."
        )
    else:
        st.subheader("Pipeline Activity")
        st.caption(
            "Each row is a single batch — captured from Kafka (stream) or "
            "MongoDB (batch). Lineage proves the system is auditable end-to-end."
        )

        # ── KPI strip ─────────────────────────────────────────────────────────
        l1, l2, l3, l4, l5 = st.columns(5)
        total_batches = len(lineage_df)
        success_rate  = (lineage_df["status"] == "success").mean() * 100 \
                        if "status" in lineage_df.columns else 0
        avg_duration  = float(lineage_df["duration_ms"].mean()) \
                        if "duration_ms" in lineage_df.columns else 0
        total_records = int(lineage_df["record_count"].sum()) \
                        if "record_count" in lineage_df.columns else 0
        last_batch    = lineage_df["started_at"].max() \
                        if "started_at" in lineage_df.columns else None

        l1.metric("Recent Batches",     f"{total_batches:,}")
        l2.metric("Success Rate",       f"{success_rate:.1f}%",
                  delta="100% target", delta_color="off")
        l3.metric("Avg Batch Duration", f"{avg_duration/1000:.1f} s")
        l4.metric("Records Processed",  f"{total_records:,}")
        if last_batch is not None and pd.notnull(last_batch):
            age_min = max(0, int((pd.Timestamp.utcnow() - last_batch).total_seconds() / 60))
            l5.metric("Last Activity", f"{age_min} min ago",
                      delta_color="inverse" if age_min > 30 else "off")
        else:
            l5.metric("Last Activity", "—")

        # Alert if success rate dropped
        if success_rate < 95 and total_batches >= 5:
            st.error(
                f"Pipeline success rate is **{success_rate:.1f}%** "
                f"({(lineage_df['status'] != 'success').sum()} failed batches in last {total_batches}). "
                "Inspect the failed entries below."
            )

        st.divider()

        col_a, col_b = st.columns(2)

        # ── Phase distribution + status breakdown ─────────────────────────────
        with col_a:
            if "phase" in lineage_df.columns:
                phase_counts = lineage_df["phase"].value_counts().reset_index()
                phase_counts.columns = ["Phase", "Count"]
                fig_phase = px.bar(
                    phase_counts, x="Phase", y="Count",
                    title="Batches by Pipeline Phase",
                    color="Phase",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_phase.update_layout(height=320, showlegend=False, margin=dict(t=40, b=0))
                st.plotly_chart(fig_phase, use_container_width=True)

        with col_b:
            if "status" in lineage_df.columns:
                status_counts = lineage_df["status"].value_counts().reset_index()
                status_counts.columns = ["Status", "Count"]
                color_map = {"success": "#2ecc71", "partial": "#f39c12",
                             "failed":  "#e74c3c", "skipped_empty": "#95a5a6"}
                fig_status = px.pie(
                    status_counts, names="Status", values="Count",
                    title="Batch Status Distribution",
                    color="Status",
                    color_discrete_map=color_map,
                    hole=0.4,
                )
                fig_status.update_layout(height=320, margin=dict(t=40, b=0))
                st.plotly_chart(fig_status, use_container_width=True)

        # ── Duration trend over time (look for slowdowns) ─────────────────────
        if "started_at" in lineage_df.columns and "duration_ms" in lineage_df.columns:
            df_dur = lineage_df.dropna(subset=["started_at", "duration_ms"]).copy()
            if not df_dur.empty:
                df_dur["duration_sec"] = df_dur["duration_ms"] / 1000
                fig_dur = px.scatter(
                    df_dur.sort_values("started_at"),
                    x="started_at", y="duration_sec",
                    color="phase" if "phase" in df_dur.columns else None,
                    size="record_count" if "record_count" in df_dur.columns else None,
                    title="Batch Duration Trend (size = record count)",
                    labels={"started_at": "Time", "duration_sec": "Duration (s)",
                            "phase": "Phase"},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                    hover_data={"record_count": True, "status": True},
                )
                fig_dur.update_layout(height=340, margin=dict(t=40, b=0))
                st.plotly_chart(fig_dur, use_container_width=True)
                st.caption(
                    "Look for upward trends (degradation) or sudden spikes (resource "
                    "contention). Stable line = healthy pipeline."
                )

        # ── Throughput trend ───────────────────────────────────────────────────
        if "started_at" in lineage_df.columns and "record_count" in lineage_df.columns:
            df_thr = lineage_df.dropna(subset=["started_at", "record_count"]).copy()
            if not df_thr.empty:
                fig_thr = px.area(
                    df_thr.sort_values("started_at"),
                    x="started_at", y="record_count",
                    color="phase" if "phase" in df_thr.columns else None,
                    title="Records Processed per Batch",
                    labels={"started_at": "Time", "record_count": "Records",
                            "phase": "Phase"},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_thr.update_layout(height=300, margin=dict(t=40, b=0))
                st.plotly_chart(fig_thr, use_container_width=True)

        st.divider()

        # ── Failed batches highlight ───────────────────────────────────────────
        if "status" in lineage_df.columns:
            failed = lineage_df[lineage_df["status"].isin(["failed", "partial"])].copy()
            if not failed.empty:
                st.subheader(f"⚠️ Failed / Partial Batches ({len(failed)})")
                show_cols = [c for c in ["lineage_id", "phase", "processor",
                                         "started_at", "duration_ms",
                                         "record_count", "status", "errors"]
                             if c in failed.columns]
                st.dataframe(failed[show_cols].head(20),
                             use_container_width=True, height=240)

        # ── Recent batches table (drill-down) ─────────────────────────────────
        st.subheader("Recent Batches — Full Lineage")
        show_cols = [c for c in [
            "lineage_id", "phase", "processor", "processor_version",
            "started_at", "duration_ms", "record_count", "status",
        ] if c in lineage_df.columns]
        recent = lineage_df[show_cols].head(50)
        st.dataframe(recent, use_container_width=True, height=320)

        # CSV export
        st.download_button(
            "Download lineage entries (CSV)",
            data=lineage_df.head(200).to_csv(index=False).encode("utf-8"),
            file_name=f"lineage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

        # ── Inspect a single lineage entry (drill-down) ────────────────────────
        with st.expander("Inspect a single lineage entry"):
            if "lineage_id" in lineage_df.columns:
                ids = lineage_df["lineage_id"].tolist()
                selected = st.selectbox("Pick a batch:", options=ids, key="lineage_picker")
                row = lineage_df[lineage_df["lineage_id"] == selected].iloc[0].to_dict()
                # Pretty print as JSON
                st.json(row)


# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    st.markdown(
        f'<meta http-equiv="refresh" content="{REFRESH_SECS}">',
        unsafe_allow_html=True,
    )
    st.sidebar.caption(f"Next refresh in ~{REFRESH_SECS}s")
