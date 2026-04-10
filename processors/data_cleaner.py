"""
Phase 2 – Data Cleaning & Preprocessing

Pure-Python functions shared by both the Spark stream processor and the
batch processor.  No Spark dependency here — Spark calls these via UDFs or
applies them in foreachBatch.

Transformations applied to each record:
  • Numeric casting  – speed → float, travel_time → int, status → int
  • Timestamp parsing – data_as_of → datetime
  • Time features     – hour_of_day, day_of_week, is_peak_hour, time_bucket
  • Location          – extract first lat/lon from link_points
  • Congestion level  – FREE_FLOW / MODERATE / CONGESTED / SEVERE
  • Congestion score  – 0.0 (free) → 1.0 (gridlock), referenced to 60 mph
  • Quality flags     – list of data issues found in the record
  • Lineage           – processed_at, processor_version, phase
"""
from datetime import datetime, timezone
from typing import Optional

PROCESSOR_VERSION = "2.0.0"

# Speed thresholds (mph) that define congestion tiers
_SPEED_THRESHOLDS = {
    "FREE_FLOW": 35,
    "MODERATE":  15,
    "CONGESTED":  5,
}

# Peak-hour windows (inclusive start, exclusive end, weekdays only)
_PEAK_WINDOWS = [(7, 9), (17, 19)]


# ── Public API ─────────────────────────────────────────────────────────────────

def clean_record(raw: dict) -> dict:
    """
    Clean and enrich a single raw traffic record.

    Args:
        raw: Record dict as stored in Kafka (may include _pipeline sub-doc).

    Returns:
        New dict with cleaned fields and computed features.
        The original dict is not mutated.
    """
    pipeline = raw.get("_pipeline") or {}

    # Prefer pre-cast values from Phase 1 _pipeline, fall back to raw strings
    speed       = safe_float(pipeline.get("speed_float")       or raw.get("speed"))
    travel_time = safe_int(pipeline.get("travel_time_int")     or raw.get("travel_time"))
    status      = safe_int(pipeline.get("status_int")          or raw.get("status"))

    ts       = parse_timestamp(raw.get("data_as_of"))
    time_ft  = extract_time_features(ts)
    location = extract_first_location(raw.get("link_points"))

    now = datetime.now(timezone.utc)

    return {
        # ── Identity ──────────────────────────────────────────────────
        "record_id":   raw.get("id"),
        "link_id":     raw.get("link_id"),
        "transcom_id": raw.get("transcom_id"),
        "link_name":   raw.get("link_name"),
        "owner":       raw.get("owner"),

        # ── Geography ─────────────────────────────────────────────────
        "borough":   raw.get("borough"),
        "latitude":  location["latitude"],
        "longitude": location["longitude"],

        # ── Measurements ──────────────────────────────────────────────
        "speed":       speed,
        "travel_time": travel_time,
        "status":      status,

        # ── Temporal ──────────────────────────────────────────────────
        "data_as_of_parsed": ts,
        **time_ft,

        # ── Computed Features ─────────────────────────────────────────
        "congestion_level": compute_congestion_level(speed),
        "congestion_score": compute_congestion_score(speed),

        # ── Data Quality ──────────────────────────────────────────────
        "quality_flags": compute_quality_flags(raw, speed, ts),

        # ── Lineage ───────────────────────────────────────────────────
        "_lineage": {
            "ingested_at":        pipeline.get("ingested_at"),
            "processed_at":       now.isoformat(),
            "source":             pipeline.get("source", "nyc_dot_speeds_nbe"),
            "dataset_id":         pipeline.get("dataset_id", "i4gi-tjb9"),
            "phase":              "stream_processing",
            "processor_version":  PROCESSOR_VERSION,
        },
    }


def compute_congestion_level(speed: Optional[float]) -> str:
    """Classify speed into a congestion tier."""
    if speed is None:
        return "UNKNOWN"
    if speed >= _SPEED_THRESHOLDS["FREE_FLOW"]:
        return "FREE_FLOW"
    if speed >= _SPEED_THRESHOLDS["MODERATE"]:
        return "MODERATE"
    if speed >= _SPEED_THRESHOLDS["CONGESTED"]:
        return "CONGESTED"
    return "SEVERE"


def compute_congestion_score(speed: Optional[float]) -> Optional[float]:
    """
    Normalised score: 0.0 = free flow (≥60 mph), 1.0 = gridlock (0 mph).
    Reference speed = 60 mph (typical NYC highway free-flow).
    """
    if speed is None:
        return None
    return round(1.0 - min(speed / 60.0, 1.0), 4)


def extract_time_features(dt: Optional[datetime]) -> dict:
    """Return hour, weekday, peak-hour flag, weekend flag, and time bucket."""
    if dt is None:
        return {
            "hour_of_day": None,
            "day_of_week": None,
            "is_peak_hour": None,
            "is_weekend":   None,
            "time_bucket":  None,
        }

    hour = dt.hour
    dow  = dt.weekday()          # 0 = Monday … 6 = Sunday
    is_weekend = dow >= 5
    is_peak    = not is_weekend and any(
        lo <= hour < hi for lo, hi in _PEAK_WINDOWS
    )

    if 0 <= hour < 6:
        bucket = "OVERNIGHT"
    elif 7 <= hour < 9 and not is_weekend:
        bucket = "MORNING_PEAK"
    elif 17 <= hour < 19 and not is_weekend:
        bucket = "EVENING_PEAK"
    else:
        bucket = "OFF_PEAK"

    return {
        "hour_of_day": hour,
        "day_of_week": dow,
        "is_peak_hour": is_peak,
        "is_weekend":   is_weekend,
        "time_bucket":  bucket,
    }


def extract_first_location(link_points: Optional[str]) -> dict:
    """
    Parse the first 'lat,lon' pair from the space-separated link_points string.
    Returns {"latitude": float|None, "longitude": float|None}.
    """
    if not link_points:
        return {"latitude": None, "longitude": None}
    try:
        first = link_points.strip().split(" ")[0]
        lat, lon = map(float, first.split(","))
        return {"latitude": lat, "longitude": lon}
    except (ValueError, IndexError):
        return {"latitude": None, "longitude": None}


def compute_quality_flags(
    raw: dict,
    speed: Optional[float],
    ts: Optional[datetime],
) -> list:
    """Return a list of data-quality flag strings for this record."""
    flags = []
    if speed is None:
        flags.append("MISSING_SPEED")
    elif speed == 0.0:
        flags.append("ZERO_SPEED")
    if ts is None:
        flags.append("MISSING_TIMESTAMP")
    if not raw.get("borough"):
        flags.append("MISSING_BOROUGH")
    if not raw.get("link_id"):
        flags.append("MISSING_LINK_ID")
    return flags


# ── Helpers ────────────────────────────────────────────────────────────────────

def safe_float(value) -> Optional[float]:
    """Cast value to float, return None on failure."""
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_int(value) -> Optional[int]:
    """Cast value to int (via float), return None on failure."""
    if value in (None, "", "null"):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def parse_timestamp(ts_str: Optional[str]) -> Optional[datetime]:
    """Try several ISO-8601 formats and return a naive UTC datetime or None."""
    if not ts_str:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    return None
