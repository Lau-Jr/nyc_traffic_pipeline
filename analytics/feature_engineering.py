"""
Phase 3 – Feature Engineering

Shared feature preparation used by both the training pipeline and the
real-time predictor.  No Spark dependency — pure pandas / numpy.
"""
import numpy as np
import pandas as pd
from typing import Optional

# ── Constants ──────────────────────────────────────────────────────────────────

# Ordered class labels for the congestion classifier
CONGESTION_LABELS = ["FREE_FLOW", "MODERATE", "CONGESTED", "SEVERE"]
CONGESTION_LABEL_MAP   = {label: i for i, label in enumerate(CONGESTION_LABELS)}
CONGESTION_LABEL_UNMAP = {i: label for label, i in CONGESTION_LABEL_MAP.items()}

BOROUGH_LIST = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]

# Core feature columns used by the congestion classifier and anomaly detector
FEATURE_COLS = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_peak_hour",
    "borough_encoded",
    "latitude",
    "longitude",
]

# Extended feature set for the speed predictor (adds travel_time)
SPEED_FEATURE_COLS = FEATURE_COLS + ["travel_time"]

# ── Encoding helpers ───────────────────────────────────────────────────────────

def encode_borough(borough) -> int:
    """Map borough name → integer index.  Unknown / null → -1."""
    try:
        return BOROUGH_LIST.index(str(borough))
    except (ValueError, TypeError):
        return -1


def encode_labels(series: pd.Series) -> pd.Series:
    """Map congestion_level strings → integer class labels (0-3)."""
    return series.map(CONGESTION_LABEL_MAP).fillna(-1).astype(int)


def decode_labels(series: pd.Series) -> pd.Series:
    """Map integer class labels back → congestion_level strings."""
    return series.map(CONGESTION_LABEL_UNMAP).fillna("UNKNOWN")


# ── Main preparation function ──────────────────────────────────────────────────

def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform a raw MongoDB DataFrame into a model-ready feature DataFrame.

    Applies:
      - Borough label encoding
      - Boolean → int cast
      - Numeric coercion and median imputation for nulls
      - Drops rows where borough is unknown (-1) or speed is null

    Returns a copy; does not mutate the input.
    """
    out = df.copy()

    # Borough encoding
    out["borough_encoded"] = out["borough"].apply(encode_borough)

    # Booleans → int
    for col in ("is_weekend", "is_peak_hour"):
        if col in out.columns:
            out[col] = out[col].fillna(False).astype(int)

    # Numeric coercion + median fill
    for col in ["hour_of_day", "day_of_week", "latitude", "longitude", "travel_time"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            median = out[col].median()
            out[col] = out[col].fillna(median if not np.isnan(median) else 0)

    return out


def build_X(df: pd.DataFrame, feature_cols: list) -> pd.DataFrame:
    """Select and return only the model feature columns from df."""
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")
    return df[feature_cols].copy()


def record_to_features(record: dict) -> pd.DataFrame:
    """
    Convert a single record dict (as stored in MongoDB / produced by Kafka)
    to a one-row feature DataFrame suitable for real-time prediction.
    """
    row = {
        "hour_of_day":  record.get("hour_of_day",  0) or 0,
        "day_of_week":  record.get("day_of_week",   1) or 1,
        "is_weekend":   int(bool(record.get("is_weekend",   False))),
        "is_peak_hour": int(bool(record.get("is_peak_hour", False))),
        "borough":      record.get("borough", ""),
        "latitude":     record.get("latitude",  0.0) or 0.0,
        "longitude":    record.get("longitude", 0.0) or 0.0,
        "travel_time":  record.get("travel_time", 0) or 0,
        "speed":        record.get("speed"),
        "congestion_level": record.get("congestion_level", "UNKNOWN"),
    }
    df = pd.DataFrame([row])
    df["borough_encoded"] = df["borough"].apply(encode_borough)
    return df
