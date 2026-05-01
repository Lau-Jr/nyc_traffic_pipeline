"""
Phase 3 – Feature Engineering (v2: leakage-free)

Shared feature preparation used by training, evaluation, and the real-time
predictor.  Pure pandas / numpy / sklearn — no Spark dependency.

Key design principles
─────────────────────
1.  Imputation is fit on *training data only*, then applied to validation /
    test / live data — preventing leakage of future statistics into the past.
2.  Feature sets are deliberately scoped per model to avoid target leakage:

      * Congestion Classifier  – only causal features known at inference time
        (time, borough, location).  Speed and travel_time are EXCLUDED
        because congestion_level is computed deterministically from speed.

      * Speed Predictor        – same causal features.  travel_time is also
        EXCLUDED because it is near-perfectly correlated with speed
        (travel_time = segment_length / speed).  Including it would make
        the regression trivially solvable and inflate R².

      * Anomaly Detector       – uses raw measurements (speed, travel_time)
        plus context features.  Unsupervised, so no target leakage concern.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer

# ── Constants ──────────────────────────────────────────────────────────────────

# Ordered class labels for the congestion classifier
CONGESTION_LABELS      = ["FREE_FLOW", "MODERATE", "CONGESTED", "SEVERE"]
CONGESTION_LABEL_MAP   = {label: i for i, label in enumerate(CONGESTION_LABELS)}
CONGESTION_LABEL_UNMAP = {i: label for label, i in CONGESTION_LABEL_MAP.items()}

BOROUGH_LIST = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]

# ── Feature sets (scoped per model to prevent leakage) ─────────────────────────

# Causal features available at inference time.  Used by congestion classifier
# and speed predictor.  No speed / travel_time → no target leakage.
CAUSAL_FEATURE_COLS = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_peak_hour",
    "borough_encoded",
    "latitude",
    "longitude",
]

# Anomaly detector uses raw measurements + context.  Unsupervised, so no
# leakage concern.  These are the features whose joint distribution defines
# "normal" traffic.
ANOMALY_FEATURE_COLS = [
    "speed",
    "travel_time",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_peak_hour",
    "borough_encoded",
    "latitude",
    "longitude",
]

# Aliases for backwards compatibility
FEATURE_COLS       = CAUSAL_FEATURE_COLS
SPEED_FEATURE_COLS = CAUSAL_FEATURE_COLS  # No travel_time → no leakage

NUMERIC_COLS = ["hour_of_day", "day_of_week", "latitude", "longitude",
                "travel_time", "speed"]
BOOLEAN_COLS = ["is_weekend", "is_peak_hour"]


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


# ── Type coercion (no imputation — leak-free) ──────────────────────────────────

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Type-coerce the input frame *without* imputing missing values.
    Imputation is the responsibility of a fitted Imputer (see fit_imputer).
    """
    out = df.copy()

    # Borough encoding (-1 for unknown)
    if "borough" in out.columns:
        out["borough_encoded"] = out["borough"].apply(encode_borough)

    # Booleans → int (False if missing)
    for col in BOOLEAN_COLS:
        if col in out.columns:
            out[col] = out[col].fillna(False).astype(int)

    # Numeric coercion only (NaN preserved for imputer)
    for col in NUMERIC_COLS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    return out


def build_X(df: pd.DataFrame, feature_cols: list) -> pd.DataFrame:
    """Select and return only the model feature columns from df."""
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")
    return df[feature_cols].copy()


# ── Imputer pipeline (fit on TRAIN, apply to VAL/TEST/LIVE) ────────────────────

class FeatureImputer:
    """
    Wraps a sklearn SimpleImputer per feature column.  The imputer is fit
    on training data only; the same imputer is then applied to validation,
    test and live data.  This prevents median-leakage from the future.
    """

    def __init__(self, feature_cols: list, strategy: str = "median"):
        self.feature_cols = feature_cols
        self.strategy = strategy
        self._imputer = SimpleImputer(strategy=strategy)
        self._fitted = False

    def fit(self, X: pd.DataFrame) -> "FeatureImputer":
        self._imputer.fit(X[self.feature_cols].values)
        self._fitted = True
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if not self._fitted:
            raise RuntimeError("FeatureImputer must be fit before transform")
        out = X.copy()
        out[self.feature_cols] = self._imputer.transform(out[self.feature_cols].values)
        return out

    def fit_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        return self.fit(X).transform(X)

    @property
    def statistics_(self) -> dict:
        """Return the imputation values (median per feature) for inspection."""
        if not self._fitted:
            return {}
        return dict(zip(self.feature_cols, self._imputer.statistics_.tolist()))

    def save(self, path: Path):
        joblib.dump(self, Path(path))

    @staticmethod
    def load(path: Path) -> "FeatureImputer":
        return joblib.load(Path(path))


# ── Single-record helpers (real-time inference) ────────────────────────────────

def record_to_features(record: dict) -> pd.DataFrame:
    """
    Convert one record dict (as stored in MongoDB / produced by Kafka) to a
    one-row DataFrame.  Output is type-coerced but NOT imputed — the predictor
    must apply its saved FeatureImputer.
    """
    row = {
        "hour_of_day":  record.get("hour_of_day"),
        "day_of_week":  record.get("day_of_week"),
        "is_weekend":   bool(record.get("is_weekend",   False)),
        "is_peak_hour": bool(record.get("is_peak_hour", False)),
        "borough":      record.get("borough", ""),
        "latitude":     record.get("latitude"),
        "longitude":    record.get("longitude"),
        "travel_time":  record.get("travel_time"),
        "speed":        record.get("speed"),
        "congestion_level": record.get("congestion_level", "UNKNOWN"),
    }
    df = pd.DataFrame([row])
    return coerce_types(df)


# ── Temporal split (proper for time-series data) ───────────────────────────────

def temporal_split(
    df: pd.DataFrame,
    time_col: str = "processed_at",
    train_frac: float = 0.70,
    val_frac:   float = 0.15,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Sort df by time_col and split into (train, val, test) with no shuffling.
    Default: 70/15/15.  Required for time-series data — random splits leak
    future information into past.
    """
    if time_col not in df.columns:
        raise ValueError(f"Column '{time_col}' not in DataFrame")

    df = df.sort_values(time_col).reset_index(drop=True)
    n = len(df)
    n_train = int(n * train_frac)
    n_val   = int(n * val_frac)

    train = df.iloc[:n_train]
    val   = df.iloc[n_train:n_train + n_val]
    test  = df.iloc[n_train + n_val:]

    return train, val, test
