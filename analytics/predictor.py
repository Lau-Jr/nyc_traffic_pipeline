"""
Phase 3 – Real-Time Traffic Predictor

Loads the three trained models and exposes a simple API:

    predictor = TrafficPredictor()
    result = predictor.predict(record_dict)
    # result = {
    #   "predicted_congestion": "MODERATE",
    #   "predicted_speed":      28.4,
    #   "is_anomaly":           False,
    #   "anomaly_score":        0.12,
    # }

Can be used from:
  - The batch processor (score historical records)
  - A REST API endpoint (Phase 4)
  - The Streamlit dashboard (Phase 4)
"""
import os
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from loguru import logger

from analytics.feature_engineering import (
    FEATURE_COLS, SPEED_FEATURE_COLS,
    build_X, decode_labels, record_to_features,
)

MODELS_DIR = Path(__file__).parent.parent / "models"

# Anomaly detector uses extended features
ANOMALY_FEATURE_COLS = FEATURE_COLS + ["congestion_score"]


class TrafficPredictor:
    """
    Wraps the three trained models (congestion classifier, speed predictor,
    anomaly detector) and provides record-level and batch prediction.

    Models are loaded lazily on first use so import is cheap.
    """

    def __init__(self, models_dir: Optional[Path] = None):
        self._dir = Path(models_dir) if models_dir else MODELS_DIR
        self._clf = None   # congestion classifier
        self._reg = None   # speed predictor
        self._iso = None   # anomaly detector
        self._loaded = False

    # ── Loading ────────────────────────────────────────────────────────────────

    def load(self):
        """Explicitly load all models from disk."""
        self._clf = self._load_model("congestion_classifier")
        self._reg = self._load_model("speed_predictor")
        self._iso = self._load_model("anomaly_detector")
        self._loaded = True
        logger.info("TrafficPredictor: all models loaded")

    def _load_model(self, name: str):
        path = self._dir / f"{name}.joblib"
        if not path.exists():
            raise FileNotFoundError(
                f"Model not found: {path}\n"
                "Run `python -m analytics.train` first."
            )
        model = joblib.load(path)
        logger.debug(f"Loaded {name} from {path}")
        return model

    def _ensure_loaded(self):
        if not self._loaded:
            self.load()

    # ── Single-record prediction ───────────────────────────────────────────────

    def predict(self, record: dict) -> dict:
        """
        Score a single record dict and return a prediction dict.

        Input keys used (all optional — missing values are imputed):
            hour_of_day, day_of_week, is_weekend, is_peak_hour,
            borough, latitude, longitude, travel_time, speed, congestion_score

        Returns:
            predicted_congestion : str  – FREE_FLOW / MODERATE / CONGESTED / SEVERE
            congestion_confidence: float – probability of predicted class (0–1)
            predicted_speed      : float – speed in mph
            is_anomaly           : bool
            anomaly_score        : float – higher = more normal (IsolationForest convention)
        """
        self._ensure_loaded()
        df = record_to_features(record)

        # ── Congestion classification ──────────────────────────────
        X_cls = build_X(df, FEATURE_COLS)
        cls_label_int = self._clf.predict(X_cls)[0]
        cls_label     = decode_labels(pd.Series([cls_label_int])).iloc[0]
        cls_proba     = self._clf.predict_proba(X_cls)[0].max()

        # ── Speed prediction ───────────────────────────────────────
        # Use actual travel_time if available; 0 otherwise
        X_spd = build_X(df, SPEED_FEATURE_COLS)
        pred_speed = float(self._reg.predict(X_spd)[0])
        pred_speed = max(0.0, round(pred_speed, 2))

        # ── Anomaly detection ──────────────────────────────────────
        # Safely compute congestion_score — avoid KeyError if column missing
        has_score = (
            "congestion_score" in df.columns
            and not pd.isna(df["congestion_score"].iloc[0])
            and df["congestion_score"].iloc[0] != 0
        )
        if not has_score:
            speed_val  = float(record.get("speed") or pred_speed or 0.0)
            cong_score = round(1.0 - min(speed_val / 60.0, 1.0), 4)
            df["congestion_score"] = cong_score

        X_anom      = build_X(df, ANOMALY_FEATURE_COLS)
        anom_pred   = self._iso.predict(X_anom)[0]          # 1=normal, -1=anomaly
        anom_score  = float(self._iso.decision_function(X_anom)[0])
        is_anomaly  = bool(anom_pred == -1)

        return {
            "predicted_congestion":  cls_label,
            "congestion_confidence": round(float(cls_proba), 4),
            "predicted_speed":       pred_speed,
            "is_anomaly":            is_anomaly,
            "anomaly_score":         round(anom_score, 4),
        }

    # ── Batch prediction ───────────────────────────────────────────────────────

    def predict_batch(self, records: list[dict]) -> list[dict]:
        """
        Score a list of records.  Returns a list of prediction dicts in the
        same order as the input.
        """
        self._ensure_loaded()
        results = []
        for rec in records:
            try:
                results.append(self.predict(rec))
            except Exception as exc:
                logger.warning(f"Prediction failed for record: {exc}")
                results.append({
                    "predicted_congestion":  "UNKNOWN",
                    "congestion_confidence": 0.0,
                    "predicted_speed":       None,
                    "is_anomaly":            None,
                    "anomaly_score":         None,
                })
        return results

    def predict_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Score a full pandas DataFrame (as returned by MongoDB fetch).
        Adds prediction columns in-place and returns the augmented DataFrame.
        """
        self._ensure_loaded()
        records  = df.to_dict(orient="records")
        preds    = self.predict_batch(records)
        pred_df  = pd.DataFrame(preds)
        return pd.concat([df.reset_index(drop=True), pred_df], axis=1)

    # ── Model info ─────────────────────────────────────────────────────────────

    def is_ready(self) -> bool:
        """Return True if all models are loaded."""
        return self._loaded

    def models_exist(self) -> bool:
        """Return True if all model files are present on disk."""
        names = ["congestion_classifier", "speed_predictor", "anomaly_detector"]
        return all((self._dir / f"{n}.joblib").exists() for n in names)


# ── Convenience singleton ──────────────────────────────────────────────────────
# Import and call predict() anywhere without managing the object lifecycle.

_predictor: Optional[TrafficPredictor] = None


def get_predictor() -> TrafficPredictor:
    """Return a module-level singleton TrafficPredictor (lazy-loaded)."""
    global _predictor
    if _predictor is None:
        _predictor = TrafficPredictor()
    return _predictor
