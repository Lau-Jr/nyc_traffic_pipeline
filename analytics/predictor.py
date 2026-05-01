"""
Phase 3 – Real-Time Traffic Predictor (v2: imputer-aware)

Loads the three trained models AND their fitted imputers, then exposes a
simple prediction API:

    predictor = TrafficPredictor()
    result = predictor.predict(record_dict)
    # {
    #   "predicted_congestion":  "MODERATE",
    #   "congestion_confidence": 0.87,
    #   "predicted_speed":       28.4,
    #   "is_anomaly":            False,
    #   "anomaly_score":         0.12,
    # }

Each model has a paired FeatureImputer that was fit on training data only.
The same imputer is applied at inference time so live records are
preprocessed identically to training, with no leakage of post-train
statistics.

Used by:
  - The Streamlit dashboard (Phase 4)
  - The optional REST API endpoint (bonus)
  - Any downstream batch scoring job
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from loguru import logger

from analytics.feature_engineering import (
    CAUSAL_FEATURE_COLS, ANOMALY_FEATURE_COLS,
    build_X, decode_labels, record_to_features,
    FeatureImputer,
)

MODELS_DIR = Path(__file__).parent.parent / "models"

MODEL_NAMES = ["congestion_classifier", "speed_predictor", "anomaly_detector"]


class TrafficPredictor:
    """
    Wraps the three trained models + their FeatureImputers and provides
    record-level and batch prediction.  Models are loaded lazily so import
    is cheap.
    """

    def __init__(self, models_dir: Optional[Path] = None):
        self._dir = Path(models_dir) if models_dir else MODELS_DIR
        self._clf = self._reg = self._iso = None
        self._imp_clf = self._imp_reg = self._imp_iso = None
        self._loaded = False

    # ── Loading ────────────────────────────────────────────────────────────────

    def load(self):
        """Explicitly load all models + imputers from disk."""
        self._clf, self._imp_clf = self._load_pair("congestion_classifier")
        self._reg, self._imp_reg = self._load_pair("speed_predictor")
        self._iso, self._imp_iso = self._load_pair("anomaly_detector")
        self._loaded = True
        logger.info("TrafficPredictor: all models + imputers loaded")

    def _load_pair(self, name: str):
        model_path   = self._dir / f"{name}.joblib"
        imputer_path = self._dir / f"{name}_imputer.joblib"

        if not model_path.exists():
            raise FileNotFoundError(
                f"Model not found: {model_path}\n"
                "Run `python -m analytics.train` first."
            )
        model = joblib.load(model_path)

        imputer = None
        if imputer_path.exists():
            imputer = FeatureImputer.load(imputer_path)
        else:
            logger.warning(f"No imputer found for {name} — predictions may "
                           "differ from training preprocessing")
        return model, imputer

    def _ensure_loaded(self):
        if not self._loaded:
            self.load()

    # ── Single-record prediction ───────────────────────────────────────────────

    def predict(self, record: dict) -> dict:
        """
        Score a single record dict.

        Returns:
            predicted_congestion : str  – FREE_FLOW / MODERATE / CONGESTED / SEVERE
            congestion_confidence: float – probability of predicted class (0–1)
            predicted_speed      : float – speed in mph
            is_anomaly           : bool
            anomaly_score        : float – higher = more normal (IsolationForest)
        """
        self._ensure_loaded()
        df = record_to_features(record)

        # ── Congestion classification ─────────────────────────────────
        X_cls = build_X(df, CAUSAL_FEATURE_COLS)
        if self._imp_clf:
            X_cls = self._imp_clf.transform(X_cls)
        cls_label_int = self._clf.predict(X_cls)[0]
        cls_label     = decode_labels(pd.Series([cls_label_int])).iloc[0]
        cls_proba     = float(self._clf.predict_proba(X_cls)[0].max())

        # ── Speed prediction ──────────────────────────────────────────
        X_spd = build_X(df, CAUSAL_FEATURE_COLS)
        if self._imp_reg:
            X_spd = self._imp_reg.transform(X_spd)
        pred_speed = float(self._reg.predict(X_spd)[0])
        pred_speed = max(0.0, round(pred_speed, 2))

        # ── Anomaly detection ─────────────────────────────────────────
        X_anom = build_X(df, ANOMALY_FEATURE_COLS)
        if self._imp_iso:
            X_anom = self._imp_iso.transform(X_anom)
        anom_label = int(self._iso.predict(X_anom)[0])           # 1=normal, -1=anomaly
        anom_score = float(self._iso.decision_function(X_anom)[0])
        is_anomaly = bool(anom_label == -1)

        return {
            "predicted_congestion":  cls_label,
            "congestion_confidence": round(cls_proba, 4),
            "predicted_speed":       pred_speed,
            "is_anomaly":            is_anomaly,
            "anomaly_score":         round(anom_score, 4),
        }

    # ── Batch prediction ───────────────────────────────────────────────────────

    def predict_batch(self, records: list[dict]) -> list[dict]:
        """Score a list of records.  Failures are logged and replaced with UNKNOWN."""
        self._ensure_loaded()
        results = []
        for rec in records:
            try:
                results.append(self.predict(rec))
            except Exception as exc:
                logger.warning(f"Prediction failed: {exc}")
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
        Score a full pandas DataFrame.  Returns a new DataFrame with
        prediction columns appended.
        """
        self._ensure_loaded()
        records  = df.to_dict(orient="records")
        preds    = self.predict_batch(records)
        pred_df  = pd.DataFrame(preds)
        return pd.concat([df.reset_index(drop=True), pred_df], axis=1)

    # ── Status ─────────────────────────────────────────────────────────────────

    def is_ready(self) -> bool:
        return self._loaded

    def models_exist(self) -> bool:
        return all((self._dir / f"{n}.joblib").exists() for n in MODEL_NAMES)


# ── Convenience singleton ──────────────────────────────────────────────────────

_predictor: Optional[TrafficPredictor] = None


def get_predictor() -> TrafficPredictor:
    """Return a module-level singleton TrafficPredictor (lazy-loaded)."""
    global _predictor
    if _predictor is None:
        _predictor = TrafficPredictor()
    return _predictor
