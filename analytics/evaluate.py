"""
Phase 3 – Model Evaluation & Performance Report

Loads the trained models, fetches a fresh hold-out sample from MongoDB,
and prints a full performance report covering:
  • Congestion classifier: accuracy, per-class precision/recall/F1,
    confusion matrix, feature importances
  • Speed predictor: MAE, RMSE, R², residual analysis
  • Anomaly detector: anomaly rate, score distribution

Usage:
    python -m analytics.evaluate              # full report
    python -m analytics.evaluate --days 3    # evaluate on last 3 days of data
"""
import argparse
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from loguru import logger
from pymongo import MongoClient
from sklearn.metrics import (
    accuracy_score, classification_report, confusion_matrix,
    mean_absolute_error, mean_squared_error, r2_score,
)

from analytics.feature_engineering import (
    FEATURE_COLS, SPEED_FEATURE_COLS, CONGESTION_LABELS,
    prepare_features, build_X, encode_labels, decode_labels,
)

MONGO_URI     = os.getenv("MONGO_URI",  "mongodb://localhost:27017")
MONGO_DB      = os.getenv("MONGO_DB",   "nyc_traffic")
COL_PROCESSED = "traffic_processed"
MODELS_DIR    = Path(__file__).parent.parent / "models"
ANOMALY_FEATURE_COLS = FEATURE_COLS + ["congestion_score"]


# ── Data Fetch ─────────────────────────────────────────────────────────────────

def fetch_eval_data(days: int = 7) -> pd.DataFrame:
    """Fetch the most recent `days` days from MongoDB for evaluation."""
    since  = datetime.now(timezone.utc) - timedelta(days=days)
    client = MongoClient(MONGO_URI)
    try:
        projection = {
            "_id": 0,
            "borough": 1, "speed": 1, "travel_time": 1,
            "congestion_level": 1, "congestion_score": 1,
            "hour_of_day": 1, "day_of_week": 1,
            "is_peak_hour": 1, "is_weekend": 1,
            "latitude": 1, "longitude": 1,
        }
        docs = list(client[MONGO_DB][COL_PROCESSED].find(
            {"processed_at": {"$gte": since}}, projection
        ))
        logger.info(f"Evaluation dataset: {len(docs):,} records (last {days} days)")
        return pd.DataFrame(docs) if docs else pd.DataFrame()
    finally:
        client.close()


# ── Report Sections ────────────────────────────────────────────────────────────

def _separator(title: str = ""):
    width = 60
    if title:
        pad = (width - len(title) - 2) // 2
        print("=" * pad + f" {title} " + "=" * (width - pad - len(title) - 2))
    else:
        print("=" * width)


def report_classifier(clf, X, y_true_int):
    _separator("CONGESTION CLASSIFIER")
    y_pred = clf.predict(X)
    acc    = accuracy_score(y_true_int, y_pred)

    print(f"\nAccuracy : {acc:.4f}  ({acc*100:.2f}%)")
    print(f"Samples  : {len(y_true_int):,}\n")

    print("Classification Report:")
    print(classification_report(
        y_true_int, y_pred,
        target_names=CONGESTION_LABELS,
        zero_division=0,
    ))

    print("Confusion Matrix (rows=actual, cols=predicted):")
    cm = confusion_matrix(y_true_int, y_pred)
    header = "         " + "  ".join(f"{l:>10}" for l in CONGESTION_LABELS)
    print(header)
    for i, row in enumerate(cm):
        label = CONGESTION_LABELS[i] if i < len(CONGESTION_LABELS) else str(i)
        print(f"{label:>10}  " + "  ".join(f"{v:>10}" for v in row))

    print("\nTop 10 Feature Importances:")
    importances = dict(zip(FEATURE_COLS, clf.feature_importances_))
    for feat, imp in sorted(importances.items(), key=lambda x: -x[1])[:10]:
        bar = "█" * int(imp * 40)
        print(f"  {feat:<20} {imp:.4f}  {bar}")

    return {"accuracy": round(acc, 4), "accuracy_pct": round(acc * 100, 2)}


def report_regressor(reg, X, y_true):
    _separator("SPEED PREDICTOR")
    y_pred = reg.predict(X)
    mae    = mean_absolute_error(y_true, y_pred)
    rmse   = np.sqrt(mean_squared_error(y_true, y_pred))
    r2     = r2_score(y_true, y_pred)

    residuals = y_true.values - y_pred
    print(f"\nMAE  : {mae:.3f} mph")
    print(f"RMSE : {rmse:.3f} mph")
    print(f"R²   : {r2:.4f}")
    print(f"\nResiduals (actual - predicted):")
    print(f"  Mean   : {residuals.mean():.3f}")
    print(f"  Std    : {residuals.std():.3f}")
    print(f"  p5/p95 : {np.percentile(residuals,5):.2f} / {np.percentile(residuals,95):.2f}")

    # Speed bins accuracy
    bins   = [0, 5, 15, 35, 200]
    labels = ["SEVERE", "CONGESTED", "MODERATE", "FREE_FLOW"]
    actual_bin = pd.cut(y_true.values,  bins=bins, labels=labels, right=False)
    pred_bin   = pd.cut(y_pred,         bins=bins, labels=labels, right=False)
    valid_mask = ~pd.isnull(actual_bin) & ~pd.isnull(pred_bin)
    bin_acc    = accuracy_score(actual_bin[valid_mask], pred_bin[valid_mask])
    print(f"\nBin-level accuracy (speed tier): {bin_acc:.4f}  ({bin_acc*100:.2f}%)")

    print("\nTop 10 Feature Importances:")
    importances = dict(zip(SPEED_FEATURE_COLS, reg.feature_importances_))
    for feat, imp in sorted(importances.items(), key=lambda x: -x[1])[:10]:
        bar = "█" * int(imp * 40)
        print(f"  {feat:<20} {imp:.4f}  {bar}")

    return {"mae": round(mae, 4), "rmse": round(rmse, 4), "r2": round(r2, 4)}


def report_anomaly(iso, X):
    _separator("ANOMALY DETECTOR")
    preds  = iso.predict(X)
    scores = iso.decision_function(X)

    anomaly_rate = (preds == -1).mean()
    print(f"\nAnomaly rate   : {anomaly_rate:.2%}  ({(preds==-1).sum():,} / {len(preds):,})")
    print(f"Score mean     : {scores.mean():.4f}")
    print(f"Score std      : {scores.std():.4f}")
    print(f"Score p5/p95   : {np.percentile(scores,5):.4f} / {np.percentile(scores,95):.4f}")
    print(f"\nNote: negative scores = more anomalous, positive = more normal")

    return {"anomaly_rate": round(float(anomaly_rate), 4)}


# ── Main ───────────────────────────────────────────────────────────────────────

def main(days: int = 7):
    logger.info("═══════════════════════════════════════════════════")
    logger.info(" NYC Traffic – Phase 3 Model Evaluation           ")
    logger.info("═══════════════════════════════════════════════════")

    # Load models
    def _load(name):
        p = MODELS_DIR / f"{name}.joblib"
        if not p.exists():
            raise FileNotFoundError(f"Model not found: {p} — run `python -m analytics.train` first")
        return joblib.load(p)

    clf = _load("congestion_classifier")
    reg = _load("speed_predictor")
    iso = _load("anomaly_detector")

    # Load metadata
    results = {}
    for name in ("congestion_classifier", "speed_predictor", "anomaly_detector"):
        meta_path = MODELS_DIR / f"{name}_metadata.json"
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
            trained_at = meta.get("trained_at", "unknown")
            logger.info(f"  {name}: trained at {trained_at}")

    # Fetch eval data
    df_raw = fetch_eval_data(days=days)
    if df_raw.empty:
        logger.error("No evaluation data available")
        return

    df = prepare_features(df_raw)
    df = df[df["borough_encoded"] >= 0]
    df = df[df["speed"].notna()]
    df["speed"] = pd.to_numeric(df["speed"], errors="coerce")
    df = df[df["speed"].notna()]
    logger.info(f"Evaluation records after cleaning: {len(df):,}")

    print()
    _separator()
    print(" NYC TRAFFIC PIPELINE — PHASE 3 EVALUATION REPORT")
    print(f" Evaluated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Dataset     : {len(df):,} records (last {days} days)")
    _separator()
    print()

    # ── Congestion Classifier ──────────────────────────────────────
    X_cls = build_X(df, FEATURE_COLS)
    y_cls = encode_labels(df["congestion_level"])
    valid = y_cls >= 0
    clf_metrics = report_classifier(clf, X_cls[valid], y_cls[valid])
    results["congestion_classifier"] = clf_metrics

    print()

    # ── Speed Predictor ────────────────────────────────────────────
    df_spd = df[df["travel_time"].notna() & (df["travel_time"] > 0)].copy()
    X_spd  = build_X(df_spd, SPEED_FEATURE_COLS)
    y_spd  = df_spd["speed"].astype(float)
    reg_metrics = report_regressor(reg, X_spd, y_spd)
    results["speed_predictor"] = reg_metrics

    print()

    # ── Anomaly Detector ───────────────────────────────────────────
    df_anom = df[df["congestion_score"].notna()].copy()
    df_anom["congestion_score"] = pd.to_numeric(df_anom["congestion_score"], errors="coerce")
    df_anom = df_anom[df_anom["congestion_score"].notna()]
    X_anom  = build_X(df_anom, ANOMALY_FEATURE_COLS)
    iso_metrics = report_anomaly(iso, X_anom)
    results["anomaly_detector"] = iso_metrics

    print()
    _separator("SUMMARY")
    print(f"\n  Congestion accuracy : {clf_metrics['accuracy_pct']}%  (target ≥ 75%)")
    target_met = "PASS" if clf_metrics["accuracy_pct"] >= 75 else "FAIL"
    print(f"  Phase 3 target      : {target_met}")
    print(f"  Speed MAE           : {reg_metrics['mae']} mph")
    print(f"  Speed R²            : {reg_metrics['r2']}")
    print(f"  Anomaly rate        : {iso_metrics['anomaly_rate']:.2%}")
    _separator()
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Traffic Phase 3 – Model Evaluation")
    parser.add_argument("--days", type=int, default=7,
                        help="Days of recent data to evaluate on (default: 7)")
    args = parser.parse_args()
    main(days=args.days)
