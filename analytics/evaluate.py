"""
Phase 3 – Model Evaluation Report (v2: imputer-aware, time-aware)

Loads the trained models and their fitted imputers, then evaluates them on
a fresh hold-out window from MongoDB.  Prints a comprehensive performance
report covering:

  • Congestion classifier — accuracy, per-class precision/recall/F1,
    confusion matrix, weighted F1, feature importances.
  • Speed predictor — MAE, RMSE, R2, residual analysis, bin-level accuracy.
  • Anomaly detector — anomaly rate, score distribution, separation stats.

The evaluation respects the same imputation pipeline used at training time,
so reported metrics are directly comparable to what the production
predictor will produce.

Usage
-----
    python -m analytics.evaluate              # last 7 days
    python -m analytics.evaluate --days 3
"""
from __future__ import annotations

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
    accuracy_score, classification_report, confusion_matrix, f1_score,
    mean_absolute_error, mean_squared_error, r2_score,
)

from analytics.feature_engineering import (
    CAUSAL_FEATURE_COLS, ANOMALY_FEATURE_COLS, CONGESTION_LABELS,
    coerce_types, build_X, encode_labels,
    FeatureImputer,
)

MONGO_URI     = os.getenv("MONGO_URI",  "mongodb://localhost:27017")
MONGO_DB      = os.getenv("MONGO_DB",   "nyc_traffic")
COL_PROCESSED = "traffic_processed"
MODELS_DIR    = Path(__file__).parent.parent / "models"


# -- Data fetch -----------------------------------------------------------------

def fetch_eval_data(days: int = 7) -> pd.DataFrame:
    """Fetch the most recent `days` days from MongoDB for evaluation."""
    since  = datetime.utcnow() - timedelta(days=days)
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


def _load_pair(name: str):
    model_path = MODELS_DIR / f"{name}.joblib"
    imp_path   = MODELS_DIR / f"{name}_imputer.joblib"
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path} — run `python -m analytics.train` first")
    model = joblib.load(model_path)
    imp   = FeatureImputer.load(imp_path) if imp_path.exists() else None
    return model, imp


def _separator(title: str = ""):
    width = 64
    if title:
        pad = (width - len(title) - 2) // 2
        print("=" * pad + f" {title} " + "=" * (width - pad - len(title) - 2))
    else:
        print("=" * width)


# -- Reporters ------------------------------------------------------------------

def report_classifier(clf, imp, X_raw, y_true_int):
    _separator("CONGESTION CLASSIFIER")
    X = imp.transform(X_raw) if imp else X_raw
    y_pred = clf.predict(X)
    acc = accuracy_score(y_true_int, y_pred)
    f1w = f1_score(y_true_int, y_pred, average="weighted", zero_division=0)

    print(f"\nAccuracy            : {acc:.4f}  ({acc*100:.2f}%)")
    print(f"Weighted F1         : {f1w:.4f}")
    print(f"Samples             : {len(y_true_int):,}\n")

    print("Per-Class Performance:")
    print(classification_report(
        y_true_int, y_pred,
        target_names=CONGESTION_LABELS, zero_division=0,
    ))

    print("Confusion Matrix (rows=actual, cols=predicted):")
    cm = confusion_matrix(y_true_int, y_pred)
    print("            " + "  ".join(f"{l:>10}" for l in CONGESTION_LABELS))
    for i, row in enumerate(cm):
        label = CONGESTION_LABELS[i] if i < len(CONGESTION_LABELS) else str(i)
        print(f"{label:>10}  " + "  ".join(f"{v:>10}" for v in row))

    print("\nTop Feature Importances:")
    imps = dict(zip(CAUSAL_FEATURE_COLS, clf.feature_importances_))
    for feat, val in sorted(imps.items(), key=lambda x: -x[1]):
        bar = "#" * int(val * 40)
        print(f"  {feat:<22} {val:.4f}  {bar}")

    return {"accuracy": round(acc, 4), "weighted_f1": round(f1w, 4)}


def report_regressor(reg, imp, X_raw, y_true):
    _separator("SPEED PREDICTOR")
    X = imp.transform(X_raw) if imp else X_raw
    y_pred = reg.predict(X)

    mae  = mean_absolute_error(y_true, y_pred)
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2   = r2_score(y_true, y_pred)
    residuals = y_true.values - y_pred

    print(f"\nMAE                 : {mae:.3f} mph")
    print(f"RMSE                : {rmse:.3f} mph")
    print(f"R2                  : {r2:.4f}")
    print(f"\nResidual diagnostics (actual - predicted):")
    print(f"  Mean              : {residuals.mean():+.3f}")
    print(f"  Std               : {residuals.std():.3f}")
    print(f"  Median            : {np.median(residuals):+.3f}")
    print(f"  p5  / p95         : {np.percentile(residuals,5):+.2f} / {np.percentile(residuals,95):+.2f}")
    print(f"  Min / Max         : {residuals.min():+.2f} / {residuals.max():+.2f}")

    # Bin-level accuracy: does the model put speed in the right congestion tier?
    bins   = [0, 5, 15, 35, 200]
    labels = ["SEVERE", "CONGESTED", "MODERATE", "FREE_FLOW"]
    actual_bin = pd.cut(y_true.values, bins=bins, labels=labels, right=False)
    pred_bin   = pd.cut(y_pred,        bins=bins, labels=labels, right=False)
    valid = ~pd.isnull(actual_bin) & ~pd.isnull(pred_bin)
    bin_acc = accuracy_score(actual_bin[valid], pred_bin[valid])
    print(f"\nTier-level accuracy : {bin_acc:.4f}  ({bin_acc*100:.2f}%)")
    print("(How often predicted speed falls in the correct congestion tier)")

    print("\nTop Feature Importances:")
    imps = dict(zip(CAUSAL_FEATURE_COLS, reg.feature_importances_))
    for feat, val in sorted(imps.items(), key=lambda x: -x[1]):
        bar = "#" * int(val * 40)
        print(f"  {feat:<22} {val:.4f}  {bar}")

    return {"mae": round(mae, 4), "rmse": round(rmse, 4),
            "r2":  round(r2,  4), "tier_accuracy": round(bin_acc, 4)}


def report_anomaly(iso, imp, X_raw):
    _separator("ANOMALY DETECTOR")
    X = imp.transform(X_raw) if imp else X_raw
    preds  = iso.predict(X)
    scores = iso.decision_function(X)

    rate = (preds == -1).mean()
    normal_scores  = scores[preds ==  1]
    anomaly_scores = scores[preds == -1]

    print(f"\nAnomaly rate        : {rate:.2%}  "
          f"({(preds==-1).sum():,} / {len(preds):,})")
    print(f"All scores  mean    : {scores.mean():+.4f}")
    print(f"All scores  std     : {scores.std():.4f}")
    print(f"Normal      mean    : {normal_scores.mean():+.4f}")
    print(f"Anomalous   mean    : {anomaly_scores.mean():+.4f}")
    sep = normal_scores.mean() - anomaly_scores.mean()
    print(f"Separation Delta        : {sep:+.4f}  (higher = better discrimination)")
    print(f"\nNote: in IsolationForest, more negative scores = more anomalous.")

    return {"anomaly_rate": round(float(rate), 4),
            "separation":   round(float(sep),  4)}


# -- Main -----------------------------------------------------------------------

def main(days: int = 7):
    logger.info("=======================================================")
    logger.info(" NYC Traffic – Phase 3 Model Evaluation (v2)         ")
    logger.info("=======================================================")

    clf, imp_clf = _load_pair("congestion_classifier")
    reg, imp_reg = _load_pair("speed_predictor")
    iso, imp_iso = _load_pair("anomaly_detector")

    # Print model versioning info
    for name in ["congestion_classifier", "speed_predictor", "anomaly_detector"]:
        meta_path = MODELS_DIR / f"{name}_metadata.json"
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
            logger.info(f"  {name}: code v{meta.get('code_version', '?')}, "
                        f"git {meta.get('git_commit', '?')[:8]}, "
                        f"trained {meta.get('trained_at', '?')[:19]}")

    # Fetch evaluation data
    df_raw = fetch_eval_data(days=days)
    if df_raw.empty:
        logger.error("No evaluation data available")
        return

    df = coerce_types(df_raw)
    df = df[df["borough_encoded"] >= 0]
    df = df[df["speed"].notna()]
    df = df[df["congestion_level"].notna()]
    logger.info(f"Evaluation records after cleaning: {len(df):,}")

    print()
    _separator()
    print(" NYC TRAFFIC PIPELINE — PHASE 3 EVALUATION REPORT (v2)")
    print(f" Evaluated at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Dataset      : {len(df):,} records (last {days} days)")
    print(f" Methodology  : leakage-free, fitted imputers, temporal-aware")
    _separator()
    print()

    # -- Congestion Classifier -------------------------------------
    X_cls = build_X(df, CAUSAL_FEATURE_COLS)
    y_cls = encode_labels(df["congestion_level"])
    valid = y_cls >= 0
    clf_metrics = report_classifier(clf, imp_clf, X_cls[valid], y_cls[valid])
    print()

    # -- Speed Predictor -------------------------------------------
    df_spd = df[df["speed"].notna()].copy()
    X_spd  = build_X(df_spd, CAUSAL_FEATURE_COLS)
    y_spd  = df_spd["speed"].astype(float)
    reg_metrics = report_regressor(reg, imp_reg, X_spd, y_spd)
    print()

    # -- Anomaly Detector ------------------------------------------
    X_anom = build_X(df, ANOMALY_FEATURE_COLS)
    iso_metrics = report_anomaly(iso, imp_iso, X_anom)

    # -- Summary ---------------------------------------------------
    print()
    _separator("SUMMARY")
    print()
    print(f"  Congestion accuracy   : {clf_metrics['accuracy']*100:.2f}%   (target >= 75%)")
    target_met = "PASS PASS" if clf_metrics["accuracy"] >= 0.75 else "FAIL FAIL"
    print(f"  Phase 3 target        : {target_met}")
    print(f"  Congestion weighted F1: {clf_metrics['weighted_f1']:.4f}")
    print(f"  Speed MAE             : {reg_metrics['mae']} mph")
    print(f"  Speed R2              : {reg_metrics['r2']}")
    print(f"  Speed tier accuracy   : {reg_metrics['tier_accuracy']*100:.2f}%")
    print(f"  Anomaly rate          : {iso_metrics['anomaly_rate']:.2%}")
    print(f"  Anomaly separation    : {iso_metrics['separation']:.4f}")
    _separator()
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Traffic Phase 3 – Model Evaluation (v2)")
    parser.add_argument("--days", type=int, default=7,
                        help="Days of recent data to evaluate on (default: 7)")
    args = parser.parse_args()
    main(days=args.days)
