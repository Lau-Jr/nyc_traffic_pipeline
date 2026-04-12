"""
Phase 3 – Model Training Pipeline

Reads processed traffic records from MongoDB, trains three models:
  1. Congestion Classifier  – RandomForestClassifier  (target: congestion_level)
  2. Speed Predictor        – GradientBoostingRegressor (target: speed)
  3. Anomaly Detector       – IsolationForest          (unsupervised)

Saves all models + metadata to models/ using joblib.

Usage:
    python -m analytics.train                  # train on last 30 days
    python -m analytics.train --days 7         # shorter window
    python -m analytics.train --min-records 500
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
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor, IsolationForest
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    accuracy_score, classification_report,
    mean_absolute_error, mean_squared_error, r2_score,
)

from analytics.feature_engineering import (
    FEATURE_COLS, SPEED_FEATURE_COLS,
    prepare_features, build_X, encode_labels, CONGESTION_LABELS,
)

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI    = os.getenv("MONGO_URI",  "mongodb://localhost:27017")
MONGO_DB     = os.getenv("MONGO_DB",   "nyc_traffic")
COL_PROCESSED = "traffic_processed"
MODELS_DIR   = Path(__file__).parent.parent / "models"

MIN_RECORDS_DEFAULT = 1_000
LOOKBACK_DAYS_DEFAULT = 30

# ── Data Fetching ──────────────────────────────────────────────────────────────

def fetch_training_data(days: int = LOOKBACK_DAYS_DEFAULT) -> pd.DataFrame:
    """Pull processed records from MongoDB for the last `days` days."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
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
        docs = list(
            client[MONGO_DB][COL_PROCESSED].find(
                {"processed_at": {"$gte": since}},
                projection,
            )
        )
        logger.info(f"Fetched {len(docs):,} records from MongoDB (last {days} days)")
        return pd.DataFrame(docs) if docs else pd.DataFrame()
    finally:
        client.close()


# ── Training Functions ─────────────────────────────────────────────────────────

def train_congestion_classifier(X_train, y_train, X_test, y_test):
    """
    Train a RandomForest to classify congestion level.
    Returns (model, metrics_dict).
    """
    logger.info("Training congestion classifier (RandomForest)…")
    clf = RandomForestClassifier(
        n_estimators=200,
        max_depth=12,
        min_samples_leaf=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    acc    = accuracy_score(y_test, y_pred)
    report = classification_report(
        y_test, y_pred,
        target_names=CONGESTION_LABELS,
        output_dict=True,
        zero_division=0,
    )

    logger.info(f"Congestion classifier accuracy: {acc:.4f} ({acc*100:.2f}%)")
    metrics = {
        "accuracy":          round(acc, 4),
        "accuracy_pct":      round(acc * 100, 2),
        "classification_report": report,
    }
    return clf, metrics


def train_speed_predictor(X_train, y_train, X_test, y_test):
    """
    Train a GradientBoosting regressor to predict speed (mph).
    Returns (model, metrics_dict).
    """
    logger.info("Training speed predictor (GradientBoosting)…")
    reg = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        random_state=42,
    )
    reg.fit(X_train, y_train)

    y_pred = reg.predict(X_test)
    mae    = mean_absolute_error(y_test, y_pred)
    rmse   = np.sqrt(mean_squared_error(y_test, y_pred))
    r2     = r2_score(y_test, y_pred)

    logger.info(f"Speed predictor — MAE: {mae:.3f} mph  RMSE: {rmse:.3f}  R²: {r2:.4f}")
    metrics = {
        "mae":  round(mae,  4),
        "rmse": round(rmse, 4),
        "r2":   round(r2,   4),
    }
    return reg, metrics


def train_anomaly_detector(X_train):
    """
    Train an IsolationForest anomaly detector (unsupervised).
    Returns (model, metadata_dict).
    """
    logger.info("Training anomaly detector (IsolationForest)…")
    iso = IsolationForest(
        n_estimators=150,
        contamination=0.05,   # expect ~5% anomalous records
        random_state=42,
        n_jobs=-1,
    )
    iso.fit(X_train)

    scores = iso.decision_function(X_train)
    anomaly_rate = (iso.predict(X_train) == -1).mean()
    logger.info(f"Anomaly detector — training anomaly rate: {anomaly_rate:.2%}")

    metadata = {
        "contamination":        0.05,
        "train_anomaly_rate":   round(float(anomaly_rate), 4),
        "score_mean":           round(float(scores.mean()), 4),
        "score_std":            round(float(scores.std()),  4),
    }
    return iso, metadata


# ── Persistence ────────────────────────────────────────────────────────────────

def save_model(model, name: str, metadata: dict):
    """Save a model and its metadata JSON to models/."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODELS_DIR / f"{name}.joblib"
    meta_path  = MODELS_DIR / f"{name}_metadata.json"

    joblib.dump(model, model_path)
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    logger.info(f"Saved model → {model_path}")
    logger.info(f"Saved metadata → {meta_path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main(days: int = LOOKBACK_DAYS_DEFAULT, min_records: int = MIN_RECORDS_DEFAULT):
    logger.info("═══════════════════════════════════════════════════")
    logger.info(" NYC Traffic – Phase 3 Model Training             ")
    logger.info("═══════════════════════════════════════════════════")

    # ── Fetch data ────────────────────────────────────────────────
    df_raw = fetch_training_data(days=days)
    if df_raw.empty or len(df_raw) < min_records:
        logger.error(
            f"Not enough data: {len(df_raw):,} records "
            f"(need ≥ {min_records:,}). Run the stream processor first."
        )
        return

    logger.info(f"Dataset size: {len(df_raw):,} records")

    # ── Prepare features ──────────────────────────────────────────
    df = prepare_features(df_raw)

    # Drop rows with unknown borough or null speed
    df = df[df["borough_encoded"] >= 0]
    df = df[df["speed"].notna()]
    df["speed"] = pd.to_numeric(df["speed"], errors="coerce")
    df = df[df["speed"].notna()]

    logger.info(f"After cleaning: {len(df):,} records")

    # ── Congestion Classifier ─────────────────────────────────────
    X_cls = build_X(df, FEATURE_COLS)
    y_cls = encode_labels(df["congestion_level"])

    # Drop rows where label encoding failed (-1)
    valid_mask = y_cls >= 0
    X_cls, y_cls = X_cls[valid_mask], y_cls[valid_mask]

    X_train_c, X_test_c, y_train_c, y_test_c = train_test_split(
        X_cls, y_cls, test_size=0.2, random_state=42, stratify=y_cls
    )
    clf, clf_metrics = train_congestion_classifier(
        X_train_c, y_train_c, X_test_c, y_test_c
    )
    save_model(clf, "congestion_classifier", {
        "model":        "RandomForestClassifier",
        "features":     FEATURE_COLS,
        "target":       "congestion_level",
        "classes":      ["FREE_FLOW", "MODERATE", "CONGESTED", "SEVERE"],
        "train_size":   len(X_train_c),
        "test_size":    len(X_test_c),
        "trained_at":   datetime.now(timezone.utc).isoformat(),
        **clf_metrics,
    })

    # ── Speed Predictor ───────────────────────────────────────────
    df_speed = df[df["travel_time"].notna() & (df["travel_time"] > 0)].copy()
    X_spd  = build_X(df_speed, SPEED_FEATURE_COLS)
    y_spd  = df_speed["speed"].astype(float)

    X_train_s, X_test_s, y_train_s, y_test_s = train_test_split(
        X_spd, y_spd, test_size=0.2, random_state=42
    )
    reg, reg_metrics = train_speed_predictor(
        X_train_s, y_train_s, X_test_s, y_test_s
    )
    save_model(reg, "speed_predictor", {
        "model":      "GradientBoostingRegressor",
        "features":   SPEED_FEATURE_COLS,
        "target":     "speed",
        "train_size": len(X_train_s),
        "test_size":  len(X_test_s),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        **reg_metrics,
    })

    # ── Anomaly Detector ──────────────────────────────────────────
    # Uses speed + congestion_score + time features
    anomaly_features = FEATURE_COLS + ["congestion_score"]
    df_anom = df[df["congestion_score"].notna()].copy()
    df_anom["congestion_score"] = pd.to_numeric(df_anom["congestion_score"], errors="coerce")
    df_anom = df_anom[df_anom["congestion_score"].notna()]
    X_anom = build_X(df_anom, anomaly_features)

    iso, iso_meta = train_anomaly_detector(X_anom)
    save_model(iso, "anomaly_detector", {
        "model":    "IsolationForest",
        "features": anomaly_features,
        "target":   "anomaly_score",
        "train_size": len(X_anom),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        **iso_meta,
    })

    # ── Summary ───────────────────────────────────────────────────
    logger.info("═══════════════════════════════════════════════════")
    logger.info(" Training complete. Results:                       ")
    logger.info(f"  Congestion accuracy : {clf_metrics['accuracy_pct']}%")
    logger.info(f"  Speed MAE           : {reg_metrics['mae']} mph")
    logger.info(f"  Speed R²            : {reg_metrics['r2']}")
    logger.info(f"  Anomaly train rate  : {iso_meta['train_anomaly_rate']:.2%}")
    logger.info(f"  Models saved to     : {MODELS_DIR}")
    logger.info("═══════════════════════════════════════════════════")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Traffic Phase 3 – Model Training")
    parser.add_argument("--days",        type=int, default=LOOKBACK_DAYS_DEFAULT,
                        help="Days of history to use for training")
    parser.add_argument("--min-records", type=int, default=MIN_RECORDS_DEFAULT,
                        help="Minimum records required before training")
    args = parser.parse_args()
    main(days=args.days, min_records=args.min_records)
