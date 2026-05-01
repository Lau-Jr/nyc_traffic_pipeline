"""
Phase 3 – Model Training Pipeline (v2: leakage-free, time-series-validated)

Methodology
───────────
1.  TEMPORAL SPLIT — Records sorted by processed_at, then split 70/15/15
    (train / validation / test).  Random shuffling on time-series data
    leaks future information into the past and was removed.

2.  IMPUTATION FIT ON TRAIN ONLY — Median imputation parameters are
    estimated on the training fold and applied unchanged to validation
    and test folds.  Prevents median-leakage.

3.  TIME-SERIES CROSS-VALIDATION — TimeSeriesSplit(n_splits=5) used for
    hyperparameter tuning instead of plain KFold.  Each fold's training
    set strictly precedes its validation set.

4.  HYPERPARAMETER TUNING — GridSearchCV over a small but principled grid.
    Best parameters logged to metadata.

5.  MODEL VERSIONING — Each model's metadata records git commit hash,
    sklearn version, training dataset signature (record count + date
    range), feature columns, hyperparameters, and full evaluation
    metrics so any past run can be reproduced or audited.

Models
──────
  • Congestion Classifier  – RandomForest, target = congestion_level
  • Speed Predictor        – GradientBoosting, target = speed
  • Anomaly Detector       – IsolationForest, unsupervised

Usage
─────
    python -m analytics.train                    # 30-day window
    python -m analytics.train --days 7
    python -m analytics.train --no-tune          # skip GridSearchCV
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from loguru import logger
from pymongo import MongoClient
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor, IsolationForest
from sklearn.metrics import (
    accuracy_score, classification_report, f1_score,
    mean_absolute_error, mean_squared_error, r2_score,
)
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit

from analytics.feature_engineering import (
    CAUSAL_FEATURE_COLS, ANOMALY_FEATURE_COLS, CONGESTION_LABELS,
    coerce_types, build_X, encode_labels,
    FeatureImputer, temporal_split,
)

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI     = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB      = os.getenv("MONGO_DB",  "nyc_traffic")
COL_PROCESSED = "traffic_processed"
MODELS_DIR    = Path(__file__).parent.parent / "models"

MIN_RECORDS_DEFAULT  = 1_000
LOOKBACK_DAYS_DEFAULT = 30
CODE_VERSION = "3.0.0"  # bump when training methodology changes


# ── Versioning helpers ─────────────────────────────────────────────────────────

def _git_commit_hash() -> str:
    """Return the current git HEAD commit hash, or 'unknown'."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).parent.parent,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


def _data_signature(df: pd.DataFrame) -> dict:
    """Compute a deterministic signature of the training dataset."""
    h = hashlib.sha256()
    h.update(str(len(df)).encode())
    if "processed_at" in df.columns and not df["processed_at"].empty:
        h.update(str(df["processed_at"].min()).encode())
        h.update(str(df["processed_at"].max()).encode())
    return {
        "record_count": int(len(df)),
        "min_processed_at": str(df["processed_at"].min()) if "processed_at" in df.columns else None,
        "max_processed_at": str(df["processed_at"].max()) if "processed_at" in df.columns else None,
        "sha256_prefix": h.hexdigest()[:16],
    }


def _versioning_metadata() -> dict:
    return {
        "code_version":    CODE_VERSION,
        "git_commit":      _git_commit_hash(),
        "python_version":  platform.python_version(),
        "sklearn_version": sklearn.__version__,
        "trained_at":      datetime.now(timezone.utc).isoformat(),
        "platform":        platform.system(),
    }


# ── Data fetching ──────────────────────────────────────────────────────────────

def fetch_training_data(days: int = LOOKBACK_DAYS_DEFAULT) -> pd.DataFrame:
    """Pull processed records from MongoDB for the last `days` days."""
    since = datetime.utcnow() - timedelta(days=days)
    client = MongoClient(MONGO_URI)
    try:
        projection = {
            "_id": 0,
            "borough": 1, "speed": 1, "travel_time": 1,
            "congestion_level": 1, "congestion_score": 1,
            "hour_of_day": 1, "day_of_week": 1,
            "is_peak_hour": 1, "is_weekend": 1,
            "latitude": 1, "longitude": 1,
            "processed_at": 1,
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


# ── Training routines ──────────────────────────────────────────────────────────

def train_congestion_classifier(
    X_train, y_train, X_val, y_val, X_test, y_test, tune: bool = True,
):
    """RandomForest with optional GridSearchCV over a principled grid."""
    logger.info("Training congestion classifier (RandomForest)…")

    base = RandomForestClassifier(
        class_weight="balanced", random_state=42, n_jobs=-1,
    )

    if tune:
        param_grid = {
            "n_estimators": [100, 200],
            "max_depth":    [10, 15, None],
            "min_samples_leaf": [3, 5],
        }
        cv = TimeSeriesSplit(n_splits=5)
        gs = GridSearchCV(
            base, param_grid, cv=cv, scoring="f1_weighted",
            n_jobs=-1, verbose=0, refit=True,
        )
        gs.fit(X_train, y_train)
        clf = gs.best_estimator_
        best_params = gs.best_params_
        cv_score    = float(gs.best_score_)
        logger.info(f"Best params: {best_params}  CV F1={cv_score:.4f}")
    else:
        clf = RandomForestClassifier(
            n_estimators=200, max_depth=12, min_samples_leaf=5,
            class_weight="balanced", random_state=42, n_jobs=-1,
        ).fit(X_train, y_train)
        best_params = clf.get_params()
        cv_score = None

    # Validation + test metrics
    val_pred  = clf.predict(X_val)
    test_pred = clf.predict(X_test)

    val_acc  = accuracy_score(y_val,  val_pred)
    test_acc = accuracy_score(y_test, test_pred)
    val_f1   = f1_score(y_val,  val_pred,  average="weighted", zero_division=0)
    test_f1  = f1_score(y_test, test_pred, average="weighted", zero_division=0)

    logger.info(
        f"Congestion — val acc {val_acc:.4f} / test acc {test_acc:.4f} | "
        f"val f1 {val_f1:.4f} / test f1 {test_f1:.4f}"
    )

    test_report = classification_report(
        y_test, test_pred,
        target_names=CONGESTION_LABELS, output_dict=True, zero_division=0,
    )

    metrics = {
        "cv_f1_weighted":     cv_score,
        "val_accuracy":       round(val_acc,  4),
        "test_accuracy":      round(test_acc, 4),
        "val_f1_weighted":    round(val_f1,   4),
        "test_f1_weighted":   round(test_f1,  4),
        "test_classification_report": test_report,
        "best_params":        best_params,
        "feature_importances": dict(zip(CAUSAL_FEATURE_COLS,
                                        [round(float(v), 4) for v in clf.feature_importances_])),
    }
    return clf, metrics


def train_speed_predictor(
    X_train, y_train, X_val, y_val, X_test, y_test, tune: bool = True,
):
    """GradientBoosting regressor with optional GridSearchCV."""
    logger.info("Training speed predictor (GradientBoosting)…")

    base = GradientBoostingRegressor(random_state=42)

    if tune:
        param_grid = {
            "n_estimators":  [150, 250],
            "max_depth":     [3, 5],
            "learning_rate": [0.05, 0.1],
        }
        cv = TimeSeriesSplit(n_splits=5)
        gs = GridSearchCV(
            base, param_grid, cv=cv, scoring="neg_mean_absolute_error",
            n_jobs=-1, verbose=0, refit=True,
        )
        gs.fit(X_train, y_train)
        reg = gs.best_estimator_
        best_params = gs.best_params_
        cv_score    = float(-gs.best_score_)   # back to MAE
        logger.info(f"Best params: {best_params}  CV MAE={cv_score:.3f} mph")
    else:
        reg = GradientBoostingRegressor(
            n_estimators=200, max_depth=5,
            learning_rate=0.05, subsample=0.8, random_state=42,
        ).fit(X_train, y_train)
        best_params = reg.get_params()
        cv_score = None

    val_pred  = reg.predict(X_val)
    test_pred = reg.predict(X_test)

    val_mae  = mean_absolute_error(y_val,  val_pred)
    test_mae = mean_absolute_error(y_test, test_pred)
    test_rmse = float(np.sqrt(mean_squared_error(y_test, test_pred)))
    test_r2  = r2_score(y_test, test_pred)

    logger.info(
        f"Speed — val MAE {val_mae:.3f} / test MAE {test_mae:.3f} mph "
        f"RMSE {test_rmse:.3f}  R² {test_r2:.4f}"
    )

    metrics = {
        "cv_mae":             cv_score,
        "val_mae":             round(val_mae,  4),
        "test_mae":            round(test_mae, 4),
        "test_rmse":           round(test_rmse, 4),
        "test_r2":             round(test_r2, 4),
        "best_params":         best_params,
        "feature_importances": dict(zip(CAUSAL_FEATURE_COLS,
                                        [round(float(v), 4) for v in reg.feature_importances_])),
    }
    return reg, metrics


def train_anomaly_detector(X_train, X_test):
    """Unsupervised IsolationForest — no leakage concept (no labels)."""
    logger.info("Training anomaly detector (IsolationForest)…")
    iso = IsolationForest(
        n_estimators=200,
        contamination=0.05,
        random_state=42,
        n_jobs=-1,
    ).fit(X_train)

    train_anomaly_rate = float((iso.predict(X_train) == -1).mean())
    test_anomaly_rate  = float((iso.predict(X_test)  == -1).mean())
    train_scores = iso.decision_function(X_train)
    test_scores  = iso.decision_function(X_test)

    logger.info(
        f"Anomaly — train rate {train_anomaly_rate:.2%} / "
        f"test rate {test_anomaly_rate:.2%}"
    )

    metrics = {
        "contamination":      0.05,
        "train_anomaly_rate": round(train_anomaly_rate, 4),
        "test_anomaly_rate":  round(test_anomaly_rate,  4),
        "train_score_mean":   round(float(train_scores.mean()), 4),
        "train_score_std":    round(float(train_scores.std()),  4),
        "test_score_mean":    round(float(test_scores.mean()),  4),
        "test_score_std":     round(float(test_scores.std()),   4),
    }
    return iso, metrics


# ── Persistence ────────────────────────────────────────────────────────────────

def save_model(model, name: str, metadata: dict):
    """Save a model and its metadata JSON to models/."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODELS_DIR / f"{name}.joblib"
    meta_path  = MODELS_DIR / f"{name}_metadata.json"

    joblib.dump(model, model_path)
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    logger.info(f"Saved {name}.joblib + metadata")


# ── Orchestration ──────────────────────────────────────────────────────────────

def main(days: int = LOOKBACK_DAYS_DEFAULT,
         min_records: int = MIN_RECORDS_DEFAULT,
         tune: bool = True):
    logger.info("═══════════════════════════════════════════════════════")
    logger.info(" NYC Traffic – Phase 3 Model Training (v2)            ")
    logger.info(f" Methodology: temporal split + TimeSeriesSplit CV     ")
    logger.info(f" Hyperparameter tuning: {'ON' if tune else 'OFF'}                          ")
    logger.info("═══════════════════════════════════════════════════════")

    # ── Fetch ─────────────────────────────────────────────────────
    df_raw = fetch_training_data(days=days)
    if df_raw.empty or len(df_raw) < min_records:
        logger.error(
            f"Not enough data: {len(df_raw):,} records "
            f"(need ≥ {min_records:,}). Run the stream processor first."
        )
        return

    # Required: processed_at for temporal split
    if "processed_at" not in df_raw.columns:
        logger.error("`processed_at` field missing — cannot do temporal split")
        return

    # ── Coerce + clean ────────────────────────────────────────────
    df = coerce_types(df_raw)
    df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")
    df = df[df["processed_at"].notna()]
    df = df[df["borough_encoded"] >= 0]
    df = df[df["speed"].notna()]
    df = df[df["congestion_level"].notna()]
    logger.info(f"After cleaning: {len(df):,} records")

    versioning = _versioning_metadata()
    versioning["data_signature"] = _data_signature(df)
    logger.info(f"Data signature: {versioning['data_signature']}")
    logger.info(f"Code version: {versioning['code_version']}, "
                f"git: {versioning['git_commit'][:8]}")

    # ── Temporal split (70/15/15) ─────────────────────────────────
    train_df, val_df, test_df = temporal_split(
        df, time_col="processed_at",
        train_frac=0.70, val_frac=0.15,
    )
    logger.info(
        f"Temporal split — train: {len(train_df):,}  "
        f"val: {len(val_df):,}  test: {len(test_df):,}"
    )

    # ════════════════════════════════════════════════════════════════
    # MODEL 1 — CONGESTION CLASSIFIER
    # ════════════════════════════════════════════════════════════════
    imputer_cls = FeatureImputer(CAUSAL_FEATURE_COLS).fit(
        build_X(train_df, CAUSAL_FEATURE_COLS)
    )
    X_train_c = imputer_cls.transform(build_X(train_df, CAUSAL_FEATURE_COLS))
    X_val_c   = imputer_cls.transform(build_X(val_df,   CAUSAL_FEATURE_COLS))
    X_test_c  = imputer_cls.transform(build_X(test_df,  CAUSAL_FEATURE_COLS))

    y_train_c = encode_labels(train_df["congestion_level"])
    y_val_c   = encode_labels(val_df  ["congestion_level"])
    y_test_c  = encode_labels(test_df ["congestion_level"])

    # Drop any unmapped labels (-1)
    m = y_train_c >= 0
    X_train_c, y_train_c = X_train_c[m], y_train_c[m]
    m = y_val_c >= 0
    X_val_c, y_val_c     = X_val_c[m],   y_val_c[m]
    m = y_test_c >= 0
    X_test_c, y_test_c   = X_test_c[m],  y_test_c[m]

    clf, clf_metrics = train_congestion_classifier(
        X_train_c, y_train_c, X_val_c, y_val_c, X_test_c, y_test_c, tune=tune,
    )
    save_model(clf, "congestion_classifier", {
        "model_type":       "RandomForestClassifier",
        "target":           "congestion_level",
        "classes":          CONGESTION_LABELS,
        "features":         CAUSAL_FEATURE_COLS,
        "imputer_strategy": "median",
        "imputer_statistics": imputer_cls.statistics_,
        "split_strategy":   "temporal_70_15_15",
        "cv_strategy":      "TimeSeriesSplit_5folds",
        "train_size":       int(len(X_train_c)),
        "val_size":         int(len(X_val_c)),
        "test_size":        int(len(X_test_c)),
        **versioning,
        **clf_metrics,
    })
    imputer_cls.save(MODELS_DIR / "congestion_classifier_imputer.joblib")

    # ════════════════════════════════════════════════════════════════
    # MODEL 2 — SPEED PREDICTOR
    # ════════════════════════════════════════════════════════════════
    imputer_spd = FeatureImputer(CAUSAL_FEATURE_COLS).fit(
        build_X(train_df, CAUSAL_FEATURE_COLS)
    )
    X_train_s = imputer_spd.transform(build_X(train_df, CAUSAL_FEATURE_COLS))
    X_val_s   = imputer_spd.transform(build_X(val_df,   CAUSAL_FEATURE_COLS))
    X_test_s  = imputer_spd.transform(build_X(test_df,  CAUSAL_FEATURE_COLS))

    y_train_s = pd.to_numeric(train_df["speed"], errors="coerce")
    y_val_s   = pd.to_numeric(val_df  ["speed"], errors="coerce")
    y_test_s  = pd.to_numeric(test_df ["speed"], errors="coerce")

    # Drop rows with null speed targets
    m = y_train_s.notna()
    X_train_s, y_train_s = X_train_s[m.values], y_train_s[m]
    m = y_val_s.notna()
    X_val_s,   y_val_s   = X_val_s[m.values],   y_val_s[m]
    m = y_test_s.notna()
    X_test_s,  y_test_s  = X_test_s[m.values],  y_test_s[m]

    reg, reg_metrics = train_speed_predictor(
        X_train_s, y_train_s, X_val_s, y_val_s, X_test_s, y_test_s, tune=tune,
    )
    save_model(reg, "speed_predictor", {
        "model_type":       "GradientBoostingRegressor",
        "target":           "speed",
        "features":         CAUSAL_FEATURE_COLS,
        "imputer_strategy": "median",
        "imputer_statistics": imputer_spd.statistics_,
        "split_strategy":   "temporal_70_15_15",
        "cv_strategy":      "TimeSeriesSplit_5folds",
        "train_size":       int(len(X_train_s)),
        "val_size":         int(len(X_val_s)),
        "test_size":        int(len(X_test_s)),
        **versioning,
        **reg_metrics,
    })
    imputer_spd.save(MODELS_DIR / "speed_predictor_imputer.joblib")

    # ════════════════════════════════════════════════════════════════
    # MODEL 3 — ANOMALY DETECTOR
    # ════════════════════════════════════════════════════════════════
    imputer_anom = FeatureImputer(ANOMALY_FEATURE_COLS).fit(
        build_X(train_df, ANOMALY_FEATURE_COLS)
    )
    X_train_a = imputer_anom.transform(build_X(train_df, ANOMALY_FEATURE_COLS))
    X_test_a  = imputer_anom.transform(build_X(test_df,  ANOMALY_FEATURE_COLS))

    iso, iso_metrics = train_anomaly_detector(X_train_a, X_test_a)
    save_model(iso, "anomaly_detector", {
        "model_type":       "IsolationForest",
        "features":         ANOMALY_FEATURE_COLS,
        "imputer_strategy": "median",
        "imputer_statistics": imputer_anom.statistics_,
        "split_strategy":   "temporal_70_15_15",
        "train_size":       int(len(X_train_a)),
        "test_size":        int(len(X_test_a)),
        **versioning,
        **iso_metrics,
    })
    imputer_anom.save(MODELS_DIR / "anomaly_detector_imputer.joblib")

    # ── Final summary ─────────────────────────────────────────────
    logger.info("═══════════════════════════════════════════════════════")
    logger.info(" Training complete — HONEST hold-out test metrics:    ")
    logger.info(f"  Congestion test accuracy : {clf_metrics['test_accuracy']*100:.2f}%")
    logger.info(f"  Congestion test F1 (wgt) : {clf_metrics['test_f1_weighted']:.4f}")
    logger.info(f"  Speed test MAE           : {reg_metrics['test_mae']} mph")
    logger.info(f"  Speed test R²            : {reg_metrics['test_r2']}")
    logger.info(f"  Anomaly test rate        : {iso_metrics['test_anomaly_rate']:.2%}")
    logger.info(f"  Phase 3 target (≥75%)    : "
                f"{'PASS ✓' if clf_metrics['test_accuracy'] >= 0.75 else 'FAIL ✗'}")
    logger.info("═══════════════════════════════════════════════════════")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Traffic Phase 3 – Model Training (v2)")
    parser.add_argument("--days",        type=int, default=LOOKBACK_DAYS_DEFAULT)
    parser.add_argument("--min-records", type=int, default=MIN_RECORDS_DEFAULT)
    parser.add_argument("--no-tune",     action="store_true",
                        help="Skip GridSearchCV (faster, lower quality)")
    args = parser.parse_args()
    main(days=args.days, min_records=args.min_records, tune=not args.no_tune)
