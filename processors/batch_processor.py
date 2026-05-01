"""
Phase 2 – Batch Processor

Reads processed records from MongoDB (traffic_processed) and produces:
  • Hourly × borough speed/congestion aggregations  → traffic_aggregated
  • Network-wide window summary                     → traffic_aggregated
  • Per-run data quality metrics                    → data_quality_metrics

Falls back to Kafka backfill (earliest offset) when MongoDB has no data yet.

Usage:
    python processors/batch_processor.py            # run once and exit
    python processors/batch_processor.py --loop     # run every BATCH_INTERVAL_MINUTES
"""
import argparse
import json
import os
from collections import Counter
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import schedule
import time
from loguru import logger
from pymongo import MongoClient, UpdateOne

from storage.mongodb_client import (
    MONGO_URI, MONGO_DB,
    COL_PROCESSED, COL_AGGREGATED, COL_DQ_METRICS, COL_LINEAGE,
    setup_collections, write_with_retry,
    make_lineage_entry, record_lineage,
)
from processors.data_cleaner import clean_record

PROCESSOR_VERSION = "2.1.0"

BATCH_TRANSFORMATIONS = [
    "fetch_from_mongodb_or_kafka",
    "compute_hourly_borough_aggregations",
    "compute_network_summary",
    "compute_quality_metrics",
]

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_SERVERS          = os.getenv("KAFKA_BOOTSTRAP_SERVERS",  "localhost:9092")
KAFKA_TOPIC            = os.getenv("TOPIC_TRAFFIC_VALIDATED",  "traffic-validated")
LOOKBACK_HOURS         = int(os.getenv("BATCH_LOOKBACK_HOURS", "24"))
BATCH_INTERVAL_MINUTES = int(os.getenv("BATCH_INTERVAL_MINUTES", "60"))


# ── Helpers ────────────────────────────────────────────────────────────────────

def _round(val, digits: int = 2):
    if val is None:
        return None
    try:
        return round(float(val), digits)
    except (TypeError, ValueError):
        return None


def _to_iso(val) -> str:
    """DEPRECATED — use _to_datetime; kept for any legacy callers."""
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def _to_datetime(val) -> datetime:
    """
    Convert a pandas Timestamp / datetime / string into a tz-aware UTC datetime
    suitable for storing as a BSON Date.  Returns naive UTC if the input has
    no timezone info.
    """
    if val is None:
        return None
    if hasattr(val, "to_pydatetime"):    # pandas Timestamp
        val = val.to_pydatetime()
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)
    # String fallback
    try:
        parsed = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return datetime.now(timezone.utc)


# ── Data Fetching ──────────────────────────────────────────────────────────────

def fetch_from_mongodb(db, hours: int) -> pd.DataFrame:
    """Pull the last `hours` of stream-processed records from MongoDB."""
    # Use naive UTC to match Spark's current_timestamp() storage convention.
    since = datetime.utcnow() - timedelta(hours=hours)

    projection = {
        "_id": 0,
        "borough": 1, "speed": 1, "travel_time": 1,
        "congestion_level": 1, "congestion_score": 1,
        "hour_of_day": 1, "day_of_week": 1,
        "is_peak_hour": 1, "time_bucket": 1,
        "data_as_of_parsed": 1, "processed_at": 1,
        "quality_flags": 1,
    }
    docs = list(db[COL_PROCESSED].find(
        {"processed_at": {"$gte": since}},
        projection,
    ))
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


def backfill_from_kafka() -> pd.DataFrame:
    """
    Re-consume traffic-validated from the earliest available offset.
    Used only when MongoDB has no data yet (first run / fresh deployment).
    """
    from kafka import KafkaConsumer

    logger.info("Backfilling from Kafka (earliest offset)…")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=15_000,      # stop polling after 15 s of silence
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="batch-backfill",
    )

    records = []
    for msg in consumer:
        records.append(clean_record(msg.value))
    consumer.close()

    logger.info(f"Backfill: received {len(records)} records from Kafka")
    return pd.DataFrame(records) if records else pd.DataFrame()


# ── Aggregations ───────────────────────────────────────────────────────────────

def hourly_borough_aggregations(df: pd.DataFrame) -> list[dict]:
    """
    Compute hourly × borough statistics:
      speed (avg/min/max/median), travel_time avg, congestion score,
      congestion level distribution + percentages.
    """
    if df.empty:
        return []

    df = df.copy()
    time_col = "data_as_of_parsed" if "data_as_of_parsed" in df.columns else "processed_at"
    df["window_hour"] = pd.to_datetime(df[time_col], utc=True, errors="coerce").dt.floor("h")
    df = df.dropna(subset=["borough", "window_hour"])

    # Base speed/time stats
    base = (
        df.groupby(["window_hour", "borough"])
        .agg(
            record_count        = ("speed",            "count"),
            avg_speed           = ("speed",            "mean"),
            min_speed           = ("speed",            "min"),
            max_speed           = ("speed",            "max"),
            median_speed        = ("speed",            "median"),
            avg_travel_time     = ("travel_time",      "mean"),
            avg_congestion_score= ("congestion_score", "mean"),
        )
        .reset_index()
    )

    # Congestion level distribution (pivot)
    cong = (
        df.groupby(["window_hour", "borough", "congestion_level"])
        .size()
        .reset_index(name="cnt")
        .pivot_table(
            index=["window_hour", "borough"],
            columns="congestion_level",
            values="cnt",
            fill_value=0,
        )
        .reset_index()
    )
    agg = base.merge(cong, on=["window_hour", "borough"], how="left")

    congestion_levels = ["FREE_FLOW", "MODERATE", "CONGESTED", "SEVERE"]
    for lvl in congestion_levels:
        if lvl not in agg.columns:
            agg[lvl] = 0

    now = datetime.now(timezone.utc)
    docs = []
    for _, row in agg.iterrows():
        total = int(row["record_count"])
        cong_dist = {lvl: int(row.get(lvl, 0) or 0) for lvl in congestion_levels}
        cong_pct  = {
            lvl: round(cong_dist[lvl] / total * 100, 2) if total else 0.0
            for lvl in congestion_levels
        }
        docs.append({
            "aggregation_type":    "hourly_borough",
            "window_start":        _to_datetime(row["window_hour"]),  # BSON datetime
            "borough":             row["borough"],
            "record_count":        total,
            "speed_stats": {
                "avg":    _round(row.get("avg_speed")),
                "min":    _round(row.get("min_speed")),
                "max":    _round(row.get("max_speed")),
                "median": _round(row.get("median_speed")),
            },
            "avg_travel_time":          _round(row.get("avg_travel_time")),
            "avg_congestion_score":     _round(row.get("avg_congestion_score"), 4),
            "congestion_distribution":  cong_dist,
            "congestion_pct":           cong_pct,
            "computed_at":              now,                          # BSON datetime
        })
    return docs


def network_summary(df: pd.DataFrame, window_start: datetime, window_end: datetime) -> dict:
    """Network-wide summary over the entire batch window."""
    if df.empty:
        return {}

    cong_dist = (
        df["congestion_level"].value_counts().to_dict()
        if "congestion_level" in df.columns else {}
    )
    borough_counts = (
        df["borough"].value_counts().to_dict()
        if "borough" in df.columns else {}
    )
    peak_pct = None
    if "is_peak_hour" in df.columns:
        valid = df["is_peak_hour"].dropna()
        peak_pct = round(valid.mean() * 100, 2) if not valid.empty else None

    return {
        "aggregation_type":              "window_summary",
        "window_start":                  _to_datetime(window_start),   # BSON datetime
        "window_end":                    _to_datetime(window_end),     # BSON datetime
        "total_records":                 len(df),
        "network_avg_speed":             _round(df["speed"].mean() if "speed" in df.columns else None),
        "network_avg_congestion_score":  _round(df["congestion_score"].mean() if "congestion_score" in df.columns else None, 4),
        "congestion_distribution":       {k: int(v) for k, v in cong_dist.items()},
        "borough_record_counts":         {k: int(v) for k, v in borough_counts.items()},
        "peak_hour_pct":                 peak_pct,
        "computed_at":                   datetime.now(timezone.utc),   # BSON datetime
    }


# ── Persistence ────────────────────────────────────────────────────────────────

def upsert_aggregations(db, docs: list[dict]):
    """Upsert all aggregation docs (safe to re-run on same window).  Retries on transient MongoDB errors."""
    if not docs:
        return

    ops = []
    for doc in docs:
        flt = {
            "aggregation_type": doc["aggregation_type"],
            "window_start":     doc["window_start"],
        }
        if "borough" in doc:
            flt["borough"] = doc["borough"]
        ops.append(UpdateOne(flt, {"$set": doc}, upsert=True))

    result = write_with_retry(
        db[COL_AGGREGATED].bulk_write, ops,
        op_name="aggregations_bulk_upsert",
    )
    logger.info(
        f"Aggregations → upserted {result.upserted_count}, "
        f"modified {result.modified_count}"
    )


def record_batch_quality(db, df: pd.DataFrame, batch_id: str):
    """Store data quality metrics for this batch run."""
    if df.empty:
        total = null_speed = null_borough = 0
        flags_dist: dict = {}
        quality_score = 0.0
    else:
        total        = len(df)
        null_speed   = int(df["speed"].isna().sum())   if "speed"   in df.columns else 0
        null_borough = int(df["borough"].isna().sum()) if "borough" in df.columns else 0
        valid        = total - null_speed - null_borough
        quality_score = round(valid / total * 100, 2) if total else 0.0

        if "quality_flags" in df.columns:
            all_flags = [
                f
                for flags in df["quality_flags"].dropna()
                for f in (flags if isinstance(flags, list) else [])
            ]
            flags_dist = dict(Counter(all_flags))
        else:
            flags_dist = {}

    metric = {
        "batch_id":                batch_id,
        "source":                  "batch_processor",
        "recorded_at":             datetime.now(timezone.utc),  # BSON datetime
        "total_records":           total,
        "valid_records":           total - null_speed - null_borough,
        "quality_score":           quality_score,
        "null_counts":             {"speed": null_speed, "borough": null_borough},
        "quality_flags_distribution": flags_dist,
    }
    write_with_retry(
        db[COL_DQ_METRICS].update_one,
        {"batch_id": batch_id},
        {"$set": metric},
        upsert=True,
        op_name="batch_quality_upsert",
    )
    logger.info(f"Quality metrics stored (score={quality_score}%)")
    return {"quality_score": quality_score, "valid": total - null_speed - null_borough}


# ── Main Batch Job ─────────────────────────────────────────────────────────────

def run_batch():
    started_at = datetime.now(timezone.utc)
    batch_id   = f"batch_{started_at.strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"==== Batch job started: {batch_id} ====")

    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]
    setup_collections(db)

    status: str = "success"
    errors: list = []
    record_count: int = 0
    quality_summary: Optional[dict] = None
    source_type = "mongodb"

    try:
        df = fetch_from_mongodb(db, hours=LOOKBACK_HOURS)

        if df.empty:
            logger.warning(f"No records in MongoDB for last {LOOKBACK_HOURS}h — trying Kafka backfill")
            df = backfill_from_kafka()
            source_type = "kafka_backfill"

        if df.empty:
            logger.warning("No data available — skipping this batch run")
            status = "skipped_empty"
        else:
            record_count = len(df)
            logger.info(f"Processing {record_count:,} records")

            window_end   = started_at
            window_start = started_at - timedelta(hours=LOOKBACK_HOURS)

            # Hourly × borough aggregations
            hourly_docs = hourly_borough_aggregations(df)
            upsert_aggregations(db, hourly_docs)
            logger.info(f"Stored {len(hourly_docs)} hourly aggregations")

            # Network-wide summary
            summary = network_summary(df, window_start, window_end)
            if summary:
                write_with_retry(
                    db[COL_AGGREGATED].update_one,
                    {"aggregation_type": "window_summary",
                     "window_start": summary["window_start"]},
                    {"$set": summary}, upsert=True,
                    op_name="network_summary_upsert",
                )
                logger.info("Network summary stored")

            # Data quality metrics
            quality_summary = record_batch_quality(db, df, batch_id)

        logger.info(f"==== Batch job complete: {batch_id} ====")

    except Exception as exc:
        status = "failed"
        errors.append(f"{type(exc).__name__}: {exc}")
        logger.error(f"Batch job failed: {exc}")
        # Re-raise after lineage is recorded
        _raise = exc
    else:
        _raise = None
    finally:
        # Always record lineage — even on failure
        completed_at = datetime.now(timezone.utc)
        try:
            entry = make_lineage_entry(
                phase             = "batch_processing",
                processor         = "batch_processor",
                processor_version = PROCESSOR_VERSION,
                source = {
                    "type":       source_type,
                    "collection": COL_PROCESSED if source_type == "mongodb" else None,
                    "topic":      KAFKA_TOPIC if source_type == "kafka_backfill" else None,
                    "lookback_hours": LOOKBACK_HOURS,
                },
                destination = {
                    "type":        "mongodb",
                    "database":    MONGO_DB,
                    "collection":  COL_AGGREGATED,
                },
                record_count    = record_count,
                started_at      = started_at,
                completed_at    = completed_at,
                transformations = BATCH_TRANSFORMATIONS,
                schema_version  = "1.0",
                status          = status,
                errors          = errors,
                extra = {
                    "batch_id":        batch_id,
                    "quality_summary": quality_summary,
                },
            )
            record_lineage(db, entry)
        except Exception as lineage_exc:
            logger.warning(f"Failed to record lineage: {lineage_exc}")
        client.close()
        if _raise is not None:
            raise _raise


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="NYC Traffic Batch Processor v2")
    parser.add_argument(
        "--loop", action="store_true",
        help=f"Run every {BATCH_INTERVAL_MINUTES} minutes (default: run once)",
    )
    parser.add_argument(
        "--interval-minutes", type=int, default=BATCH_INTERVAL_MINUTES,
        help="Schedule interval in minutes",
    )
    args = parser.parse_args()

    if args.loop:
        logger.info(f"Batch processor scheduled every {args.interval_minutes} minutes")
        run_batch()                                             # immediate first run
        schedule.every(args.interval_minutes).minutes.do(run_batch)
        while True:
            schedule.run_pending()
            time.sleep(30)
    else:
        run_batch()


if __name__ == "__main__":
    main()
