"""
Phase 2 – Data Quality Monitor

Provides helper methods for querying quality metrics, aggregations, and
storage statistics from MongoDB.  Consumed by the Phase 2 dashboard extension
and any ad-hoc reporting scripts.

Tracks:
  • Quality score trends (stream + batch)
  • Null/anomaly summaries per batch
  • Congestion trends over time
  • Storage utilisation per collection
  • End-to-end data lineage summary
"""
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
from loguru import logger
from pymongo import MongoClient

from storage.mongodb_client import (
    MONGO_URI, MONGO_DB,
    COL_PROCESSED, COL_AGGREGATED, COL_DQ_METRICS,
)


class DataQualityMonitor:
    """
    Context-manager-aware wrapper around the MongoDB quality/aggregation
    collections introduced in Phase 2.

    Usage:
        with DataQualityMonitor() as monitor:
            df = monitor.get_recent_quality_metrics(hours=12)
    """

    def __init__(self):
        self._client = MongoClient(MONGO_URI)
        self._db     = self._client[MONGO_DB]

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ── Quality Metrics ────────────────────────────────────────────────────────

    def get_recent_quality_metrics(self, hours: int = 24) -> pd.DataFrame:
        """Return quality metric documents from the last `hours` hours."""
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        docs = list(
            self._db[COL_DQ_METRICS].find(
                {"recorded_at": {"$gte": since}},
                {"_id": 0},
                sort=[("recorded_at", -1)],
                limit=500,
            )
        )
        return pd.DataFrame(docs)

    def get_latest_quality_score(self, source: str = "spark_stream_processor") -> Optional[float]:
        """Return the most recent quality_score for the given processor source."""
        doc = self._db[COL_DQ_METRICS].find_one(
            {"source": source},
            sort=[("recorded_at", -1)],
        )
        return doc.get("quality_score") if doc else None

    def get_quality_trend(self, hours: int = 24) -> pd.DataFrame:
        """
        Quality score over time, combining stream and batch sources.
        Useful for plotting quality drift.
        """
        df = self.get_recent_quality_metrics(hours=hours)
        if df.empty or "recorded_at" not in df.columns:
            return df
        df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True, errors="coerce")
        return df[["recorded_at", "source", "quality_score", "total_records"]].dropna()

    # ── Aggregations ───────────────────────────────────────────────────────────

    def get_hourly_aggregations(
        self,
        hours: int = 24,
        borough: Optional[str] = None,
    ) -> pd.DataFrame:
        """Return hourly × borough aggregation documents."""
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        query: dict = {
            "aggregation_type": "hourly_borough",
            "window_start":     {"$gte": since},
        }
        if borough:
            query["borough"] = borough

        docs = list(
            self._db[COL_AGGREGATED].find(
                query, {"_id": 0},
                sort=[("window_start", -1)],
            )
        )
        return pd.DataFrame(docs)

    def get_network_summaries(self, hours: int = 24) -> pd.DataFrame:
        """Return network-wide window summaries from the batch processor."""
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        docs = list(
            self._db[COL_AGGREGATED].find(
                {
                    "aggregation_type": "window_summary",
                    "window_start":     {"$gte": since},
                },
                {"_id": 0},
                sort=[("window_start", -1)],
            )
        )
        return pd.DataFrame(docs)

    # ── Congestion Trends ──────────────────────────────────────────────────────

    def get_congestion_trends(self, hours: int = 6) -> pd.DataFrame:
        """
        Sample of stream-processed records for trend visualisation.
        Capped at 10 000 documents to keep response times low.
        """
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        docs = list(
            self._db[COL_PROCESSED].find(
                {"processed_at": {"$gte": since}},
                {
                    "_id": 0,
                    "borough": 1, "congestion_level": 1,
                    "speed": 1,   "hour_of_day": 1,
                    "processed_at": 1,
                },
                limit=10_000,
            )
        )
        return pd.DataFrame(docs)

    # ── Storage Statistics ─────────────────────────────────────────────────────

    def get_storage_stats(self) -> dict:
        """Return document counts per Phase 2 collection."""
        stats = {}
        for col_name in [COL_PROCESSED, COL_AGGREGATED, COL_DQ_METRICS]:
            stats[col_name] = {
                "document_count": self._db[col_name].count_documents({}),
            }
        return stats

    # ── Lineage Summary ────────────────────────────────────────────────────────

    def get_lineage_summary(self) -> dict:
        """
        High-level pipeline lineage report: how many records flowed through
        each stage and what the latest quality score was.
        """
        total_processed = self._db[COL_PROCESSED].count_documents({})
        total_aggs      = self._db[COL_AGGREGATED].count_documents({})

        latest = self._db[COL_DQ_METRICS].find_one(
            {}, sort=[("recorded_at", -1)]
        )
        stream_batches = self._db[COL_DQ_METRICS].count_documents(
            {"source": "spark_stream_processor"}
        )
        batch_jobs = self._db[COL_DQ_METRICS].count_documents(
            {"source": "batch_processor"}
        )

        return {
            "total_processed_records": total_processed,
            "total_aggregations":       total_aggs,
            "stream_batches_recorded":  stream_batches,
            "batch_jobs_recorded":      batch_jobs,
            "latest_quality_score":     latest.get("quality_score")  if latest else None,
            "latest_recorded_at":       latest.get("recorded_at")    if latest else None,
        }

    # ── Anomaly Summary ────────────────────────────────────────────────────────

    def get_anomaly_summary(self, hours: int = 24) -> dict:
        """
        Aggregate anomaly counts across recent stream batches.
        Returns totals for zero_speed, unknown_congestion, and null fields.
        """
        df = self.get_recent_quality_metrics(hours=hours)
        if df.empty or "anomalies" not in df.columns:
            return {}

        totals: dict = {}
        for row in df["anomalies"].dropna():
            if isinstance(row, dict):
                for k, v in row.items():
                    totals[k] = totals.get(k, 0) + int(v)

        null_totals: dict = {}
        if "null_counts" in df.columns:
            for row in df["null_counts"].dropna():
                if isinstance(row, dict):
                    for k, v in row.items():
                        null_totals[k] = null_totals.get(k, 0) + int(v)

        return {
            "anomalies":   totals,
            "null_counts": null_totals,
            "window_hours": hours,
        }
