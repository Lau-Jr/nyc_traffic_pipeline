"""
NYC Traffic Analytics Pipeline — REST API (Bonus: +10 pts)

Standalone FastAPI service that exposes pipeline data and ML predictions
for third-party integration.

Run:
    uvicorn api.traffic_api:app --reload --port 8000

Swagger UI: http://localhost:8000/docs
ReDoc:       http://localhost:8000/redoc
"""
from __future__ import annotations

import sys
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

# ── Path bootstrap (run from project root) ─────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# ── MongoDB config (mirrors storage/mongodb_client.py) ────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "nyc_traffic")

COL_PROCESSED  = "traffic_processed"
COL_AGGREGATED = "traffic_aggregated"
COL_DQ_METRICS = "data_quality_metrics"
COL_LINEAGE    = "data_lineage"

VALID_BOROUGHS = {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"}

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="NYC Traffic Analytics API",
    description=(
        "Real-time REST API for the NYC Traffic Analytics Pipeline.\n\n"
        "Provides live traffic records, borough congestion summaries, "
        "ML predictions, anomaly alerts, and pipeline health — all sourced "
        "from the live MongoDB store.\n\n"
        "**Data source:** NYC Department of Transportation (DOT) Open Data API, "
        "updated every ~60 seconds."
    ),
    version="1.0.0",
    contact={
        "name": "NYC Traffic Analytics Team",
        "url":  "http://localhost:8501",
    },
    license_info={
        "name": "Academic — Big Data Analytics MSc Capstone",
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── MongoDB connection (module-level, pooled) ──────────────────────────────────
_client: Optional[MongoClient] = None


def get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client[MONGO_DB]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(doc: dict) -> dict:
    """Remove MongoDB _id and convert datetimes to ISO strings."""
    doc.pop("_id", None)
    for k, v in doc.items():
        if isinstance(v, datetime):
            doc[k] = v.isoformat()
    return doc


# ── Pydantic response models ───────────────────────────────────────────────────

class APIInfo(BaseModel):
    name: str
    version: str
    description: str
    endpoints: List[str]
    swagger_ui: str
    timestamp: str


class HealthStatus(BaseModel):
    status: str = Field(description="HEALTHY | DEGRADED | UNAVAILABLE")
    mongodb: bool
    total_processed_records: int
    total_aggregated_records: int
    last_record_at: Optional[str]
    minutes_since_last_record: Optional[float]
    pipeline_status: str
    timestamp: str


class TrafficRecord(BaseModel):
    link_name: Optional[str]
    borough: Optional[str]
    speed: Optional[float]
    travel_time: Optional[int]
    congestion_level: Optional[str]
    hour_of_day: Optional[int]
    is_peak_hour: Optional[bool]
    latitude: Optional[float]
    longitude: Optional[float]
    data_as_of: Optional[str]
    processed_at: Optional[str]


class BoroughSummary(BaseModel):
    borough: str
    avg_speed_mph: float
    dominant_congestion: str
    record_count: int
    pct_congested: float
    last_updated: Optional[str]


class AggregatedRecord(BaseModel):
    borough: Optional[str]
    window_start: Optional[str]
    window_end: Optional[str]
    avg_speed: Optional[float]
    record_count: Optional[int]
    aggregation_type: Optional[str]


class AnomalyRecord(BaseModel):
    link_name: Optional[str]
    borough: Optional[str]
    speed: Optional[float]
    congestion_level: Optional[str]
    anomaly_score: Optional[float]
    processed_at: Optional[str]


class PredictionResult(BaseModel):
    link_name: Optional[str]
    borough: Optional[str]
    observed_speed: Optional[float]
    predicted_congestion: str
    congestion_confidence: float
    predicted_speed: float
    is_anomaly: bool
    anomaly_score: float


class StandardResponse(BaseModel):
    status: str
    count: int
    data: list
    timestamp: str


# ── Lazy ML predictor ─────────────────────────────────────────────────────────
_predictor = None


def _get_predictor():
    global _predictor
    if _predictor is None:
        from analytics.predictor import TrafficPredictor
        p = TrafficPredictor()
        if p.models_exist():
            p.load()
            _predictor = p
    return _predictor


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get(
    "/",
    response_model=APIInfo,
    summary="API overview",
    tags=["General"],
)
def root():
    """Returns API metadata and a list of all available endpoints."""
    return APIInfo(
        name="NYC Traffic Analytics API",
        version="1.0.0",
        description="Real-time traffic data, ML predictions, and pipeline health for NYC roads.",
        endpoints=[
            "GET /health                       — Pipeline health and MongoDB status",
            "GET /traffic/live                 — Latest traffic records (filterable)",
            "GET /traffic/borough/{borough}    — Records for a specific borough",
            "GET /traffic/congestion           — Current congestion summary per borough",
            "GET /traffic/aggregated           — Hourly batch aggregations",
            "GET /traffic/anomalies            — Recent anomaly detections",
            "GET /traffic/predict              — ML predictions on latest live records",
        ],
        swagger_ui="http://localhost:8000/docs",
        timestamp=_now_iso(),
    )


@app.get(
    "/health",
    response_model=HealthStatus,
    summary="Pipeline health check",
    tags=["General"],
)
def health():
    """
    Returns the current health of the pipeline:
    - MongoDB connectivity
    - Total record counts
    - Time since the last ingested record
    - Overall pipeline status (HEALTHY / DEGRADED / UNAVAILABLE)
    """
    try:
        db = get_db()
        db.command("ping")
        mongo_ok = True
    except (ConnectionFailure, ServerSelectionTimeoutError):
        return HealthStatus(
            status="UNAVAILABLE",
            mongodb=False,
            total_processed_records=0,
            total_aggregated_records=0,
            last_record_at=None,
            minutes_since_last_record=None,
            pipeline_status="MongoDB unreachable",
            timestamp=_now_iso(),
        )

    processed_count  = db[COL_PROCESSED].count_documents({})
    aggregated_count = db[COL_AGGREGATED].count_documents({})

    last_doc = db[COL_PROCESSED].find_one(
        {}, {"processed_at": 1}, sort=[("processed_at", -1)]
    )
    last_at = last_doc.get("processed_at") if last_doc else None
    minutes_ago = None
    if last_at:
        if last_at.tzinfo is None:
            last_at = last_at.replace(tzinfo=timezone.utc)
        minutes_ago = round(
            (datetime.now(timezone.utc) - last_at).total_seconds() / 60, 1
        )

    if minutes_ago is None or minutes_ago > 10:
        status = "DEGRADED"
        pipeline_status = "No recent data — producer may be stopped"
    elif processed_count == 0:
        status = "DEGRADED"
        pipeline_status = "No records in database"
    else:
        status = "HEALTHY"
        pipeline_status = "All systems operational"

    return HealthStatus(
        status=status,
        mongodb=mongo_ok,
        total_processed_records=processed_count,
        total_aggregated_records=aggregated_count,
        last_record_at=last_at.isoformat() if last_at else None,
        minutes_since_last_record=minutes_ago,
        pipeline_status=pipeline_status,
        timestamp=_now_iso(),
    )


@app.get(
    "/traffic/live",
    summary="Latest traffic records",
    tags=["Traffic"],
)
def traffic_live(
    limit: int   = Query(50, ge=1, le=500, description="Number of records to return"),
    borough: str = Query(None, description="Filter by borough name"),
):
    """
    Returns the most recent processed traffic records from MongoDB.

    - **limit**: 1–500 (default 50)
    - **borough**: optionally filter to one borough (Manhattan, Brooklyn, Queens, Bronx, Staten Island)
    """
    if borough and borough not in VALID_BOROUGHS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid borough '{borough}'. Choose from: {sorted(VALID_BOROUGHS)}",
        )

    query = {"borough": borough} if borough else {}
    db    = get_db()
    docs  = list(
        db[COL_PROCESSED]
        .find(query, {"_id": 0})
        .sort("processed_at", -1)
        .limit(limit)
    )
    docs = [_clean(d) for d in docs]

    return JSONResponse({
        "status":    "ok",
        "count":     len(docs),
        "filters":   {"borough": borough, "limit": limit},
        "data":      docs,
        "timestamp": _now_iso(),
    })


@app.get(
    "/traffic/borough/{borough}",
    summary="Records for a specific borough",
    tags=["Traffic"],
)
def traffic_by_borough(
    borough: str,
    limit: int = Query(100, ge=1, le=500, description="Number of records to return"),
):
    """
    Returns recent traffic records filtered to a single NYC borough.

    Valid borough names: **Manhattan**, **Brooklyn**, **Queens**, **Bronx**, **Staten Island**
    """
    if borough not in VALID_BOROUGHS:
        raise HTTPException(
            status_code=404,
            detail=f"Borough '{borough}' not found. Valid options: {sorted(VALID_BOROUGHS)}",
        )

    db   = get_db()
    docs = list(
        db[COL_PROCESSED]
        .find({"borough": borough}, {"_id": 0})
        .sort("processed_at", -1)
        .limit(limit)
    )
    docs = [_clean(d) for d in docs]

    return JSONResponse({
        "status":    "ok",
        "borough":   borough,
        "count":     len(docs),
        "data":      docs,
        "timestamp": _now_iso(),
    })


@app.get(
    "/traffic/congestion",
    summary="Current congestion summary per borough",
    tags=["Traffic"],
)
def traffic_congestion():
    """
    Returns a real-time congestion snapshot for each NYC borough:
    - Average speed (mph)
    - Dominant congestion level (FREE_FLOW / MODERATE / CONGESTED / SEVERE)
    - Percentage of road links that are congested or worse
    - Record count used for the summary
    - Timestamp of the most recent record
    """
    db = get_db()
    pipeline = [
        {"$sort": {"processed_at": -1}},
        {"$limit": 5000},
        {"$group": {
            "_id": "$borough",
            "avg_speed":    {"$avg": "$speed"},
            "record_count": {"$sum": 1},
            "last_updated": {"$max": "$processed_at"},
            "congestion_levels": {"$push": "$congestion_level"},
        }},
        {"$sort": {"_id": 1}},
    ]
    results = list(db[COL_PROCESSED].aggregate(pipeline))

    summaries = []
    for r in results:
        if not r["_id"]:
            continue
        levels = r.get("congestion_levels", [])
        total  = len(levels) or 1

        from collections import Counter
        level_counts    = Counter(levels)
        dominant        = level_counts.most_common(1)[0][0] if level_counts else "UNKNOWN"
        congested_count = sum(
            v for k, v in level_counts.items()
            if k in ("CONGESTED", "SEVERE")
        )

        last = r.get("last_updated")
        summaries.append({
            "borough":            r["_id"],
            "avg_speed_mph":      round(r["avg_speed"] or 0, 2),
            "dominant_congestion": dominant,
            "record_count":       r["record_count"],
            "pct_congested":      round(congested_count / total * 100, 1),
            "last_updated":       last.isoformat() if last else None,
        })

    return JSONResponse({
        "status":    "ok",
        "count":     len(summaries),
        "data":      summaries,
        "timestamp": _now_iso(),
    })


@app.get(
    "/traffic/aggregated",
    summary="Hourly batch aggregations",
    tags=["Traffic"],
)
def traffic_aggregated(
    limit:   int = Query(50, ge=1, le=200, description="Number of aggregation windows to return"),
    borough: str = Query(None, description="Filter by borough"),
):
    """
    Returns hourly aggregation records produced by the batch processor.

    Each record represents one hour of traffic data for one borough,
    including average speed, record count, and data quality indicators.
    """
    if borough and borough not in VALID_BOROUGHS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid borough '{borough}'. Choose from: {sorted(VALID_BOROUGHS)}",
        )

    query = {"borough": borough} if borough else {}
    db    = get_db()
    docs  = list(
        db[COL_AGGREGATED]
        .find(query, {"_id": 0})
        .sort("window_start", -1)
        .limit(limit)
    )
    docs = [_clean(d) for d in docs]

    return JSONResponse({
        "status":    "ok",
        "count":     len(docs),
        "filters":   {"borough": borough, "limit": limit},
        "data":      docs,
        "timestamp": _now_iso(),
    })


@app.get(
    "/traffic/anomalies",
    summary="Recent anomaly detections",
    tags=["Traffic"],
)
def traffic_anomalies(
    limit: int = Query(20, ge=1, le=200, description="Number of anomaly records to return"),
):
    """
    Returns recently detected anomalies from the traffic stream.

    Anomalies are flagged by the Isolation Forest model when traffic
    behaviour deviates significantly from normal patterns — indicating
    possible accidents, sensor faults, or sudden incidents.

    Records are sorted by processing time (most recent first).
    """
    db   = get_db()
    docs = list(
        db[COL_PROCESSED]
        .find({"is_anomaly": True}, {"_id": 0})
        .sort("processed_at", -1)
        .limit(limit)
    )
    docs = [_clean(d) for d in docs]

    return JSONResponse({
        "status":    "ok",
        "count":     len(docs),
        "note":      "Flagged by Isolation Forest anomaly detector",
        "data":      docs,
        "timestamp": _now_iso(),
    })


@app.get(
    "/traffic/predict",
    summary="ML predictions on latest live records",
    tags=["Machine Learning"],
)
def traffic_predict(
    limit:   int = Query(20, ge=1, le=100, description="Number of records to score"),
    borough: str = Query(None, description="Filter records before scoring"),
):
    """
    Runs all three ML models on the most recent live traffic records and
    returns predictions alongside the observed data.

    Models used:
    - **Congestion Classifier** (Random Forest, 93.34% accuracy) — FREE_FLOW / MODERATE / CONGESTED / SEVERE
    - **Speed Predictor** (Random Forest Regressor, MAE 3.47 mph) — predicted speed in mph
    - **Anomaly Detector** (Isolation Forest) — flags unusual traffic behaviour

    Set `limit` to control how many records are scored (1–100).
    """
    predictor = _get_predictor()
    if predictor is None:
        raise HTTPException(
            status_code=503,
            detail="ML models not available. Run `python -m analytics.train` first.",
        )

    if borough and borough not in VALID_BOROUGHS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid borough '{borough}'. Choose from: {sorted(VALID_BOROUGHS)}",
        )

    query = {"borough": borough} if borough else {}
    db    = get_db()
    docs  = list(
        db[COL_PROCESSED]
        .find(query, {"_id": 0})
        .sort("processed_at", -1)
        .limit(limit)
    )

    if not docs:
        raise HTTPException(status_code=404, detail="No records found to score.")

    results = []
    for doc in docs:
        clean_doc = _clean(dict(doc))
        try:
            pred = predictor.predict(doc)
            results.append({
                "link_name":              clean_doc.get("link_name"),
                "borough":                clean_doc.get("borough"),
                "observed_speed":         clean_doc.get("speed"),
                "observed_congestion":    clean_doc.get("congestion_level"),
                "predicted_congestion":   pred["predicted_congestion"],
                "congestion_confidence":  pred["congestion_confidence"],
                "predicted_speed":        pred["predicted_speed"],
                "is_anomaly":             pred["is_anomaly"],
                "anomaly_score":          pred["anomaly_score"],
                "processed_at":           clean_doc.get("processed_at"),
            })
        except Exception as exc:
            results.append({
                "link_name":  clean_doc.get("link_name"),
                "borough":    clean_doc.get("borough"),
                "error":      str(exc),
            })

    return JSONResponse({
        "status":     "ok",
        "count":      len(results),
        "models":     {
            "classifier":  "Random Forest — 93.34% accuracy",
            "regressor":   "Random Forest — MAE 3.47 mph",
            "anomaly":     "Isolation Forest",
        },
        "data":       results,
        "timestamp":  _now_iso(),
    })


# ── Global exception handler ───────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "status":    "error",
            "detail":    str(exc),
            "timestamp": _now_iso(),
        },
    )
