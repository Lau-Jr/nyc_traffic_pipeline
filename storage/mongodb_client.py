"""
Phase 2 – MongoDB Storage Layer

Manages connections, collection setup, and indexes for the NYC traffic pipeline.

Collections:
  traffic_processed   – Real-time stream-processed records
  traffic_aggregated  – Hourly/borough batch aggregations
  data_quality_metrics – Per-batch quality reports
  data_lineage        – Pipeline stage tracking
"""
import os

from loguru import logger
from pymongo import MongoClient, ASCENDING, DESCENDING, IndexModel
from pymongo.errors import ConnectionFailure

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB", "nyc_traffic")

# Collection names (importable by other modules)
COL_PROCESSED  = "traffic_processed"
COL_AGGREGATED = "traffic_aggregated"
COL_DQ_METRICS = "data_quality_metrics"
COL_LINEAGE    = "data_lineage"


# ── Connection ─────────────────────────────────────────────────────────────────

def get_client() -> MongoClient:
    """Return a connected MongoClient; raises ConnectionFailure on error."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")          # fast connectivity check
    return client


def get_db():
    """Return the nyc_traffic database handle."""
    return get_client()[MONGO_DB]


# ── Collection / Index Setup ───────────────────────────────────────────────────

def setup_collections(db=None):
    """
    Idempotently create indexes on all Phase 2 collections.
    Safe to call multiple times.
    """
    if db is None:
        db = get_db()

    # traffic_processed – queried by borough+time, link, congestion level
    _ensure_indexes(db[COL_PROCESSED], [
        IndexModel([("borough", ASCENDING), ("data_as_of_parsed", DESCENDING)]),
        IndexModel([("link_id", ASCENDING)]),
        IndexModel([("congestion_level", ASCENDING)]),
        IndexModel([("processed_at", DESCENDING)]),
        IndexModel([("hour_of_day", ASCENDING), ("borough", ASCENDING)]),
    ])

    # traffic_aggregated – queried by time window + borough
    _ensure_indexes(db[COL_AGGREGATED], [
        IndexModel([("window_start", DESCENDING)]),
        IndexModel([("borough", ASCENDING), ("window_start", DESCENDING)]),
        IndexModel([("aggregation_type", ASCENDING)]),
    ])

    # data_quality_metrics – queried by batch + time
    _ensure_indexes(db[COL_DQ_METRICS], [
        IndexModel([("batch_id", ASCENDING)], unique=True),
        IndexModel([("recorded_at", DESCENDING)]),
        IndexModel([("source", ASCENDING)]),
    ])

    # data_lineage – queried by record_id and stage
    _ensure_indexes(db[COL_LINEAGE], [
        IndexModel([("record_id", ASCENDING)]),
        IndexModel([("stage", ASCENDING), ("timestamp", DESCENDING)]),
    ])

    logger.info("MongoDB collections and indexes are ready")
    return db


def _ensure_indexes(collection, index_models):
    try:
        collection.create_indexes(index_models)
        logger.debug(f"Indexes ensured for collection: {collection.name}")
    except Exception as exc:
        logger.warning(f"Index creation warning for {collection.name}: {exc}")


# ── Health Check ───────────────────────────────────────────────────────────────

def ping() -> bool:
    """Return True if MongoDB is reachable."""
    try:
        get_client().admin.command("ping")
        return True
    except ConnectionFailure:
        return False
