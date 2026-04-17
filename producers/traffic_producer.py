"""
Kafka Producer — NYC DOT Traffic Speed Records.

Responsibilities:
  1. Poll the NYC DOT API every POLL_INTERVAL_SECONDS seconds.
  2. Publish all records to 'traffic-raw' (full-fidelity backup).
  3. Emit stats for the monitoring dashboard.
  4. Track seen record IDs to avoid duplicate messages within a session.

Validation and routing to 'traffic-validated' / 'traffic-dlq' is handled
exclusively by the validation_consumer.
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(): pass
from kafka import KafkaProducer
try:
    from loguru import logger
except ImportError:
    import logging as _l; logger = _l.getLogger(__name__)

load_dotenv()

# Import from our own modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.kafka_config import get_producer_config, TOPICS
from producers.nyc_dot_client import NYCDOTClient

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
BATCH_LIMIT   = int(os.getenv("BATCH_LIMIT", "1000"))

# ── In-memory dedup cache (session-scoped) ───────────────────────────────────
_seen_ids: set = set()
_MAX_SEEN_CACHE = 50_000    # Evict oldest when cache grows too large


class ProducerStats:
    """Simple stats counter for the monitoring dashboard."""
    def __init__(self):
        self.reset()

    def reset(self):
        self.fetched    = 0
        self.published  = 0
        self.duplicates = 0
        self.errors     = 0
        self.cycle_start = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        elapsed = (datetime.now(timezone.utc) - self.cycle_start).total_seconds()
        return {
            "fetched":    self.fetched,
            "published":  self.published,
            "duplicates": self.duplicates,
            "errors":     self.errors,
            "elapsed_s":  round(elapsed, 2),
            "rate_per_min": round(self.published / max(elapsed / 60, 0.01), 1),
            "timestamp":  datetime.now(timezone.utc).isoformat(),
        }


stats = ProducerStats()


def _on_send_success(record_metadata):
    logger.debug(
        f"Sent → {record_metadata.topic} "
        f"[partition={record_metadata.partition}, "
        f"offset={record_metadata.offset}]"
    )


def _on_send_error(exc):
    logger.error(f"Kafka send error: {exc}")
    stats.errors += 1


def run_producer():
    """Main producer loop — polls API and publishes to Kafka indefinitely."""
    logger.info("=== NYC Traffic Pipeline — Producer Starting ===")

    client  = NYCDOTClient()
    cfg     = get_producer_config()
    producer = KafkaProducer(**cfg)

    raw_topic = TOPICS["traffic_raw"].name

    # Graceful shutdown on SIGINT / SIGTERM
    running = {"active": True}
    def _shutdown(sig, frame):
        logger.info("Shutdown signal received — flushing and exiting …")
        running["active"] = False
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ── Verify API is reachable before starting the loop ────────────────────
    logger.info("Checking NYC DOT API connectivity …")
    if not client.health_check():
        logger.error("NYC DOT API is unreachable. Check network and retry.")
        sys.exit(1)
    logger.success("NYC DOT API reachable — starting poll loop.")

    poll_count = 0
    while running["active"]:
        poll_start = time.time()
        poll_count += 1
        stats.reset()

        logger.info(f"── Poll #{poll_count} starting ──")

        try:
            records = client.fetch_latest(limit=BATCH_LIMIT)
            stats.fetched = len(records)
            logger.info(f"Fetched {len(records)} records from NYC DOT API.")

            for record in records:
                record_id = record.get("id") or record.get("link_id", "")
                key = f"{record_id}_{record.get('data_as_of', '')}"


                # ── 1. Dedup check ───────────────────────────────────────────
                if key and key in _seen_ids:
                    stats.duplicates += 1
                    continue

                # ── 2. Publish raw record — validation_consumer handles the rest
                producer.send(
                    raw_topic,
                    key=record_id,
                    value=json.dumps(record, default=str).encode("utf-8"),
                ).add_callback(_on_send_success).add_errback(_on_send_error)

                stats.published += 1

                # ── 3. Cache the ID to prevent future duplicates ─────────────
                if key:
                    if len(_seen_ids) > _MAX_SEEN_CACHE:
                        _seen_ids.clear()
                    _seen_ids.add(key)

        except Exception as exc:
            logger.exception(f"Unexpected error during poll #{poll_count}: {exc}")
            stats.errors += 1
        finally:
            producer.flush()

        # ── Log summary ──────────────────────────────────────────────────────
        s = stats.to_dict()
        logger.info(
            f"Poll #{poll_count} complete | "
            f"fetched={s['fetched']} "
            f"published={s['published']} "
            f"dupes={s['duplicates']} "
            f"rate={s['rate_per_min']} rec/min"
        )

        # ── Save stats snapshot for monitoring dashboard ─────────────────────
        stats_file = os.path.join(
            os.path.dirname(__file__), "..", "logs", "producer_stats.json"
        )
        os.makedirs(os.path.dirname(stats_file), exist_ok=True)
        with open(stats_file, "w") as f:
            json.dump(s, f, indent=2)

        # ── Wait for next poll ───────────────────────────────────────────────
        elapsed = time.time() - poll_start
        sleep_for = max(0, POLL_INTERVAL - elapsed)
        if running["active"]:
            logger.info(f"Sleeping {sleep_for:.1f}s until next poll …")
            time.sleep(sleep_for)

    producer.flush()
    producer.close()
    logger.info("Producer shut down cleanly.")


if __name__ == "__main__":
    run_producer()
