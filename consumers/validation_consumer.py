"""
Kafka Consumer — Validation Layer.

Reads from 'traffic-raw', re-validates records, and routes them to:
  - traffic-validated  (good records)
  - traffic-dlq        (bad records with reason attached)

This consumer acts as a secondary validation gate, useful when multiple
producers may be writing to the raw topic.
"""

import json
import os
import sys
import signal
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from loguru import logger

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.kafka_config import get_consumer_config, get_producer_config, TOPICS
from config.schema import validate_record, enrich_record


def run_validation_consumer():
    logger.info("Starting Validation Consumer …")

    consumer_cfg = get_consumer_config(group_id="traffic-validator-v1")
    producer_cfg = get_producer_config()

    raw_topic       = TOPICS["traffic_raw"].name
    validated_topic = TOPICS["traffic_validated"].name
    dlq_topic       = TOPICS["traffic_dlq"].name

    consumer = KafkaConsumer(raw_topic, **consumer_cfg)
    producer = KafkaProducer(**producer_cfg)

    running = {"active": True}
    def _shutdown(sig, frame):
        running["active"] = False
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    processed = 0
    valid     = 0
    invalid   = 0

    while running["active"]:
        msg_batch = consumer.poll(timeout_ms=5000, max_records=500)

        for topic_partition, messages in msg_batch.items():
            for msg in messages:
                try:
                    record = json.loads(msg.value.decode("utf-8"))
                    ingested_at = datetime.now(timezone.utc).isoformat()

                    is_valid, error_msg = validate_record(record)
                    if is_valid:
                        enriched = enrich_record(record, ingested_at)
                        producer.send(
                            validated_topic,
                            key=msg.key,
                            value=json.dumps(enriched, default=str).encode("utf-8"),
                        )
                        valid += 1
                    else:
                        dlq_payload = {
                            "original": record,
                            "error": error_msg,
                            "rejected_at": ingested_at,
                            "source_offset": msg.offset,
                            "source_partition": msg.partition,
                        }
                        producer.send(
                            dlq_topic,
                            key=msg.key,
                            value=json.dumps(dlq_payload, default=str).encode("utf-8"),
                        )
                        invalid += 1

                    processed += 1
                    if processed % 500 == 0:
                        logger.info(
                            f"Processed={processed} "
                            f"valid={valid} invalid={invalid}"
                        )

                except (json.JSONDecodeError, KeyError) as exc:
                    logger.error(f"Failed to parse message: {exc}")
                    invalid += 1

            consumer.commit()

        producer.flush()

    consumer.close()
    producer.close()
    logger.info(f"Validation consumer shut down. "
                f"Total: processed={processed} valid={valid} invalid={invalid}")


if __name__ == "__main__":
    run_validation_consumer()
