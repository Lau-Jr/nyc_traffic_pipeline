"""
Kafka configuration and topic management for NYC Traffic Pipeline.
Supports both local Kafka and Confluent Cloud.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(): pass

try:
    from loguru import logger
except ImportError:
    import logging as _l; logger = _l.getLogger(__name__)

load_dotenv()


@dataclass
class KafkaTopicConfig:
    name: str
    num_partitions: int
    replication_factor: int # How many copies of your data in different brokers
    config: Dict[str, str] = field(default_factory=dict)


TOPICS = {
    "traffic_raw": KafkaTopicConfig(
        name=os.getenv("TOPIC_TRAFFIC_RAW", "traffic-raw"),
        num_partitions=5,
        replication_factor=1,
        config={
            "retention.ms": str(24 * 60 * 60 * 1000), # deleted daily(segement wise)
            "segment.ms":   str(60 * 60 * 1000), # this controls rotations, archived hourly
        },
    ),
    "traffic_validated": KafkaTopicConfig(
        name=os.getenv("TOPIC_TRAFFIC_VALIDATED", "traffic-validated"),
        num_partitions=5,
        replication_factor=1,
        config={
            "retention.ms": str(7 * 24 * 60 * 60 * 1000), # retained for 7 days
            # Kafka uses its default segment.bytes=1GB as the only rotation trigger for this topic, 
            # so we can keep the segment.ms very high to avoid unnecessary rotations.
            "segment.ms":   str(24 * 60 * 60 * 1000),

        },
    ),
    "traffic_dlq": KafkaTopicConfig(
        name=os.getenv("TOPIC_TRAFFIC_DLQ", "traffic-dlq"),
        num_partitions=1,
        replication_factor=1,
        config={
            "retention.ms": str(72 * 60 * 60 * 1000),
             "segment.ms":   str(24 * 60 * 60 * 1000),
        },
    ),
}

BOROUGH_PARTITION_MAP = {
    "Manhattan":     0,
    "Brooklyn":      1,
    "Queens":        2,
    "Bronx":         3,
    "Staten Island": 4,
}


def get_producer_config() -> Dict[str, Any]:
    """Build KafkaProducer kwargs compatible with kafka-python 2.0.x."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

    config: Dict[str, Any] = {
        "bootstrap_servers": bootstrap,
        "acks": "all", # from all in-sync replicas, for strongest durability guarantee
        "retries": 5,
        "retry_backoff_ms": 500,
        "max_in_flight_requests_per_connection": 1, # wait for ack for each msg
        "compression_type": "gzip",
        "batch_size": 16384,
        "linger_ms": 500,
        "max_block_ms": 10000, # wait 10 ms before throwing buffer full error
        # record → [    buffer    ] → batch → broker
        "value_serializer": lambda v: v,
        "key_serializer": lambda k: k.encode("utf-8") if k else None,
        "api_version": (2, 8, 0),
    }

    if security_protocol in ("SASL_SSL", "SSL"):
        config.update({
            "security_protocol": security_protocol,
            "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            "sasl_plain_username": os.getenv("KAFKA_SASL_USERNAME", ""),
            "sasl_plain_password": os.getenv("KAFKA_SASL_PASSWORD", ""),
        })

    return config


def get_consumer_config(group_id: str) -> Dict[str, Any]:
    """Build KafkaConsumer kwargs compatible with kafka-python 2.0.x."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

    config: Dict[str, Any] = {
        "bootstrap_servers": bootstrap,
        "group_id": group_id,
        "auto_offset_reset": "earliest", # for  brand new consumer group,beginning, read everything, earliest only new msgs
        "enable_auto_commit": False, # Must manually commit after processing batch to avoid data loss on failure
        "max_poll_records": 500, # process 500 at time before committing offsets
        "session_timeout_ms": 30000, # if no heartbeat received in 30s, consider consumer dead and rebalance partitions to other consumers in the group
        "heartbeat_interval_ms": 10000,# tell kafka you are alive or else it will reassign ur partitions to other consumers in the group
        "value_deserializer": lambda v: v,
        "key_deserializer": lambda k: k.decode("utf-8") if k else None,
    }

    if security_protocol in ("SASL_SSL", "SSL"):
        config.update({
            "security_protocol": security_protocol,
            "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            "sasl_plain_username": os.getenv("KAFKA_SASL_USERNAME", ""),
            "sasl_plain_password": os.getenv("KAFKA_SASL_PASSWORD", ""),
        })

    return config


def create_topics() -> None:
    """Create all pipeline Kafka topics. Safe to call multiple times."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    producer_cfg = get_producer_config()
    admin_kwargs: Dict[str, Any] = {
        "bootstrap_servers": producer_cfg["bootstrap_servers"],
        "api_version": producer_cfg.get("api_version"),
    }
    if "security_protocol" in producer_cfg:
        admin_kwargs.update({
            "security_protocol":   producer_cfg["security_protocol"],
            "sasl_mechanism":      producer_cfg.get("sasl_mechanism"),
            "sasl_plain_username": producer_cfg.get("sasl_plain_username"),
            "sasl_plain_password": producer_cfg.get("sasl_plain_password"),
        })

    admin = KafkaAdminClient(**admin_kwargs)
    new_topics = [
        NewTopic(
            name=t.name,
            num_partitions=t.num_partitions,
            replication_factor=t.replication_factor,
            topic_configs=t.config,
        )
        for t in TOPICS.values()
    ]
    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        for t in TOPICS.values():
            logger.success(f"Created topic '{t.name}' "
                           f"(partitions={t.num_partitions})")
    except TopicAlreadyExistsError:
        logger.info("Topics already exist — skipping creation.")
    finally:
        admin.close()


if __name__ == "__main__":
    create_topics()