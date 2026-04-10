#!/usr/bin/env bash
# ============================================================
# setup_kafka.sh — Create Kafka topics for NYC Traffic Pipeline
# ============================================================
# Run AFTER Kafka broker is started.
# Works for both local Kafka and Confluent Cloud (set env vars).
#
# Usage:
#   chmod +x scripts/setup_kafka.sh
#   ./scripts/setup_kafka.sh
# ============================================================

set -euo pipefail

# Load .env if present
if [ -f "$(dirname "$0")/../.env" ]; then
  export $(grep -v '^#' "$(dirname "$0")/../.env" | xargs)
fi

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "============================================"
echo " NYC Traffic Pipeline — Kafka Topic Setup"
echo " Broker: $BROKER"
echo "============================================"

create_topic() {
  local NAME=$1
  local PARTITIONS=$2
  local RETENTION_MS=$3

  echo ""
  echo "→ Creating topic: $NAME (partitions=$PARTITIONS, retention=${RETENTION_MS}ms)"

  kafka-topics.sh \
    --bootstrap-server "$BROKER" \
    --create \
    --if-not-exists \
    --topic "$NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --config retention.ms="$RETENTION_MS" \
    --config segment.ms=3600000

  echo "  ✓ $NAME ready"
}

# traffic-raw: 5 partitions (one per borough), 24h retention
create_topic "${TOPIC_TRAFFIC_RAW:-traffic-raw}"       5 86400000

# traffic-validated: 5 partitions, 7-day retention
create_topic "${TOPIC_TRAFFIC_VALIDATED:-traffic-validated}" 5 604800000

# traffic-dlq: 1 partition, 3-day retention
create_topic "${TOPIC_TRAFFIC_DLQ:-traffic-dlq}"       1 259200000

echo ""
echo "============================================"
echo " Topic summary:"
kafka-topics.sh --bootstrap-server "$BROKER" --list | grep -E "traffic-"
echo "============================================"
echo " Setup complete."
