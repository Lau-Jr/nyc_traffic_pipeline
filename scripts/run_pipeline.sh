#!/usr/bin/env bash
# ============================================================
# run_pipeline.sh — Start all Phase 1 processes
# ============================================================
# Starts in the background:
#   1. traffic_producer.py   (polls API → publishes to Kafka)
#   2. validation_consumer.py (raw → validated / dlq)
#   3. monitoring dashboard   (Streamlit on port 8502)
#
# Usage:
#   chmod +x scripts/run_pipeline.sh
#   ./scripts/run_pipeline.sh
#
# Stop all:
#   ./scripts/run_pipeline.sh stop
# ============================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$SCRIPT_DIR/.."
PID_DIR="$ROOT/logs"
mkdir -p "$PID_DIR"

start() {
  echo "================================================"
  echo " NYC Traffic Pipeline — Phase 1 Starting"
  echo "================================================"

  # Activate venv if present
  if [ -f "$ROOT/venv/bin/activate" ]; then
    source "$ROOT/venv/bin/activate"
    echo "✓ Virtual environment activated"
  fi

  # 1. Traffic Producer
  echo ""
  echo "→ Starting Traffic Producer …"
  nohup python "$ROOT/producers/traffic_producer.py" \
    > "$PID_DIR/producer.log" 2>&1 &
  echo $! > "$PID_DIR/producer.pid"
  echo "  PID: $(cat "$PID_DIR/producer.pid") | Log: logs/producer.log"

  sleep 2

  # 2. Validation Consumer
  echo ""
  echo "→ Starting Validation Consumer …"
  nohup python "$ROOT/consumers/validation_consumer.py" \
    > "$PID_DIR/consumer.log" 2>&1 &
  echo $! > "$PID_DIR/consumer.pid"
  echo "  PID: $(cat "$PID_DIR/consumer.pid") | Log: logs/consumer.log"

  sleep 1

  # 3. Monitoring Dashboard
  echo ""
  echo "→ Starting Monitoring Dashboard …"
  nohup streamlit run "$ROOT/monitoring/dashboard.py" \
    --server.port 8502 \
    --server.headless true \
    > "$PID_DIR/dashboard.log" 2>&1 &
  echo $! > "$PID_DIR/dashboard.pid"
  echo "  PID: $(cat "$PID_DIR/dashboard.pid") | Log: logs/dashboard.log"

  echo ""
  echo "================================================"
  echo " All processes started."
  echo " Dashboard: http://localhost:8502"
  echo " Stop with: ./scripts/run_pipeline.sh stop"
  echo "================================================"
}

stop() {
  echo "Stopping Phase 1 pipeline …"
  for pidfile in "$PID_DIR"/producer.pid "$PID_DIR"/consumer.pid "$PID_DIR"/dashboard.pid; do
    if [ -f "$pidfile" ]; then
      PID=$(cat "$pidfile")
      if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "  Stopped PID $PID ($(basename "$pidfile" .pid))"
      fi
      rm -f "$pidfile"
    fi
  done
  echo "Done."
}

status() {
  echo "Phase 1 process status:"
  for name in producer consumer dashboard; do
    pidfile="$PID_DIR/${name}.pid"
    if [ -f "$pidfile" ]; then
      PID=$(cat "$pidfile")
      if kill -0 "$PID" 2>/dev/null; then
        echo "  ✓ $name  running (PID $PID)"
      else
        echo "  ✗ $name  dead (stale PID $PID)"
      fi
    else
      echo "  – $name  not started"
    fi
  done
}

case "${1:-start}" in
  start)  start ;;
  stop)   stop  ;;
  status) status ;;
  restart) stop; sleep 2; start ;;
  *) echo "Usage: $0 {start|stop|status|restart}" ;;
esac
