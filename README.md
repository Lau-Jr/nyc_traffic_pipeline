# NYC Traffic Pipeline — Phase 1: Data Collection & Streaming

**Capstone Project · Big Data Analytics · Phase 1 of 6**

Real-time data ingestion pipeline pulling live traffic speed data from the
NYC Department of Transportation (DOT) via NYC Open Data (Socrata API),
streaming records through Apache Kafka with schema validation and a live
Streamlit monitoring dashboard.

---

## Quick Start (Docker — recommended)

Everything runs in containers. You only need **Docker Desktop** installed.

```bash
# 1. Clone / navigate to the project
cd nyc_traffic_pipeline

# 2. Create your environment file
cp .env.example .env
# Optionally add your NYC_OPENDATA_APP_TOKEN for higher rate limits

# 3. Start the entire stack
docker compose up --build

# 4. Open the dashboards
#    Streamlit monitoring:  http://localhost:8502
#    Kafka UI:              http://localhost:8080
```

That's it. Docker Compose starts Zookeeper, Kafka, creates the topics,
then starts the producer, consumer, and dashboard — in the correct order,
with health checks at each step.

### Useful commands

```bash
# Detached mode (runs in background)
docker compose up --build -d

# Tail logs for a specific service
docker compose logs -f producer
docker compose logs -f consumer

# Check all container statuses
docker compose ps

# Stop everything (keeps volumes)
docker compose down

# Stop and wipe all Kafka data (fresh start)
docker compose down -v

# Rebuild a single service after code change
docker compose up --build producer
```

---

## Architecture

```
┌─────────────────────────── docker network: nyc-traffic-net ───────────────────────────┐
│                                                                                        │
│   [zookeeper :2181] ──► [kafka :9092] ──► [kafka-ui :8080 → host:8080]               │
│                              │                                                         │
│                         [topic-init]  (runs once, creates topics, exits)              │
│                              │                                                         │
│              ┌───────────────┼────────────────┐                                       │
│              ▼               ▼                ▼                                       │
│         [producer]      [consumer]       [dashboard :8502 → host:8502]               │
│    polls NYC DOT API   validates + DLQ    Streamlit live monitor                      │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘

Host ports:  8080 (Kafka UI)  ·  8502 (Dashboard)  ·  29092 (Kafka direct, dev only)
```

### Containers

| Container | Image | Role |
|---|---|---|
| `zookeeper` | confluentinc/cp-zookeeper:7.5.3 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.5.3 | Message broker |
| `kafka-ui` | provectuslabs/kafka-ui | Web UI for topics/messages |
| `topic-init` | (project image) | Creates topics once on first boot |
| `producer` | (project image) | Polls NYC DOT API, publishes to Kafka |
| `consumer` | (project image) | Validates records, routes to DLQ |
| `dashboard` | (project image) | Streamlit monitoring dashboard |

---

## Dataset

**Source:** NYC Open Data · DOT Traffic Speeds NBE  
**Dataset ID:** `i4gi-tjb9`  
**Endpoint:** `https://data.cityofnewyork.us/resource/i4gi-tjb9.json`  
**Auth:** No API key required (register for an app token to lift rate limits)

| Field | Description |
|---|---|
| `speed` | Average speed in mph |
| `travel_time` | Travel time in seconds |
| `status` | 0=normal, 1=warning, 2=error |
| `data_as_of` | ISO8601 measurement timestamp |
| `link_id` | Road segment identifier |
| `link_points` | Space-separated lat,lon pairs |
| `borough` | NYC borough (used for Kafka partition routing) |
| `link_name` | Human-readable road segment name |

---

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `traffic-raw` | 5 | 24 hours | Full-fidelity backup of all API records |
| `traffic-validated` | 5 | 7 days | Schema-valid, enriched records (input for Phase 2) |
| `traffic-dlq` | 1 | 3 days | Dead Letter Queue — rejected records with error reason |

**Partition strategy:** Records routed by borough — Manhattan=0, Brooklyn=1,
Queens=2, Bronx=3, Staten Island=4.

---

## Running Without Docker (manual)

If you prefer running locally without containers:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start Kafka separately, then:
python -c "from config.kafka_config import create_topics; create_topics()"
python scripts/verify_pipeline.py   # smoke test
./scripts/run_pipeline.sh start      # start all processes
```

Set `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` in your `.env`.

---

## Project Structure

```
nyc_traffic_pipeline/
├── docker-compose.yml       ← Start here: docker compose up --build
├── Dockerfile               ← Multi-stage build for all app containers
├── .env.example             ← Copy to .env, add your app token
├── requirements.txt
├── config/
│   ├── kafka_config.py      # Topic definitions, producer/consumer configs
│   └── schema.py            # JSON Schema + validation + enrichment
├── producers/
│   ├── nyc_dot_client.py    # NYC Open Data Socrata API client
│   └── traffic_producer.py  # Kafka producer (poll loop)
├── consumers/
│   └── validation_consumer.py  # Validates raw records, routes DLQ
├── monitoring/
│   └── dashboard.py         # Streamlit live monitoring dashboard
├── scripts/
│   ├── setup_kafka.sh       # Manual topic creation (non-Docker)
│   ├── run_pipeline.sh      # Start/stop without Docker
│   └── verify_pipeline.py   # End-to-end smoke test
├── tests/
│   ├── test_schema.py
│   └── test_nyc_dot_client.py
└── logs/                    # Mounted into producer + dashboard containers
```

---

## Phase 1 Success Criteria

| Criterion | Status |
|---|---|
| 1000+ records/hour streamed | ✅ 1000 records/min default (configurable) |
| Data formatted and validated | ✅ JSON Schema + business rules |
| Kafka topics with partitioning strategy | ✅ 5 partitions by borough |
| Working ingestion pipeline | ✅ Producer + consumer + DLQ |
| Configuration documentation | ✅ `.env.example` + this README |
| Basic monitoring dashboard | ✅ Streamlit on :8502, Kafka UI on :8080 |

---

## Next: Phase 2 — Processing & Storage

Phase 2 will add a **Spark Structured Streaming** container consuming from
`traffic-validated`, computing 5-minute rolling window aggregates, and
persisting to **MongoDB Atlas**.
