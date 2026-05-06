# NYC Traffic Analytics Pipeline

**Big Data Analytics MSc · Capstone Project · All Phases Complete**

Real-time end-to-end traffic analytics pipeline pulling live data from the NYC Department of Transportation (DOT) every 60 seconds, streaming through Apache Kafka, processing with Apache Spark Structured Streaming, storing in MongoDB, predicting congestion with scikit-learn ML models, and visualising everything on a live Streamlit dashboard.

---

## Architecture

```
NYC DOT API  (every 60 seconds)
     │
     ▼
traffic_producer.py
     │  publishes to
     ▼
Kafka: traffic-raw  (5 partitions · 24h retention)
     │
     ▼
validation_consumer.py
     ├──► Kafka: traffic-validated  (valid records · 5 partitions · 7d retention)
     └──► Kafka: traffic-dlq        (rejected records · 1 partition · 3d retention)
                │
                ▼
     spark_stream_processor.py  (every 5 seconds)
                │  cleans · classifies · enriches
                ▼
     MongoDB: traffic_processed      ◄── individual records
                │
                ├──► batch_processor.py  (every 60 minutes)
                │         └──► MongoDB: traffic_aggregated
                │                       MongoDB: data_quality_metrics
                │                       MongoDB: data_lineage
                │
                └──► analytics/predictor.py  (real-time ML)
                           └──► congestion_classifier  (93.34% accuracy)
                                speed_predictor        (MAE 3.47 mph)
                                anomaly_detector       (Isolation Forest)
                                     │
                                     ▼
                           Streamlit Dashboard  :8501
                           REST API (FastAPI)   :8000
```

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.11 | Use the project virtual environment (`ve/`) |
| Apache Kafka | 3.x | Installed at `C:\kafka` |
| MongoDB | 6.x | Running locally on `localhost:27017` |
| Java (JDK) | 11 or 17 | Required by Kafka and Spark |
| HADOOP_HOME | Any | `winutils.exe` needed for Spark on Windows |

---

## Quick Start — Full Pipeline

Open **six separate terminals** and run one command per terminal, in order.

### Terminal 1 — Kafka

```bat
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

> Kafka must be running before any other component. Keep this terminal open.

### Terminal 2 — Producer + Consumer + Phase 1 Dashboard

```bash
cd nyc_traffic_pipeline
./scripts/run_pipeline.sh
```

This starts in the background:
- `producers/traffic_producer.py` — polls NYC DOT API every 60 s, publishes to `traffic-raw`
- `consumers/validation_consumer.py` — validates records, routes to `traffic-validated` or DLQ
- `monitoring/dashboard.py` — Phase 1 monitoring on **http://localhost:8502**

Stop with `./scripts/run_pipeline.sh stop`.

### Terminal 3 — Spark Stream Processor

```bash
cd nyc_traffic_pipeline
python -m processors.spark_stream_processor
```

Reads from `traffic-validated` every 5 seconds, cleans and enriches records, classifies congestion level, writes to MongoDB `traffic_processed`.

### Terminal 4 — Batch Processor (hourly loop)

```bash
cd nyc_traffic_pipeline
python -m processors.batch_processor --loop
```

Runs every 60 minutes: computes borough aggregations, quality metrics, and lineage records. Writes to `traffic_aggregated`, `data_quality_metrics`, `data_lineage`.

### Terminal 5 — Main Dashboard

```bash
cd nyc_traffic_pipeline
streamlit run monitoring/dashboard.py --server.port 8501
```

Opens at **http://localhost:8501** — live map, ML predictions, quality scores, lineage audit trail.

### Terminal 6 — REST API (optional)

```bash
cd nyc_traffic_pipeline
uvicorn api.traffic_api:app --reload --port 8000
```

REST API at **http://localhost:8000** · Interactive docs at **http://localhost:8000/docs**

---

## Verify the Pipeline

Run the end-to-end smoke test at any point:

```bash
python scripts/verify_pipeline.py
```

Checks: Kafka connectivity, topic existence, MongoDB connectivity, collection counts, model files, dashboard health.

---

## Individual Component Commands

### Create Kafka Topics (first run only)

```bash
python -c "from config.kafka_config import create_topics; create_topics()"
```

### Train ML Models

```bash
python -m analytics.train
```

Trains all three models on data in `traffic_processed`, saves `.joblib` files to `models/`.

### Run Batch Processor Once (no loop)

```bash
python -m processors.batch_processor
```

### Run Tests

```bash
pytest tests/
```

---

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `traffic-raw` | 5 | 24 hours | Full-fidelity backup of all API records |
| `traffic-validated` | 5 | 7 days | Schema-valid enriched records (Spark input) |
| `traffic-dlq` | 1 | 3 days | Dead Letter Queue — rejected records + error reason |

**Partition strategy:** Records routed by borough — Manhattan=0, Brooklyn=1, Queens=2, Bronx=3, Staten Island=4.

---

## MongoDB Collections

| Collection | Purpose |
|---|---|
| `traffic_processed` | Individual cleaned records written by Spark (every 5 s) |
| `traffic_aggregated` | Hourly borough summaries written by Batch Processor (every 60 min) |
| `data_quality_metrics` | Per-batch quality scores and field null rates |
| `data_lineage` | Complete audit trail of every processing run |

---

## Machine Learning Models

All models trained on ~309,530 records of real NYC traffic data.

| Model | Algorithm | Result |
|---|---|---|
| Congestion Classifier | Random Forest (200 trees) | **93.34% accuracy**, F1 0.936 |
| Speed Predictor | Random Forest Regressor | **MAE 3.47 mph** |
| Anomaly Detector | Isolation Forest | Flags accidents, sensor faults, sudden incidents |

Model files stored in `models/` as `.joblib`. Loaded live by the dashboard and API.

---

## Dashboard Tabs

| Tab | Content |
|---|---|
| Overview | KPI cards, avg speed vs 7-day baseline, worst borough, pipeline health banner |
| Live Traffic | Interactive NYC map coloured by congestion, speed histogram, live records table |
| ML Insights | Real-time predictions, anomaly alerts, feature importance, model explainability |
| Historical | Speed trends, hour × day heatmap, borough comparison |
| Data Quality | Quality score trends, valid vs invalid rate, null field tracking |
| Lineage & Health | Full audit trail of every batch, processing times, success rates |

---

## REST API Endpoints

Base URL: `http://localhost:8000`

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Pipeline health check |
| GET | `/traffic/current` | Latest records from MongoDB |
| GET | `/traffic/borough/{borough}` | Records filtered by borough |
| GET | `/traffic/congestion` | Current congestion summary by borough |
| GET | `/predictions/congestion` | Real-time ML congestion predictions |
| GET | `/analytics/summary` | 24-hour aggregated stats |
| GET | `/docs` | Interactive Swagger UI |

---

## Environment Variables

Copy `.env.example` to `.env` and set:

```env
# NYC Open Data — optional but raises rate limits
NYC_OPENDATA_APP_TOKEN=

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Kafka topics
TOPIC_TRAFFIC_RAW=traffic-raw
TOPIC_TRAFFIC_VALIDATED=traffic-validated
TOPIC_TRAFFIC_DLQ=traffic-dlq

# MongoDB
MONGO_URI=mongodb://localhost:27017
MONGO_DB=nyc_traffic

# Producer tuning
POLL_INTERVAL_SECONDS=60
BATCH_LIMIT=1000
```

---

## Project Structure

```
nyc_traffic_pipeline/
├── .env.example                 ← Copy to .env before running
├── requirements.txt
│
├── config/
│   ├── kafka_config.py          # Topic definitions, producer/consumer settings
│   └── schema.py                # JSON Schema validation + record enrichment
│
├── producers/
│   ├── nyc_dot_client.py        # NYC Open Data Socrata API client
│   └── traffic_producer.py      # Kafka producer — polls API every 60 s
│
├── consumers/
│   └── validation_consumer.py   # Validates raw records, routes to DLQ
│
├── processors/
│   ├── spark_stream_processor.py # Spark Structured Streaming — reads Kafka, writes MongoDB
│   ├── batch_processor.py        # Hourly aggregations and quality metrics
│   └── data_cleaner.py           # Shared cleaning/enrichment logic
│
├── analytics/
│   ├── train.py                  # Model training pipeline
│   ├── predictor.py              # Real-time inference (loaded by dashboard + API)
│   ├── evaluate.py               # Model evaluation and reports
│   └── feature_engineering.py   # Shared feature transforms
│
├── storage/
│   └── mongodb_client.py         # Connection helpers, retry logic, lineage, indexes
│
├── api/
│   └── traffic_api.py            # FastAPI REST API (port 8000)
│
├── monitoring/
│   ├── dashboard.py              # Streamlit live dashboard (port 8501)
│   └── data_quality_monitor.py   # Quality monitoring helpers
│
├── models/                       # Trained .joblib model files (git-ignored binaries)
│   ├── congestion_classifier.joblib
│   ├── speed_predictor.joblib
│   └── anomaly_detector.joblib
│
├── checkpoints/stream/           # Spark Structured Streaming checkpoint
├── logs/                         # producer.log, consumer.log, dashboard.log
│
├── scripts/
│   ├── start-kafka.bat           # Shortcut to start Kafka on Windows
│   ├── run_pipeline.sh           # Start/stop producer + consumer + Phase 1 dashboard
│   └── verify_pipeline.py        # End-to-end smoke test
│
└── tests/
    ├── test_schema.py
    └── test_nyc_dot_client.py
```

---

## Phase Completion Status

| Phase | Description | Status |
|---|---|---|
| Phase 1 | Data Collection & Streaming | ✅ Complete |
| Phase 2 | Processing & Storage (Spark + MongoDB) | ✅ Complete |
| Phase 3 | Machine Learning & Analytics | ✅ Complete |
| Phase 4 | Live Dashboard | ✅ Complete |
| Phase 5 | Integration & Testing | ✅ Complete |
| Phase 6 | Presentation & Documentation | In progress |

---

## Dataset

**Source:** NYC Open Data · DOT Traffic Speeds NBE  
**Dataset ID:** `i4gi-tjb9`  
**Endpoint:** `https://data.cityofnewyork.us/resource/i4gi-tjb9.json`  
**Update frequency:** Every ~60 seconds  
**Coverage:** ~200 road links across all 5 NYC boroughs  
**Auth:** Free — register at https://data.cityofnewyork.us/profile/app_tokens for higher rate limits
