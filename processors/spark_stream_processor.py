"""
Phase 2 – Spark Structured Streaming Processor

Consumes from Kafka `traffic-validated`, applies data cleaning and feature
engineering, then writes processed records to MongoDB in micro-batches of
≤5 seconds (meeting the Phase 2 latency requirement).

Also records per-batch data quality metrics to `data_quality_metrics`.

Run via spark-submit (handled automatically by Docker):
    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                 org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \\
      processors/spark_stream_processor.py

Or locally with pyspark installed and SPARK_PACKAGES set in env.
"""
import os
import sys
from datetime import datetime, timezone
from typing import Optional

# ── Windows: point Spark workers at the current venv Python ───────────────────
# Without this, Spark calls bare "python" which Windows aliases to the
# Microsoft Store installer instead of the actual venv interpreter.
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ── Windows: set HADOOP_HOME so Spark can find winutils.exe ───────────────────
# winutils.exe must exist at C:\hadoop\bin\winutils.exe
# Download from: github.com/cdarlint/winutils → hadoop-3.3.5/bin/
if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
if sys.platform == "win32":
    os.environ["hadoop.home.dir"] = os.environ.get("HADOOP_HOME", r"C:\hadoop")

from loguru import logger

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_SERVERS     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC       = os.getenv("TOPIC_TRAFFIC_VALIDATED",  "traffic-validated")
MONGO_URI         = os.getenv("MONGO_URI",                "mongodb://localhost:27017")
MONGO_DB          = os.getenv("MONGO_DB",                 "nyc_traffic")
MONGO_COL         = "traffic_processed"
DQ_COL            = "data_quality_metrics"
TRIGGER_SECS      = int(os.getenv("TRIGGER_INTERVAL_SECONDS", "5"))
CHECKPOINT_DIR    = os.getenv("CHECKPOINT_DIR",
                              os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                           "checkpoints", "stream"))
PROCESSOR_VERSION = "2.0.0"

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
)


# ── Spark Session ──────────────────────────────────────────────────────────────

def build_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("NYC-Traffic-Stream-Processor-v2")
        .master("local[*]")
        .config("spark.jars.packages",                   SPARK_PACKAGES)
        .config("spark.jars.ivy",                        os.path.expanduser("~/.ivy2-spark"))
        .config("spark.mongodb.write.connection.uri",    MONGO_URI)
        .config("spark.driver.memory",                   "1g")
        .config("spark.executor.memory",                 "1g")
        .config("spark.sql.shuffle.partitions",          "4")
        .config("spark.ui.enabled",                      "false")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.hadoop.fs.file.impl",             "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )


# ── Kafka Message Schema ───────────────────────────────────────────────────────

def get_raw_schema():
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, IntegerType,
    )

    pipeline_schema = StructType([
        StructField("ingested_at",     StringType()),
        StructField("source",          StringType()),
        StructField("dataset_id",      StringType()),
        StructField("phase",           StringType()),
        StructField("speed_float",     DoubleType()),
        StructField("travel_time_int", IntegerType()),
        StructField("status_int",      IntegerType()),
    ])

    return StructType([
        StructField("id",                      StringType()),
        StructField("speed",                   StringType()),
        StructField("travel_time",             StringType()),
        StructField("status",                  StringType()),
        StructField("data_as_of",              StringType()),
        StructField("link_id",                 StringType()),
        StructField("link_points",             StringType()),
        StructField("encoded_poly_line",       StringType()),
        StructField("encoded_poly_line_lvls",  StringType()),
        StructField("owner",                   StringType()),
        StructField("transcom_id",             StringType()),
        StructField("borough",                 StringType()),
        StructField("link_name",               StringType()),
        StructField("_pipeline",               pipeline_schema),
    ])


# ── Streaming DataFrame ────────────────────────────────────────────────────────
# All transformations use native Spark SQL functions (no Python UDFs) so that
# no Python worker sub-processes are spawned — avoids Windows socket-timeout
# issues caused by spaces in the project path.

def build_streaming_df(spark):
    from pyspark.sql.functions import (
        col, from_json, lit, current_timestamp,
        to_timestamp, hour, dayofweek,
        when, split, trim, round as spark_round,
    )
    from pyspark.sql.types import DoubleType

    raw_schema = get_raw_schema()

    # Read raw bytes from Kafka.
    #
    # failOnDataLoss=false: tolerates partition count changes or topic
    # recreation between runs (e.g. local dev / topic reset).  Data that
    # was aged out of Kafka is silently skipped rather than crashing the
    # stream.  For production with strict SLAs, set to "true" and ensure
    # Kafka retention >= LOOKBACK_HOURS.
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "earliest")
        .option("failOnDataLoss",          "false")
        .option("maxOffsetsPerTrigger",    "10000")
        .load()
    )

    # Parse JSON value
    parsed = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), raw_schema).alias("d"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
        .select("d.*", "kafka_timestamp", "kafka_partition", "kafka_offset")
    )

    # Convenience aliases
    speed_col = col("_pipeline.speed_float")
    ts_col    = to_timestamp(col("data_as_of"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    hour_col  = hour(ts_col)
    dow_col   = dayofweek(ts_col)   # 1=Sun … 7=Sat in Spark
    is_weekend = (dow_col == 1) | (dow_col == 7)

    # Native congestion_level (replaces Python UDF)
    congestion_level_col = (
        when(speed_col.isNull(), "UNKNOWN")
        .when(speed_col >= 35, "FREE_FLOW")
        .when(speed_col >= 15, "MODERATE")
        .when(speed_col >= 5,  "CONGESTED")
        .otherwise("SEVERE")
    )

    # Native congestion_score: 1.0 - min(speed/60, 1.0), rounded to 4dp
    congestion_score_col = when(
        speed_col.isNotNull(),
        spark_round(lit(1.0) - when(speed_col >= 60, lit(1.0)).otherwise(speed_col / 60.0), 4)
    ).otherwise(None)

    # Native lat/lon extraction from "lat,lon lat,lon ..." string
    first_pair  = split(trim(col("link_points")), " ").getItem(0)
    lat_col     = split(first_pair, ",").getItem(0).cast(DoubleType())
    lon_col     = split(first_pair, ",").getItem(1).cast(DoubleType())

    # Native time_bucket (replaces Python UDF)
    time_bucket_col = (
        when(hour_col.between(0, 5), "OVERNIGHT")
        .when(hour_col.between(7, 8)  & ~is_weekend, "MORNING_PEAK")
        .when(hour_col.between(17, 18) & ~is_weekend, "EVENING_PEAK")
        .otherwise("OFF_PEAK")
    )

    enriched = (
        parsed
        # Cast measurements
        .withColumn("speed",        speed_col)
        .withColumn("travel_time",  col("_pipeline.travel_time_int"))
        .withColumn("status",       col("_pipeline.status_int"))
        # Parse timestamp
        .withColumn("data_as_of_parsed", ts_col)
        # Time features
        .withColumn("hour_of_day",  hour_col)
        .withColumn("day_of_week",  dow_col)
        .withColumn("is_weekend",   is_weekend)
        .withColumn("is_peak_hour",
            ((hour_col.between(7, 8)) | (hour_col.between(17, 18))) & ~is_weekend
        )
        .withColumn("time_bucket",  time_bucket_col)
        # Location
        .withColumn("latitude",  lat_col)
        .withColumn("longitude", lon_col)
        # Congestion
        .withColumn("congestion_level", congestion_level_col)
        .withColumn("congestion_score", congestion_score_col)
        # Lineage
        .withColumn("processed_at",       current_timestamp())
        .withColumn("phase",              lit("stream_processing"))
        .withColumn("processor_version",  lit(PROCESSOR_VERSION))
        # Drop large/redundant raw columns
        .drop("_pipeline", "encoded_poly_line", "encoded_poly_line_lvls")
    )

    return enriched


# ── Batch Write Callback ───────────────────────────────────────────────────────
#
# Each foreachBatch invocation produces three writes:
#   1. Processed records  → traffic_processed       (Mongo Spark Connector)
#   2. Quality metrics    → data_quality_metrics    (PyMongo, with retry)
#   3. Lineage entry      → data_lineage            (PyMongo, with retry)
#
# All writes that go through PyMongo use exponential-backoff retry.  The
# Mongo Spark Connector handles its own retries internally per the
# `retryWrites=true` URI flag.  Timestamps are stored as native BSON
# datetime (NOT ISO strings) so they sort and compare correctly in MongoDB.

TRANSFORMATIONS_APPLIED = [
    "kafka_json_parse",
    "type_cast_numeric",
    "extract_timestamp",
    "extract_time_features",
    "extract_lat_lon",
    "compute_congestion_level",
    "compute_congestion_score",
    "compute_time_bucket",
    "attach_processing_lineage",
]


def write_batch(batch_df, batch_id: int):
    """
    foreachBatch handler — write processed records + quality metrics +
    lineage entry, all with retry semantics.  Captures Kafka offset range
    for full provenance.
    """
    started_at = datetime.now(timezone.utc)
    count = batch_df.count()

    if count == 0:
        logger.debug(f"Batch {batch_id}: empty, skipping")
        return

    logger.info(f"Batch {batch_id}: writing {count:,} records → MongoDB")

    # ── Capture Kafka offset range for lineage (before drops) ─────────────
    offset_range = _capture_offset_range(batch_df)

    # ── 1. Write processed records (retried internally by connector) ──────
    write_status = "success"
    write_errors: list[str] = []
    try:
        (
            batch_df.write
            .format("mongodb")
            .mode("append")
            .option("database",   MONGO_DB)
            .option("collection", MONGO_COL)
            .save()
        )
    except Exception as exc:
        write_status = "failed"
        write_errors.append(f"records_write: {type(exc).__name__}: {exc}")
        logger.error(f"Batch {batch_id}: record write failed: {exc}")
        # Continue to record lineage even on partial failure — provenance
        # of the failure itself is valuable for debugging.

    # ── 2. Quality metrics (retried) ──────────────────────────────────────
    quality_summary = None
    try:
        quality_summary = _record_quality_metrics(batch_df, batch_id, count, started_at)
    except Exception as exc:
        write_errors.append(f"quality_metrics: {type(exc).__name__}: {exc}")
        logger.warning(f"Batch {batch_id}: quality metrics failed: {exc}")

    # ── 3. Lineage entry (retried, never raises) ──────────────────────────
    completed_at = datetime.now(timezone.utc)
    _record_lineage_entry(
        batch_id=batch_id,
        record_count=count,
        offset_range=offset_range,
        started_at=started_at,
        completed_at=completed_at,
        status=write_status if not write_errors else "partial",
        errors=write_errors,
        quality_summary=quality_summary,
    )

    duration_ms = int((completed_at - started_at).total_seconds() * 1000)
    if write_status == "success" and not write_errors:
        logger.info(f"Batch {batch_id}: OK in {duration_ms}ms")
    else:
        logger.warning(f"Batch {batch_id}: completed with errors in {duration_ms}ms — see lineage")


def _capture_offset_range(df) -> dict:
    """
    Extract min/max Kafka offset per partition for this micro-batch.
    Used in the lineage entry so any record range can be replayed deterministically.
    """
    from pyspark.sql.functions import min as spark_min, max as spark_max, count as spark_count

    try:
        offsets = (
            df.groupBy("kafka_partition")
            .agg(
                spark_min("kafka_offset").alias("min_offset"),
                spark_max("kafka_offset").alias("max_offset"),
                spark_count("kafka_offset").alias("count"),
            )
            .collect()
        )
        return {
            f"partition_{row['kafka_partition']}": {
                "min_offset": int(row["min_offset"]),
                "max_offset": int(row["max_offset"]),
                "count":      int(row["count"]),
            }
            for row in offsets
        }
    except Exception as exc:
        logger.warning(f"Could not capture offset range: {exc}")
        return {}


def _record_quality_metrics(df, batch_id: int, total: int, started_at: datetime) -> dict:
    """
    Compute quality stats and upsert into data_quality_metrics.
    Returns a brief summary used in the lineage entry.
    """
    from pyspark.sql.functions import col
    from storage.mongodb_client import write_with_retry, get_client

    df_cached = df.cache()
    null_speed   = df_cached.filter(col("speed").isNull()).count()
    null_borough = df_cached.filter(col("borough").isNull()).count()
    null_ts      = df_cached.filter(col("data_as_of_parsed").isNull()).count()
    zero_speed   = df_cached.filter(col("speed") == 0).count()
    unknown_cong = df_cached.filter(col("congestion_level") == "UNKNOWN").count()

    borough_dist = {
        row["borough"]: row["count"]
        for row in df_cached.groupBy("borough").count().collect()
        if row["borough"]
    }
    congestion_dist = {
        row["congestion_level"]: row["count"]
        for row in df_cached.groupBy("congestion_level").count().collect()
    }
    df_cached.unpersist()

    valid = total - null_speed - null_borough - null_ts
    quality_score = round(valid / total * 100, 2) if total > 0 else 0.0

    metric = {
        "batch_id":       f"stream_{batch_id}",
        "source":         "spark_stream_processor",
        "recorded_at":    started_at,                 # native datetime, not string
        "total_records":  total,
        "valid_records":  valid,
        "quality_score":  quality_score,
        "null_counts": {
            "speed":      null_speed,
            "borough":    null_borough,
            "data_as_of": null_ts,
        },
        "anomalies": {
            "unknown_congestion": unknown_cong,
            "zero_speed":         zero_speed,
        },
        "borough_distribution":    borough_dist,
        "congestion_distribution": congestion_dist,
    }

    client = get_client()
    try:
        write_with_retry(
            client[MONGO_DB][DQ_COL].update_one,
            {"batch_id": metric["batch_id"]},
            {"$set": metric},
            upsert=True,
            op_name="quality_metrics_upsert",
        )
    finally:
        client.close()

    return {
        "quality_score": quality_score,
        "valid_records": valid,
        "null_speed":    null_speed,
        "zero_speed":    zero_speed,
    }


def _record_lineage_entry(
    *, batch_id: int, record_count: int, offset_range: dict,
    started_at: datetime, completed_at: datetime,
    status: str, errors: list, quality_summary: Optional[dict] = None,
):
    """Record a structured lineage entry for this micro-batch."""
    from storage.mongodb_client import (
        get_client, make_lineage_entry, record_lineage,
    )

    entry = make_lineage_entry(
        phase             = "stream_processing",
        processor         = "spark_stream_processor",
        processor_version = PROCESSOR_VERSION,
        source = {
            "type":             "kafka",
            "topic":            KAFKA_TOPIC,
            "bootstrap_servers": KAFKA_SERVERS,
            "partition_offsets": offset_range,
        },
        destination = {
            "type":       "mongodb",
            "database":   MONGO_DB,
            "collection": MONGO_COL,
        },
        record_count    = record_count,
        started_at      = started_at,
        completed_at    = completed_at,
        transformations = TRANSFORMATIONS_APPLIED,
        schema_version  = "1.0",
        status          = status,
        errors          = errors,
        extra = {
            "spark_batch_id":   int(batch_id),
            "trigger_seconds":  TRIGGER_SECS,
            "quality_summary":  quality_summary,
        },
    )

    client = get_client()
    try:
        record_lineage(client[MONGO_DB], entry)
    finally:
        client.close()


# ── Entry Point ────────────────────────────────────────────────────────────────

def main():
    logger.info("═══════════════════════════════════════════════")
    logger.info(" NYC Traffic – Spark Stream Processor  v2.0   ")
    logger.info("═══════════════════════════════════════════════")
    logger.info(f"Kafka     : {KAFKA_SERVERS}  topic={KAFKA_TOPIC}")
    logger.info(f"MongoDB   : {MONGO_URI}/{MONGO_DB}.{MONGO_COL}")
    logger.info(f"Trigger   : every {TRIGGER_SECS}s")
    logger.info(f"Checkpoint: {CHECKPOINT_DIR}")

    # Ensure MongoDB collections/indexes exist
    from storage.mongodb_client import setup_collections, get_db
    setup_collections(get_db())

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    enriched = build_streaming_df(spark)

    query = (
        enriched.writeStream
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    logger.info("Stream processor running — Ctrl+C to stop")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
