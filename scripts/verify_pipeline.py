"""
verify_pipeline.py — Phase 1 end-to-end smoke test.

Checks every component without needing Kafka running:
  1. NYC DOT API connectivity and schema of returned data
  2. Schema validation (valid + invalid records)
  3. Record enrichment
  4. Borough partition mapping
  5. Stats summary

Run with:  python scripts/verify_pipeline.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from loguru import logger

PASS = "✅"
FAIL = "❌"
results = []

def check(label: str, condition: bool, detail: str = ""):
    icon = PASS if condition else FAIL
    msg = f"{icon}  {label}"
    if detail:
        msg += f"  → {detail}"
    print(msg)
    results.append(condition)


print("\n" + "="*55)
print("  NYC Traffic Pipeline — Phase 1 Verification")
print("="*55 + "\n")


# ── 1. API Connectivity ──────────────────────────────────────────
print("[ 1/5 ] NYC DOT API")
from producers.nyc_dot_client import NYCDOTClient
client = NYCDOTClient()
try:
    records = client.fetch_latest(limit=10)
    check("API reachable", len(records) > 0, f"{len(records)} records returned")
    check("Records have required fields",
          all("speed" in r and "borough" in r and "link_name" in r for r in records),
          "speed, borough, link_name present")
    check("Speed values are numeric strings",
          all(r.get("speed", "").replace(".", "").isdigit() for r in records if r.get("speed")),
          "all speed values parseable as float")
    boroughs_seen = set(r.get("borough") for r in records if r.get("borough"))
    check("Borough field populated",
          len(boroughs_seen) > 0, f"boroughs: {', '.join(sorted(boroughs_seen))}")
except Exception as exc:
    check("API reachable", False, str(exc))
    check("Records have required fields", False, "skipped")
    check("Speed values are numeric strings", False, "skipped")
    check("Borough field populated", False, "skipped")


# ── 2. Schema Validation ─────────────────────────────────────────
print("\n[ 2/5 ] Schema Validation")
from config.schema import validate_record

valid_record = {
    "id": "434", "speed": "47.84", "travel_time": "66",
    "status": "0", "data_as_of": "2024-02-13T07:09:09.000",
    "link_id": "4616212", "link_points": "40.60,-74.18",
    "encoded_poly_line": "abc", "encoded_poly_line_lvls": "B",
    "owner": "NYC_DOT_LIC", "transcom_id": "4616212",
    "borough": "Staten Island", "link_name": "Test Road",
}
ok, _ = validate_record(valid_record)
check("Valid record passes validation", ok)

bad_speed = dict(valid_record, speed="999")
ok, msg = validate_record(bad_speed)
check("Speed 999 mph rejected", not ok, msg)

bad_borough = dict(valid_record, borough="New Jersey")
ok, msg = validate_record(bad_borough)
check("Unknown borough rejected", not ok, msg)

missing_speed = {k: v for k, v in valid_record.items() if k != "speed"}
ok, msg = validate_record(missing_speed)
check("Missing speed field rejected", not ok, msg)


# ── 3. Record Enrichment ─────────────────────────────────────────
print("\n[ 3/5 ] Record Enrichment")
from config.schema import enrich_record

enriched = enrich_record(valid_record, ingested_at="2024-02-13T07:10:00Z")
check("_pipeline metadata added", "_pipeline" in enriched)
check("speed_float cast correctly",
      enriched["_pipeline"]["speed_float"] == 47.84)
check("Original record not mutated",
      "_pipeline" not in valid_record)
check("Source dataset ID correct",
      enriched["_pipeline"]["dataset_id"] == "i4gi-tjb9")


# ── 4. Borough → Partition Mapping ──────────────────────────────
print("\n[ 4/5 ] Borough Partition Mapping")
from config.kafka_config import BOROUGH_PARTITION_MAP

boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
partitions = [BOROUGH_PARTITION_MAP[b] for b in boroughs]
check("All 5 boroughs mapped",  len(partitions) == 5)
check("Partitions are unique",  len(set(partitions)) == 5)
check("Partition range 0–4",    min(partitions) == 0 and max(partitions) == 4)


# ── 5. Config Loading ────────────────────────────────────────────
print("\n[ 5/5 ] Configuration")
from config.kafka_config import TOPICS
check("3 topics defined", len(TOPICS) == 3,
      str([t.name for t in TOPICS.values()]))
check("traffic-raw has 5 partitions",
      TOPICS["traffic_raw"].num_partitions == 5)
check("traffic-dlq has 1 partition",
      TOPICS["traffic_dlq"].num_partitions == 1)


# ── Summary ──────────────────────────────────────────────────────
passed = sum(results)
total  = len(results)
print("\n" + "="*55)
print(f"  Result: {passed}/{total} checks passed")
if passed == total:
    print("  🚦 Phase 1 verification PASSED — ready to run!")
else:
    print(f"  ⚠️  {total - passed} check(s) failed — review output above.")
print("="*55 + "\n")

sys.exit(0 if passed == total else 1)
