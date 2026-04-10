"""
Unit tests for schema validation and record enrichment.
Run with: pytest tests/test_schema.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from config.schema import validate_record, enrich_record

# ── Fixtures ─────────────────────────────────────────────────────────────────

VALID_RECORD = {
    "id": "434",
    "speed": "47.84",
    "travel_time": "66",
    "status": "0",
    "data_as_of": "2024-02-13T07:09:09.000",
    "link_id": "4616212",
    "link_points": "40.6020904,-74.1877 40.600331,-74.18943",
    "encoded_poly_line": "abyvFbxxcM~IxI",
    "encoded_poly_line_lvls": "BBBBBB",
    "owner": "NYC_DOT_LIC",
    "transcom_id": "4616212",
    "borough": "Staten Island",
    "link_name": "WSE N VICTORY BLVD - SOUTH AVENUE",
}


# ── Validation tests ─────────────────────────────────────────────────────────

class TestValidation:

    def test_valid_record_passes(self):
        is_valid, msg = validate_record(VALID_RECORD)
        assert is_valid is True
        assert msg == ""

    def test_missing_required_field_fails(self):
        bad = dict(VALID_RECORD)
        del bad["speed"]
        is_valid, msg = validate_record(bad)
        assert is_valid is False
        assert "speed" in msg.lower() or msg != ""

    def test_missing_borough_fails(self):
        bad = dict(VALID_RECORD)
        del bad["borough"]
        is_valid, msg = validate_record(bad)
        assert is_valid is False

    def test_invalid_borough_fails(self):
        bad = dict(VALID_RECORD)
        bad["borough"] = "New Jersey"
        is_valid, msg = validate_record(bad)
        assert is_valid is False

    def test_speed_out_of_range_fails(self):
        bad = dict(VALID_RECORD)
        bad["speed"] = "999"
        is_valid, msg = validate_record(bad)
        assert is_valid is False
        assert "speed" in msg.lower() or "range" in msg.lower()

    def test_negative_speed_invalid(self):
        bad = dict(VALID_RECORD)
        bad["speed"] = "-5"
        is_valid, msg = validate_record(bad)
        assert is_valid is False

    def test_non_numeric_speed_fails(self):
        bad = dict(VALID_RECORD)
        bad["speed"] = "fast"
        is_valid, msg = validate_record(bad)
        assert is_valid is False

    def test_zero_speed_is_valid(self):
        """A sensor reading of 0 mph (standstill) is valid."""
        record = dict(VALID_RECORD)
        record["speed"] = "0"
        is_valid, _ = validate_record(record)
        assert is_valid is True

    def test_all_five_boroughs_are_valid(self):
        boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
        for borough in boroughs:
            record = dict(VALID_RECORD)
            record["borough"] = borough
            is_valid, msg = validate_record(record)
            assert is_valid is True, f"Borough '{borough}' should be valid: {msg}"

    def test_invalid_status_value_fails(self):
        bad = dict(VALID_RECORD)
        bad["status"] = "9"
        is_valid, _ = validate_record(bad)
        assert is_valid is False


# ── Enrichment tests ─────────────────────────────────────────────────────────

class TestEnrichment:

    def test_enrichment_adds_pipeline_metadata(self):
        enriched = enrich_record(VALID_RECORD, ingested_at="2024-02-13T07:10:00Z")
        assert "_pipeline" in enriched
        meta = enriched["_pipeline"]
        assert meta["source"] == "nyc_dot_speeds_nbe"
        assert meta["dataset_id"] == "i4gi-tjb9"
        assert meta["phase"] == "raw_ingestion"

    def test_enrichment_casts_speed_to_float(self):
        enriched = enrich_record(VALID_RECORD, ingested_at="2024-02-13T07:10:00Z")
        assert isinstance(enriched["_pipeline"]["speed_float"], float)
        assert enriched["_pipeline"]["speed_float"] == pytest.approx(47.84)

    def test_enrichment_does_not_mutate_original(self):
        original = dict(VALID_RECORD)
        _ = enrich_record(VALID_RECORD, ingested_at="now")
        assert original == VALID_RECORD

    def test_ingested_at_is_stored(self):
        ts = "2024-02-13T07:10:00+00:00"
        enriched = enrich_record(VALID_RECORD, ingested_at=ts)
        assert enriched["_pipeline"]["ingested_at"] == ts
