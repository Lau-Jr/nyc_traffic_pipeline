"""
JSON Schema definitions and validation for NYC DOT traffic records.

NYC DOT Traffic Speeds NBE dataset fields (Socrata dataset i4gi-tjb9):
  id            - detector record ID
  speed         - average speed in mph (string in source)
  travel_time   - travel time in seconds (string in source)
  status        - 0=ok, 1=warning, 2=error
  data_as_of    - ISO8601 timestamp of measurement
  link_id       - road link identifier
  link_points   - space-separated lat,lon pairs
  encoded_poly_line      - Google-encoded polyline
  encoded_poly_line_lvls - zoom levels for encoded poly
  owner         - data owner (e.g. NYC_DOT_LIC)
  transcom_id   - TRANSCOM link ID
  borough       - NYC borough name
  link_name     - human-readable road segment name
"""

from jsonschema import validate, ValidationError
from typing import Dict, Any, Tuple
from loguru import logger


RAW_TRAFFIC_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "NYC DOT Traffic Speed Record",
    "type": "object",
    "required": ["speed", "data_as_of", "link_id", "borough", "link_name"],
    "properties": {
        "id": {"type": "string"},
        "speed": {
            "type": "string",
            "pattern": r"^\d+(\.\d+)?$",
            "description": "Average speed in mph"
        },
        "travel_time": {"type": "string"},
        "status": {
            "type": "string",
            # "enum": ["0", "1", "2"],
            "description": "0=normal, 1=warning, 2=error"
        },
        "data_as_of": {
            "type": "string",
            "description": "ISO8601 measurement timestamp"
        },
        "link_id": {"type": "string"},
        "link_points": {
            "type": "string",
            "description": "Space-separated lat,lon coordinate pairs"
        },
        "encoded_poly_line": {"type": "string"},
        "encoded_poly_line_lvls": {"type": "string"},
        "owner": {"type": "string"},
        "transcom_id": {"type": "string"},
        "borough": {
            "type": "string",
            "enum": ["Manhattan", "Brooklyn", "Queens",
                     "Bronx", "Staten Island"]
        },
        "link_name": {"type": "string"},
    },
    "additionalProperties": True,
}


def validate_record(record: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate a single traffic record against the schema.

    Returns:
        (True, "")              - record is valid
        (False, error_message)  - record failed validation
    """
    try:
        validate(instance=record, schema=RAW_TRAFFIC_SCHEMA)

        # Additional business-logic checks beyond JSON Schema
        speed = float(record.get("speed", 0))
        if not (0 <= speed <= 200):
            return False, f"Speed {speed} mph is out of plausible range (0-200)"

        if record.get("borough") and record["borough"] not in (
            "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"
        ):
            return False, f"Unknown borough: {record['borough']}"

        return True, ""

    except ValidationError as exc:
        return False, exc.message
    except (ValueError, TypeError) as exc:
        return False, str(exc)


def enrich_record(record: Dict[str, Any], ingested_at: str) -> Dict[str, Any]:
    """
    Add pipeline metadata fields to a validated record.
    Does not mutate the original dict.
    """
    enriched = dict(record)
    enriched["_pipeline"] = {
        "ingested_at": ingested_at,
        "source": "nyc_dot_speeds_nbe",
        "dataset_id": "i4gi-tjb9",
        "phase": "raw_ingestion",
        "speed_float": float(record.get("speed", 0)),
        "travel_time_int": int(record.get("travel_time", 0)),
        "status_int": int(record.get("status", 0)),
    }
    return enriched
