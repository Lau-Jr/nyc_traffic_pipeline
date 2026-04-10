"""
Unit tests for the NYC DOT API client.
Uses mock responses to avoid real network calls.
Run with: pytest tests/test_nyc_dot_client.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import patch, MagicMock
from producers.nyc_dot_client import NYCDOTClient

SAMPLE_RESPONSE = [
    {
        "id": "434",
        "speed": "47.84",
        "travel_time": "66",
        "status": "0",
        "data_as_of": "2024-02-13T07:09:09.000",
        "link_id": "4616212",
        "link_points": "40.6020904,-74.1877 40.600331,-74.18943",
        "encoded_poly_line": "abyvFbxxcM",
        "encoded_poly_line_lvls": "BB",
        "owner": "NYC_DOT_LIC",
        "transcom_id": "4616212",
        "borough": "Staten Island",
        "link_name": "WSE N VICTORY BLVD - SOUTH AVENUE",
    },
    {
        "id": "351",
        "speed": "40.38",
        "travel_time": "133",
        "status": "0",
        "data_as_of": "2024-02-13T07:09:09.000",
        "link_id": "4616210",
        "link_points": "40.63092,-74.14592",
        "encoded_poly_line": "gv~vFtrpcM",
        "encoded_poly_line_lvls": "BB",
        "owner": "NYC_DOT_LIC",
        "transcom_id": "4616210",
        "borough": "Staten Island",
        "link_name": "Richmond Ave",
    },
]


class TestNYCDOTClient:

    def _mock_response(self, data, status_code=200):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = data
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    @patch("producers.nyc_dot_client.requests.Session")
    def test_fetch_latest_returns_records(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.get.return_value = self._mock_response(SAMPLE_RESPONSE)

        client = NYCDOTClient()
        records = client.fetch_latest(limit=10)

        assert len(records) == 2
        assert records[0]["id"] == "434"
        assert records[1]["borough"] == "Staten Island"

    @patch("producers.nyc_dot_client.requests.Session")
    def test_fetch_latest_passes_correct_params(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.get.return_value = self._mock_response(SAMPLE_RESPONSE)

        client = NYCDOTClient()
        client.fetch_latest(limit=500, borough="Manhattan")

        call_kwargs = mock_session.get.call_args
        params = call_kwargs[1]["params"]
        assert params["$limit"] == 500
        assert "Manhattan" in params["$where"]

    @patch("producers.nyc_dot_client.requests.Session")
    def test_health_check_returns_true_when_api_ok(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.get.return_value = self._mock_response([SAMPLE_RESPONSE[0]])

        client = NYCDOTClient()
        assert client.health_check() is True

    @patch("producers.nyc_dot_client.requests.Session")
    def test_health_check_returns_false_on_error(self, mock_session_cls):
        import requests
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.get.side_effect = requests.exceptions.ConnectionError("unreachable")

        client = NYCDOTClient()
        assert client.health_check() is False

    @patch("producers.nyc_dot_client.requests.Session")
    def test_paginated_fetch_stops_at_max_records(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        # Return 2 records per page
        mock_session.get.return_value = self._mock_response(SAMPLE_RESPONSE)

        client = NYCDOTClient()
        records = client.fetch_all_paginated(page_size=2, max_records=4)

        assert len(records) <= 4

    @patch("producers.nyc_dot_client.requests.Session")
    @patch("producers.nyc_dot_client.time.sleep")
    def test_retries_on_connection_error(self, mock_sleep, mock_session_cls):
        import requests
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        # Fail twice, succeed on third
        mock_session.get.side_effect = [
            requests.exceptions.ConnectionError("fail"),
            requests.exceptions.ConnectionError("fail"),
            self._mock_response(SAMPLE_RESPONSE),
        ]

        client = NYCDOTClient()
        records = client.fetch_latest(limit=10)

        assert len(records) == 2
        assert mock_session.get.call_count == 3
