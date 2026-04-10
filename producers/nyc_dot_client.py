"""
NYC DOT Traffic Speeds API Client (Socrata / NYC Open Data).

Dataset: DOT Traffic Speeds NBE
Endpoint: https://data.cityofnewyork.us/resource/i4gi-tjb9.json
Docs:     https://dev.socrata.com/foundry/data.cityofnewyork.us/i4gi-tjb9

The Socrata API supports SoQL (SQL-like) query parameters:
  $limit     - max records per request (default 1000, max 50000)
  $offset    - pagination offset
  $order     - sort order
  $where     - filter clause
  $select    - column projection
  $$app_token - optional rate-limit bypass token
"""

import os
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import requests
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

BASE_URL   = os.getenv("NYC_OPENDATA_BASE_URL", "https://data.cityofnewyork.us/resource")
DATASET_ID = os.getenv("NYC_DOT_SPEEDS_DATASET_ID", "i4gi-tjb9")
APP_TOKEN  = os.getenv("NYC_OPENDATA_APP_TOKEN", "")


class NYCDOTClient:
    """
    Thin wrapper around the NYC Open Data Socrata API for the
    DOT Traffic Speeds NBE dataset.
    """

    ENDPOINT = f"{BASE_URL}/{DATASET_ID}.json"
    REQUEST_TIMEOUT = 120         # seconds
    MAX_RETRIES     = 3
    RETRY_BACKOFF   = 2          # seconds (doubles each retry)

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "NYC-Traffic-Pipeline/1.0 (capstone-project)",
        })
        if APP_TOKEN:
            self.session.headers["X-App-Token"] = APP_TOKEN
            logger.debug("Socrata app token loaded.")
        else:
            logger.warning(
                "No NYC_OPENDATA_APP_TOKEN set. "
                "API calls are rate-limited to ~1000 req/hour per IP."
            )

    def fetch_latest(
        self,
        limit: int = 1000,
        borough: Optional[str] = None,
        order_by: str = "data_as_of DESC",
    ) -> List[Dict[str, Any]]:
        """
        Fetch the most recent traffic speed records.

        Args:
            limit:    Number of records to fetch (max 50000 per call).
            borough:  Filter to a specific borough, e.g. "Manhattan".
            order_by: SoQL ORDER BY clause.

        Returns:
            List of raw record dicts from the API.
        """
        params: Dict[str, Any] = {
            "$limit":  limit,
            "$order":  order_by,
        }
        if borough:
            params["$where"] = f"borough='{borough}'"

        return self._get(params)

    def fetch_all_paginated(
        self,
        page_size: int = 1000,
        max_records: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Page through the dataset and return up to max_records records.
        Useful for initial historical backfill.
        """
        all_records: List[Dict[str, Any]] = []
        offset = 0

        while len(all_records) < max_records:
            remaining = max_records - len(all_records)
            batch_size = min(page_size, remaining)

            params = {
                "$limit":  batch_size,
                "$offset": offset,
                "$order":  "data_as_of DESC",
            }
            batch = self._get(params)
            if not batch:
                break

            all_records.extend(batch)
            offset += len(batch)

            if len(batch) < batch_size:
                break           # Last page

            time.sleep(0.2)     # Be polite to the API

        logger.info(f"Paginated fetch complete: {len(all_records)} records.")
        return all_records

    def _get(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute a GET request with automatic retry and exponential backoff.
        """
        backoff = self.RETRY_BACKOFF
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self.session.get(
                    self.ENDPOINT,
                    params=params,
                    timeout=self.REQUEST_TIMEOUT,
                )
                resp.raise_for_status()

                records = resp.json()
                logger.debug(
                    f"API fetch: {len(records)} records "
                    f"(attempt {attempt}, params={params})"
                )
                return records

            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response else "?"
                if status == 429:
                    logger.warning(
                        f"Rate limited (429). Backing off {backoff}s …"
                    )
                    time.sleep(backoff)
                    backoff *= 2
                elif status == 403:
                    logger.error(
                        "403 Forbidden — check your app token or IP block."
                    )
                    raise
                else:
                    logger.error(f"HTTP {status} on attempt {attempt}: {exc}")
                    if attempt == self.MAX_RETRIES:
                        raise

            except requests.exceptions.ConnectionError as exc:
                logger.warning(
                    f"Connection error (attempt {attempt}): {exc}. "
                    f"Retrying in {backoff}s …"
                )
                if attempt == self.MAX_RETRIES:
                    raise
                time.sleep(backoff)
                backoff *= 2

            except requests.exceptions.Timeout:
                logger.warning(
                    f"Request timed out (attempt {attempt}). "
                    f"Retrying in {backoff}s …"
                )
                if attempt == self.MAX_RETRIES:
                    raise
                time.sleep(backoff)
                backoff *= 2

        return []

    def health_check(self) -> bool:
        """Return True if the API is reachable."""
        try:
            records = self._get({"$limit": 1})
            return len(records) > 0
        except Exception as exc:
            logger.error(f"Health check failed: {exc}")
            return False
