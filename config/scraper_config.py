# -*- coding: utf-8 -*-
"""
Centralized configuration for the reviews scraper pipeline.
All tunable constants live here to avoid magic numbers in the codebase.
"""

# ── Orchestrator ──────────────────────────────────────────────────────────────
DEFAULT_WORKERS = 8
MAX_REVIEWS_PER_PLACE = 15_000
DEFAULT_SORT_BY = "newest"

# ── Rate Limiting / Anti-Bot ──────────────────────────────────────────────────
DELAY_BETWEEN_PLACES_MIN = 2      # seconds (random uniform between min and max)
DELAY_BETWEEN_PLACES_MAX = 5
WORKER_RETRY_MAX = 2              # retries per place before skipping
WORKER_RETRY_BACKOFF_BASE = 5     # seconds; actual = base * 2^attempt
PLACE_TIMEOUT = 180               # max seconds per place before giving up

# ── Output ────────────────────────────────────────────────────────────────────
RAW_OUTPUT_FILENAME = "reviews_raw.csv"
COMPLETED_PLACES_FILENAME = "completed_places.txt"

# CSV header for raw review output
REVIEW_HEADER = [
    "place_id", "id_review", "caption", "relative_date", "review_date",
    "retrieval_date", "rating", "username", "n_review_user",
    "n_photo_user", "url_user", "url_source",
]

# ── Paths (relative to project root) ─────────────────────────────────────────
DEFAULT_INPUT_FILE = "data/input/places_peru.csv"
DEFAULT_OUTPUT_DIR = "data/output"
DEFAULT_PARQUET_DIR = "data_parquet"
DEFAULT_TEST_DIR = "data/test"

# ── ETL ───────────────────────────────────────────────────────────────────────
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 9
DEDUP_COLUMN = "id_review"
