# -*- coding: utf-8 -*-
"""
Deduplication module for review data.
Reads the raw reviews CSV, removes duplicates by review ID, and returns
a clean DataFrame ready for optimization and compression.
"""

from pathlib import Path
import polars as pl


def load_and_deduplicate(csv_path: Path) -> tuple[pl.DataFrame, int]:
    """
    Reads a single raw reviews CSV and deduplicates by ``id_review``.

    All columns are ingested as Utf8 to avoid type-inference conflicts
    on mixed-format fields (e.g. dates, ratings with locale separators).

    Args:
        csv_path: Absolute path to the raw reviews CSV.

    Returns:
        A tuple: (deduplicated DataFrame, raw_row_count).
    """
    if not csv_path.exists():
        return pl.DataFrame(), 0

    try:
        df = pl.read_csv(
            str(csv_path),
            infer_schema_length=0,
            ignore_errors=True,
            encoding="utf8-lossy",
        )
    except Exception:
        return pl.DataFrame(), 0

    if df.is_empty():
        return pl.DataFrame(), 0

    raw_count = len(df)

    # Remove rows without a review ID
    if "id_review" in df.columns:
        df = df.filter(pl.col("id_review").is_not_null() & (pl.col("id_review") != ""))
        df = df.unique(subset=["id_review"])

    return df, raw_count
