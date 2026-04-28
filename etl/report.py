# -*- coding: utf-8 -*-
"""
Reporting module for the reviews ETL pipeline.
Formats byte sizes and builds summary strings identical to the mapScraper style.
"""


def format_bytes(size_bytes: int) -> str:
    """Converts a byte count to a human-readable string (KB, MB, GB)."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.2f} GB"


def processing_summary(label: str, raw_rows: int, unique_rows: int, csv_bytes: int, parquet_bytes: int) -> str:
    """
    Builds a one-line summary string for a processing step.

    Args:
        label: Name/label for this step.
        raw_rows: Number of raw input rows.
        unique_rows: Number of unique rows after dedup.
        csv_bytes: Size of the CSV output in bytes.
        parquet_bytes: Size of the Parquet output in bytes.

    Returns:
        Formatted summary string.
    """
    if csv_bytes > 0:
        ratio = (1 - parquet_bytes / csv_bytes) * 100
    else:
        ratio = 0.0

    return (
        f"  {label:<25} | "
        f"Raw: {raw_rows:>10,} -> Unique: {unique_rows:>10,} | "
        f"CSV: {format_bytes(csv_bytes):>10} -> "
        f"Parquet: {format_bytes(parquet_bytes):>10} | "
        f"Reduction: {ratio:5.1f}%"
    )
