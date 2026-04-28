# -*- coding: utf-8 -*-
"""
Compression module for review data.
Writes an optimized Polars DataFrame to ZSTD-compressed Parquet.
"""

from pathlib import Path
import polars as pl


def write_parquet(df: pl.DataFrame, output_path: Path, compression_level: int = 9) -> int:
    """
    Serializes a DataFrame to a ZSTD-compressed Parquet file.

    Args:
        df: DataFrame to write.
        output_path: Destination file path.
        compression_level: ZSTD level (1-22). Default 9.

    Returns:
        Size of the written Parquet file in bytes.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(
        output_path,
        compression="zstd",
        compression_level=compression_level,
    )

    return output_path.stat().st_size
