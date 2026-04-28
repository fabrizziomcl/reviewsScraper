# -*- coding: utf-8 -*-
"""
Schema optimization module for review data.
Casts string columns to native types for better Parquet compression.
"""

import polars as pl


def optimize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """
    Casts review columns to optimal types for storage and analytics.

    Transformations:
      - 'rating'       : Utf8 -> Float32
      - 'n_review_user': Utf8 -> Int32
      - 'place_id'     : Utf8 -> Categorical
      - 'username'     : Utf8 -> Categorical

    Columns that do not exist are silently skipped.

    Args:
        df: Input DataFrame with all-string columns.

    Returns:
        DataFrame with optimized column types.
    """
    existing = set(df.columns)
    expressions = []

    if "rating" in existing:
        expressions.append(pl.col("rating").cast(pl.Float32, strict=False))

    if "n_review_user" in existing:
        expressions.append(
            pl.when(pl.col("n_review_user") == "")
            .then(None)
            .otherwise(pl.col("n_review_user"))
            .cast(pl.Int32, strict=False)
            .alias("n_review_user")
        )

    if "place_id" in existing:
        expressions.append(pl.col("place_id").cast(pl.Categorical))

    if "username" in existing:
        expressions.append(pl.col("username").cast(pl.Categorical))

    if expressions:
        df = df.with_columns(expressions)

    return df
