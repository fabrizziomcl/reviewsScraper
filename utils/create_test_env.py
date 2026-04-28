# -*- coding: utf-8 -*-
"""
Creates a test subset from the full places CSV.
Extracts a random sample of N places for quick testing of the pipeline.

Usage:
    python utils/create_test_env.py
    python utils/create_test_env.py --source data/input/places_peru.csv --sample 50
"""

import argparse
import random
from pathlib import Path

import polars as pl


def create_test_sample(source_path: Path, output_path: Path, n_samples: int = 50):
    """
    Creates a small test CSV from the full places dataset.

    Prioritizes places that have reviews (non-null 'reviews' column) to
    ensure the test actually scrapes something useful.

    Args:
        source_path: Path to the full places CSV.
        output_path: Path for the test sample CSV.
        n_samples: Number of places to include in the sample.
    """
    df = pl.read_csv(str(source_path), infer_schema_length=0, ignore_errors=True)

    # Filter to places with valid URLs
    df = df.filter(pl.col("url_place").is_not_null() & (pl.col("url_place") != ""))

    # Prefer places with reviews
    if "reviews" in df.columns:
        with_reviews = df.filter(
            pl.col("reviews").is_not_null() & (pl.col("reviews") != "")
        )
        if len(with_reviews) >= n_samples:
            df = with_reviews

    # Random sample
    if len(df) > n_samples:
        indices = random.sample(range(len(df)), n_samples)
        df = df[indices]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)

    print(f"Test sample created: {output_path}")
    print(f"  Places: {len(df)}")
    print(f"  Columns: {df.columns}")


def main():
    parser = argparse.ArgumentParser(description="Create test environment")
    parser.add_argument(
        "--source", type=str, default="data/input/places_peru.csv",
        help="Path to the full places CSV",
    )
    parser.add_argument(
        "--sample", type=int, default=50,
        help="Number of places to sample (default: 50)",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    source = project_root / args.source
    output = project_root / "data" / "test" / "sample_places.csv"

    if not source.exists():
        print(f"Error: Source file not found: {source}")
        return

    create_test_sample(source, output, args.sample)


if __name__ == "__main__":
    main()
