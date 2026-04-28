# -*- coding: utf-8 -*-
"""
Reviews ETL Pipeline.
Reads the raw reviews CSV produced by the orchestrator, deduplicates,
optimizes schema, and writes to ZSTD-compressed Parquet + CSV.
Generates a JSON report with processing statistics.

Usage:
    python -m etl.pipeline
    python -m etl.pipeline --input data/output/reviews_raw.csv --output-dir data_parquet
"""

import time
import argparse
import json
from pathlib import Path

import polars as pl

from etl.dedup import load_and_deduplicate
from etl.optimize import optimize_schema
from etl.compress import write_parquet
from etl.report import format_bytes, processing_summary


def run_pipeline(input_csv: Path, output_dir: Path):
    """
    Runs the full ETL pipeline on the raw reviews CSV.

    Steps:
      1. Load and deduplicate by id_review.
      2. Optimize column types.
      3. Write to Parquet + CSV.
      4. Generate JSON report.

    Args:
        input_csv: Path to the raw reviews CSV.
        output_dir: Root output directory for processed files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("REVIEWS ETL PIPELINE")
    print(f"  Input:   {input_csv.resolve()}")
    print(f"  Output:  {output_dir.resolve()}")
    print(f"  Engine:  Polars (multithreaded)")
    print("=" * 80)

    if not input_csv.exists():
        print(f"  [ERROR] Input file not found: {input_csv}")
        return

    overall_start = time.time()
    input_size = input_csv.stat().st_size

    # Step 1: Load and deduplicate
    print(f"\n  [1/3] Loading and deduplicating reviews...")
    df, raw_count = load_and_deduplicate(input_csv)

    if df.is_empty():
        print("  [ERROR] No valid reviews found after deduplication.")
        return

    unique_count = len(df)
    print(f"         Raw records:    {raw_count:,}")
    print(f"         Unique reviews: {unique_count:,}")
    print(f"         Duplicates removed: {raw_count - unique_count:,}")

    # Step 2: Optimize schema
    print(f"\n  [2/3] Optimizing schema...")
    df = optimize_schema(df)

    # Step 3: Write outputs
    print(f"\n  [3/3] Writing outputs...")
    peru_dir = output_dir / "Peru"
    peru_dir.mkdir(parents=True, exist_ok=True)

    # Parquet
    parquet_path = peru_dir / "reviews_peru.parquet"
    parquet_size = write_parquet(df, parquet_path)
    print(f"         Parquet: {parquet_path.name} ({format_bytes(parquet_size)})")

    # CSV
    csv_path = peru_dir / "reviews_peru.csv"
    df.write_csv(csv_path)
    csv_output_size = csv_path.stat().st_size
    print(f"         CSV:     {csv_path.name} ({format_bytes(csv_output_size)})")

    overall_elapsed = time.time() - overall_start

    # Count unique places
    unique_places = 0
    if "place_id" in df.columns:
        unique_places = df.select("place_id").n_unique()

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print(f"  Total raw records:          {raw_count:,}")
    print(f"  Unique reviews:             {unique_count:,}")
    print(f"  Unique places with reviews: {unique_places:,}")
    print(f"  Duplicates removed:         {raw_count - unique_count:,}")
    print(f"  Input CSV size:             {format_bytes(input_size)}")
    print(f"  Output CSV size:            {format_bytes(csv_output_size)}")
    print(f"  Output Parquet size:        {format_bytes(parquet_size)}")

    if input_size > 0:
        reduction = (1 - parquet_size / input_size) * 100
        print(f"  Size reduction:             {reduction:.2f}%")
    else:
        reduction = 0.0

    print(f"  Total time:                 {overall_elapsed:.2f}s")
    print("=" * 80)

    # JSON Report
    report_data = {
        "total_raw_records": raw_count,
        "unique_reviews": unique_count,
        "unique_places_with_reviews": unique_places,
        "duplicates_removed": raw_count - unique_count,
        "input_csv_size_bytes": input_size,
        "input_csv_size_formatted": format_bytes(input_size),
        "output_csv_size_bytes": csv_output_size,
        "output_csv_size_formatted": format_bytes(csv_output_size),
        "output_parquet_size_bytes": parquet_size,
        "output_parquet_size_formatted": format_bytes(parquet_size),
        "size_reduction_percentage": round(reduction, 2),
        "total_time_seconds": round(overall_elapsed, 2),
    }

    report_path = output_dir / "etl_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=4, ensure_ascii=False)
    print(f"  [INFO] Report saved to {report_path.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="Reviews ETL Pipeline")
    parser.add_argument(
        "--input", type=str, default="data/output/reviews_raw.csv",
        help="Path to raw reviews CSV (default: data/output/reviews_raw.csv)",
    )
    parser.add_argument(
        "--output-dir", type=str, default="data_parquet",
        help="Path to output directory (default: data_parquet)",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    input_path = project_root / args.input
    output_path = project_root / args.output_dir

    run_pipeline(input_path, output_path)


if __name__ == "__main__":
    main()
