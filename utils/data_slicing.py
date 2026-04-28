# -*- coding: utf-8 -*-
"""
GitHub Data Slicer.
Splits large Parquet files into smaller chunks to bypass GitHub's 100MB file limit.
Used for uploading massive datasets to public forks where LFS is not an option.
"""

import polars as pl
from pathlib import Path

def slice_data(input_file: Path, output_dir: Path, parts: int = 2):
    """
    Reads a large Parquet file and splits it into multiple parts.
    """
    if not input_file.exists():
        print(f"[ERROR] Input file not found: {input_file}")
        return

    print(f"Reading {input_file.name}...")
    df = pl.read_parquet(input_file)
    total_rows = len(df)
    rows_per_part = total_rows // parts

    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Splitting {total_rows:,} rows into {parts} parts...")
    for i in range(parts):
        start = i * rows_per_part
        # For the last part, take everything remaining
        end = (i + 1) * rows_per_part if i < parts - 1 else total_rows
        
        chunk = df.slice(start, end - start)
        output_path = output_dir / f"reviews_peru_part{i+1}.parquet"
        
        chunk.write_parquet(output_path, compression="zstd", compression_level=9)
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  [DONE] Saved {output_path.name} ({size_mb:.2f} MB)")

if __name__ == "__main__":
    # Configure paths
    root = Path(__file__).resolve().parent.parent
    input_p = root / "data_parquet" / "Peru" / "reviews_peru.parquet"
    output_p = root / "data_gh"
    
    slice_data(input_p, output_p)
