# -*- coding: utf-8 -*-
"""
Parallel review scraping orchestrator — async Playwright edition.

Uses a SINGLE shared Chromium browser with N lightweight BrowserContexts.
Each worker is an async coroutine pulling places from an asyncio.Queue.

Usage:
    python orchestrator.py
    python orchestrator.py --input data/test/sample_places.csv --workers 2
    python orchestrator.py --input data/input/places_peru.csv --workers 8 --max-reviews 15000
"""

import argparse
import asyncio
import csv
import logging
import sys
import time
from pathlib import Path

from tqdm import tqdm
from playwright.async_api import async_playwright

from googlemaps import UA, setup_context
from config.scraper_config import (
    DEFAULT_WORKERS,
    MAX_REVIEWS_PER_PLACE,
    REVIEW_HEADER,
    RAW_OUTPUT_FILENAME,
    COMPLETED_PLACES_FILENAME,
    DEFAULT_INPUT_FILE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PARQUET_DIR,
)
from worker import ReviewWorker

# ── Logging ───────────────────────────────────────────────────────────────────
_file_handler = logging.FileHandler("orchestrator.log", encoding="utf-8")
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.CRITICAL)
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
log = logging.getLogger("orchestrator")

logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("playwright").setLevel(logging.CRITICAL)


def load_places(input_path: Path) -> list[dict]:
    import polars as pl
    df = pl.read_csv(str(input_path), infer_schema_length=0, ignore_errors=True, encoding="utf8-lossy")
    if "id" not in df.columns or "url_place" not in df.columns:
        log.error(f"Input CSV must have 'id' and 'url_place' columns. Found: {df.columns}")
        sys.exit(1)
    df = df.filter(pl.col("url_place").is_not_null() & (pl.col("url_place") != ""))

    # Pre-filter: skip places explicitly marked with 0 reviews
    if "reviews" in df.columns:
        total_before = len(df)
        has_data = df.filter(pl.col("reviews").is_not_null() & (pl.col("reviews") != ""))
        if len(has_data) > len(df) * 0.1:
            df = df.filter(
                (pl.col("reviews").is_null()) | (pl.col("reviews") == "") | (pl.col("reviews") != "0")
            )
            skipped = total_before - len(df)
            if skipped > 0:
                log.info(f"Pre-filtered {skipped:,} places with 0 reviews")

    return [{"place_id": r["id"], "url": r["url_place"], "title": r.get("title", "")}
            for r in df.iter_rows(named=True)]


def load_completed(completed_path: Path) -> set[str]:
    if not completed_path.exists():
        return set()
    with open(completed_path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def init_output_csv(output_path: Path):
    if not output_path.exists():
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8", newline="\n") as f:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow(REVIEW_HEADER)


async def _worker_loop(worker: ReviewWorker, queue: asyncio.Queue,
                       pbar: tqdm, stats: dict):
    """Each worker pulls places from the queue until empty."""
    while True:
        try:
            place = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        place_id = place["place_id"]
        url = place["url"]

        start = time.time()
        count = await worker.scrape_place(place_id, url)
        elapsed = time.time() - start

        if count >= 0:
            stats["reviews"] += count
        else:
            stats["errors"] += 1

        pbar.set_postfix(reviews=f"{stats['reviews']:,}", errors=stats["errors"], refresh=True)
        pbar.update(1)

        await worker.add_delay()

    await worker.shutdown()


async def run_orchestrator(input_path: Path, output_dir: Path, parquet_dir: Path,
                           n_workers: int, max_reviews: int, debug: bool = False,
                           skip_etl: bool = False):
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / RAW_OUTPUT_FILENAME
    completed_path = output_dir / COMPLETED_PLACES_FILENAME

    all_places = load_places(input_path)
    completed = load_completed(completed_path)
    remaining = [p for p in all_places if p["place_id"] not in completed]

    print("=" * 80)
    print("REVIEWS SCRAPING ORCHESTRATOR (async Playwright)")
    print(f"  Input:        {input_path.resolve()}")
    print(f"  Output:       {output_csv.resolve()}")
    print(f"  Workers:      {n_workers}")
    print(f"  Max reviews:  {max_reviews:,}")
    print(f"  Total places: {len(all_places):,}")
    print(f"  Completed:    {len(completed):,}")
    print(f"  Remaining:    {len(remaining):,}")
    print("=" * 80)

    if not remaining:
        print("\n  [INFO] All places already scraped. Nothing to do.")
        if not skip_etl:
            _run_etl(output_csv, parquet_dir)
        return

    init_output_csv(output_csv)

    # ── Launch ONE shared browser ─────────────────────────────────────────
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=not debug,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu',
            ],
        )

        csv_lock = asyncio.Lock()
        completed_lock = asyncio.Lock()

        # Fill the queue
        queue = asyncio.Queue()
        for place in remaining:
            queue.put_nowait(place)

        # Create workers (each gets its own BrowserContext from shared browser)
        workers = [
            ReviewWorker(
                worker_id=i, browser=browser, output_path=output_csv,
                csv_lock=csv_lock, completed_lock=completed_lock,
                completed_path=completed_path, max_reviews=max_reviews, debug=debug,
            )
            for i in range(n_workers)
        ]

        stats = {"reviews": 0, "errors": 0}
        overall_start = time.time()

        pbar = tqdm(total=len(remaining), desc="Scraping reviews", unit="place",
                    miniters=1, dynamic_ncols=True)

        try:
            # Launch all workers as concurrent coroutines
            await asyncio.gather(*[
                _worker_loop(w, queue, pbar, stats) for w in workers
            ])
        except KeyboardInterrupt:
            log.warning("Interrupted by user. Saving progress...")
            for w in workers:
                await w.shutdown()

        pbar.close()
        await browser.close()

    overall_elapsed = time.time() - overall_start

    print("\n" + "=" * 80)
    print("SCRAPING SUMMARY")
    processed = len(remaining) - stats["errors"]
    print(f"  Places processed:   {processed:,}")
    print(f"  Places with errors: {stats['errors']:,}")
    print(f"  Total reviews:      {stats['reviews']:,}")
    print(f"  Total time:         {overall_elapsed:.1f}s")
    if len(remaining) > 0:
        print(f"  Avg time/place:     {overall_elapsed / len(remaining):.1f}s")
    print("=" * 80)

    if not skip_etl and output_csv.exists():
        _run_etl(output_csv, parquet_dir)


def _run_etl(input_csv: Path, parquet_dir: Path):
    print(f"\n  [INFO] Running ETL pipeline on {input_csv.name}...")
    try:
        from etl.pipeline import run_pipeline
        run_pipeline(input_csv, parquet_dir)
    except Exception as e:
        log.error(f"ETL pipeline failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Parallel Google Maps Reviews Scraper (async Playwright)")
    parser.add_argument("--input", type=str, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--parquet-dir", type=str, default=DEFAULT_PARQUET_DIR)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--max-reviews", type=int, default=MAX_REVIEWS_PER_PLACE)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--skip-etl", action="store_true")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    input_path = project_root / args.input
    output_dir = project_root / args.output_dir
    parquet_dir = project_root / args.parquet_dir

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    asyncio.run(run_orchestrator(
        input_path=input_path, output_dir=output_dir, parquet_dir=parquet_dir,
        n_workers=args.workers, max_reviews=args.max_reviews,
        debug=args.debug, skip_etl=args.skip_etl,
    ))


if __name__ == "__main__":
    main()
