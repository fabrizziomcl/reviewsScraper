# -*- coding: utf-8 -*-
"""
Benchmark: async Playwright edition.
Tests different worker counts on the same sample.

Usage:
    python utils/benchmark_workers.py --sample 20 --max-reviews 30 --configs 1,4,8,12
"""

import argparse
import asyncio
import csv
import shutil
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
import sys, os
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from playwright.async_api import async_playwright
from orchestrator import load_places, init_output_csv
from worker import ReviewWorker
from googlemaps import UA
from config.scraper_config import RAW_OUTPUT_FILENAME, COMPLETED_PLACES_FILENAME
from tqdm import tqdm


async def run_benchmark(places: list[dict], n_workers: int, max_reviews: int,
                        bench_dir: Path) -> dict:
    pass_dir = bench_dir / f"workers_{n_workers}"
    if pass_dir.exists():
        shutil.rmtree(pass_dir)
    pass_dir.mkdir(parents=True)

    output_csv = pass_dir / RAW_OUTPUT_FILENAME
    completed_path = pass_dir / COMPLETED_PLACES_FILENAME
    init_output_csv(output_csv)

    csv_lock = asyncio.Lock()
    completed_lock = asyncio.Lock()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled',
                  '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
        )

        queue = asyncio.Queue()
        for place in places:
            queue.put_nowait(place)

        workers = [
            ReviewWorker(i, browser, output_csv, csv_lock, completed_lock,
                         completed_path, max_reviews, False)
            for i in range(n_workers)
        ]

        stats = {"reviews": 0, "errors": 0}
        start = time.time()

        pbar = tqdm(total=len(places), desc=f"  {n_workers}w", unit="pl",
                    miniters=1, dynamic_ncols=True, leave=False)

        async def worker_loop(w):
            while True:
                try:
                    place = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                count = await w.scrape_place(place["place_id"], place["url"])
                if count >= 0:
                    stats["reviews"] += count
                else:
                    stats["errors"] += 1
                pbar.update(1)
                await w.add_delay()
            await w.shutdown()

        await asyncio.gather(*[worker_loop(w) for w in workers])
        pbar.close()
        await browser.close()

    elapsed = time.time() - start
    return {
        "workers": n_workers, "places": len(places),
        "reviews": stats["reviews"], "errors": stats["errors"],
        "elapsed": elapsed,
        "avg_per_place": elapsed / max(len(places), 1),
        "throughput": len(places) / max(elapsed, 0.01),
    }


async def async_main():
    parser = argparse.ArgumentParser(description="Worker benchmark (async Playwright)")
    parser.add_argument("--sample", type=int, default=20)
    parser.add_argument("--max-reviews", type=int, default=30)
    parser.add_argument("--configs", type=str, default="1,4,8")
    args = parser.parse_args()

    worker_counts = [int(x.strip()) for x in args.configs.split(",")]

    test_csv = PROJECT_ROOT / "data" / "test" / "sample_places.csv"
    if not test_csv.exists():
        print(f"Error: {test_csv} not found.")
        return

    all_places = load_places(test_csv)
    places = all_places[:args.sample]
    print(f"Benchmark (async Playwright): {len(places)} places, max {args.max_reviews} reviews/place")
    print(f"Configs: {worker_counts} workers\n")

    bench_dir = PROJECT_ROOT / "data" / "benchmark"
    bench_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for n in worker_counts:
        label = f"[{n} worker{'s' if n > 1 else ' '}]"
        print(f"{'='*60}")
        print(f"  {label}  Starting...")
        print(f"{'='*60}")
        stats = await run_benchmark(places, n, args.max_reviews, bench_dir)
        results.append(stats)
        print(f"  {label}  Done in {stats['elapsed']:.1f}s "
              f"({stats['avg_per_place']:.1f}s/place, "
              f"{stats['reviews']} reviews, {stats['errors']} errors)\n")

    baseline = results[0]["elapsed"] if results else 1
    print("=" * 70)
    print("BENCHMARK RESULTS (async Playwright)")
    print("=" * 70)
    print(f"{'Workers':<10} {'Time':>10} {'Avg/place':>12} {'Throughput':>14} {'Speedup':>10} {'Reviews':>10}")
    print("-" * 70)
    for r in results:
        speedup = baseline / max(r["elapsed"], 0.01)
        print(f"{r['workers']:<10} {r['elapsed']:>9.1f}s {r['avg_per_place']:>11.1f}s "
              f"{r['throughput']:>10.2f} pl/s {speedup:>9.2f}x {r['reviews']:>10}")
    print("=" * 70)

    # Comentado para que el usuario pueda inspeccionar los archivos
    # if bench_dir.exists():
    #     shutil.rmtree(bench_dir)
    #     print(f"\n[INFO] Cleaned up {bench_dir}")


if __name__ == "__main__":
    asyncio.run(async_main())
