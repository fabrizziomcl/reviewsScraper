# -*- coding: utf-8 -*-
"""
Async review scraping worker — Playwright async edition.
Each worker owns a BrowserContext + Page from a shared Browser.
"""

import asyncio
import csv
import logging
import random
import time
from pathlib import Path

from googlemaps import GoogleMapsScraper, setup_context, UA, GM_WEBPAGE, TIMEOUT_MS
from config.scraper_config import (
    REVIEW_HEADER,
    MAX_REVIEWS_PER_PLACE,
    DELAY_BETWEEN_PLACES_MIN,
    DELAY_BETWEEN_PLACES_MAX,
    WORKER_RETRY_MAX,
    WORKER_RETRY_BACKOFF_BASE,
    PLACE_TIMEOUT,
)

log = logging.getLogger("reviews-worker")


class ReviewWorker:
    """Async worker: owns a BrowserContext + Page from a shared Browser."""

    def __init__(self, worker_id: int, browser, output_path: Path,
                 csv_lock: asyncio.Lock, completed_lock: asyncio.Lock,
                 completed_path: Path, max_reviews: int = MAX_REVIEWS_PER_PLACE,
                 debug: bool = False):
        self.worker_id = worker_id
        self.browser = browser  # shared browser instance
        self.output_path = output_path
        self.csv_lock = csv_lock
        self.completed_lock = completed_lock
        self.completed_path = completed_path
        self.max_reviews = max_reviews
        self.debug = debug
        self.scraper = None
        self._context = None
        self._page = None

    async def _init_scraper(self):
        await self._close_scraper()
        self._context = await self.browser.new_context(
            user_agent=UA, locale='es-ES',
            viewport={'width': 1920, 'height': 1080},
        )
        await setup_context(self._context)
        self._page = await self._context.new_page()
        self._page.set_default_timeout(15_000)
        await self._page.goto(GM_WEBPAGE, wait_until='load')
        self.scraper = GoogleMapsScraper(self._page, debug=self.debug)

    async def _close_scraper(self):
        self.scraper = None
        if self._page:
            try: await self._page.close()
            except Exception: pass
            self._page = None
        if self._context:
            try: await self._context.close()
            except Exception: pass
            self._context = None

    async def scrape_place(self, place_id: str, url: str, sort_index: int = 1) -> int:
        if not self.scraper:
            await self._init_scraper()

        for attempt in range(WORKER_RETRY_MAX):
            try:
                error = await self.scraper.sort_by(url, sort_index)

                if error != 0:
                    log.info(f"[W{self.worker_id}] No reviews tab for {place_id}")
                    await self._mark_completed(place_id)
                    return 0

                total_scraped = 0
                start_time = time.time()
                offset = 0

                empty_count = 0
                while offset < self.max_reviews:
                    if time.time() - start_time > PLACE_TIMEOUT:
                        log.warning(f"[W{self.worker_id}] Timeout for {place_id} after {total_scraped} reviews")
                        break

                    reviews = await self.scraper.get_reviews(offset)
                    if not reviews:
                        empty_count += 1
                        if empty_count >= 3:  # Try 3 times before giving up
                            break
                        await asyncio.sleep(1) # Wait a bit before retrying scroll
                        continue
                    
                    empty_count = 0 # reset on success

                    rows = []
                    for r in reviews:
                        rows.append([
                            place_id, r.get("id_review"), r.get("caption"),
                            r.get("relative_date"), str(r.get("review_date", "")),
                            str(r.get("retrieval_date", "")), r.get("rating"),
                            r.get("username"), r.get("n_review_user"),
                            r.get("n_photo_user"), r.get("url_user"), url,
                        ])

                    async with self.csv_lock:
                        with open(self.output_path, "a", encoding="utf-8", newline="\n") as f:
                            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                            writer.writerows(rows)

                    total_scraped += len(reviews)
                    offset += len(reviews)

                await self._mark_completed(place_id)
                return total_scraped

            except Exception as e:
                log.error(f"[W{self.worker_id}] Error {place_id} (attempt {attempt+1}/{WORKER_RETRY_MAX}): {e}")
                if attempt < WORKER_RETRY_MAX - 1:
                    await asyncio.sleep(WORKER_RETRY_BACKOFF_BASE * (2 ** attempt))
                    try:
                        await self._init_scraper()
                    except Exception:
                        log.error(f"[W{self.worker_id}] Failed to reinitialize")
                        return -1

        log.error(f"[W{self.worker_id}] All retries exhausted for {place_id}")
        return -1

    async def _mark_completed(self, place_id: str):
        async with self.completed_lock:
            with open(self.completed_path, "a", encoding="utf-8") as f:
                f.write(f"{place_id}\n")

    async def add_delay(self):
        delay = random.uniform(DELAY_BETWEEN_PLACES_MIN, DELAY_BETWEEN_PLACES_MAX)
        await asyncio.sleep(delay)

    async def shutdown(self):
        await self._close_scraper()
        log.info(f"[W{self.worker_id}] Shutdown complete")
