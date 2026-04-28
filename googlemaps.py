# -*- coding: utf-8 -*-
"""
Core Google Maps review scraper — async Playwright edition.
"""

import logging
import re
from datetime import datetime, timedelta

from playwright.async_api import (
    Page,
    BrowserContext,
    TimeoutError as PlaywrightTimeout,
)

# ── Constants ────────────────────────────────────────────────────────────────
GM_WEBPAGE = 'https://www.google.com/maps/'
TIMEOUT_MS = 5_000

REVIEWS_TAB_XPATH = (
    '//button[@role="tab" and ('
    'contains(., "Opiniones") or '
    'contains(., "Reseñas") or '
    'contains(., "Reseas") or '
    'contains(., "Reviews") or '
    'contains(@aria-label, "Opiniones") or '
    'contains(@aria-label, "Reseñas") or '
    'contains(@aria-label, "Reviews")'
    ')]'
)
SORT_BUTTON_SEL = 'button[aria-label*="Ordenar"]'
SORT_OPTION_SEL = 'div[role="menuitemradio"]'
REVIEW_BLOCK_SEL = 'div.jftiEf.fontBodyMedium'
EXPAND_BUTTON_SEL = 'button.w8nwRe.kyuRq'
SCROLL_DIV_SEL = 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf'

UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
)

EXTRACT_REVIEWS_JS = """
(offset) => {
    const blocks = document.querySelectorAll('div.jftiEf.fontBodyMedium');
    return Array.from(blocks).slice(offset).map(r => ({
        id_review:     r.getAttribute('data-review-id'),
        username:      r.getAttribute('aria-label'),
        caption:       r.querySelector('span.wiI7pd')?.textContent?.replace(/[\\r\\n\\t]/g, ' ') || null,
        rating_label:  r.querySelector('span.kvMYJc')?.getAttribute('aria-label') || null,
        relative_date: r.querySelector('span.rsqaWe')?.textContent || null,
        n_review_text: r.querySelector('div.RfnDt')?.textContent || null,
        url_user:      r.querySelector('button.WEBjve')?.getAttribute('data-href') || null,
    }));
}
"""

async def setup_context(context: BrowserContext):
    """Apply stealth tweaks."""
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
        Object.defineProperty(navigator, 'devicePixelRatio', {get: () => 1});
        Object.defineProperty(navigator, 'maxTouchPoints', {get: () => 0});
        Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
        Object.defineProperty(navigator, 'languages', {get: () => ['es-ES', 'es', 'en']});
    """)

class GoogleMapsScraper:
    def __init__(self, page: Page, debug: bool = False):
        self.page = page
        self.debug = debug
        self.logger = logging.getLogger('googlemaps-scraper')

    async def sort_by(self, url: str, ind: int) -> int:
        try:
            await self.page.goto(url.strip(), wait_until='domcontentloaded', timeout=30_000)
            try:
                await self.page.wait_for_selector('h1.DUwDvf', timeout=10_000)
            except:
                pass
            await self.page.wait_for_timeout(3000)
        except PlaywrightTimeout:
            self.logger.warning('Page load timeout: %s', url.strip())

        await self._click_cookie_agreement()

        if not await self._open_reviews_tab():
            return -1

        try:
            await self.page.locator(SORT_BUTTON_SEL).click(timeout=10_000)
            await self.page.locator(SORT_OPTION_SEL).first.wait_for(state='visible', timeout=5_000)
        except PlaywrightTimeout:
            return -1

        options = await self.page.locator(SORT_OPTION_SEL).all()
        if not options or ind >= len(options):
            return -1

        await options[ind].click()
        await self.page.wait_for_timeout(1000)
        return 0

    async def get_reviews(self, offset: int) -> list[dict]:
        await self._scroll()
        await self._wait_for_new_reviews(offset)
        await self._expand_reviews()
        raw = await self.page.evaluate(EXTRACT_REVIEWS_JS, offset)
        retrieval_date = datetime.now()
        parsed = []
        for r in raw:
            rating = None
            if r.get('rating_label'):
                m = re.search(r'(\d+)', r['rating_label'])
                rating = float(m.group(1)) if m else None
            n_reviews = 0
            if r.get('n_review_text'):
                try: n_reviews = r['n_review_text'].split(' ')[3]
                except: n_reviews = 0
            parsed.append({
                'id_review': r.get('id_review'), 'caption': r.get('caption'),
                'relative_date': r.get('relative_date'),
                'review_date': self._calculate_review_date(r.get('relative_date'), retrieval_date),
                'retrieval_date': retrieval_date, 'rating': rating,
                'username': r.get('username'), 'n_review_user': n_reviews,
                'n_photo_user': None, 'url_user': r.get('url_user'),
            })
        return parsed

    async def get_account(self, url: str) -> dict:
        try:
            await self.page.goto(url.strip(), wait_until='load', timeout=30_000)
            await self.page.wait_for_timeout(3000)
        except:
            pass
        await self._click_cookie_agreement()
        return await self._parse_place(url.strip())

    async def _open_reviews_tab(self) -> bool:
        for _ in range(3):
            try:
                tab = self.page.locator(f'xpath={REVIEWS_TAB_XPATH}')
                if await tab.count() > 0:
                    await tab.first.click(timeout=3_000)
                    await self._wait_for_tab_load()
                    return True
            except: pass

            try:
                summary_selectors = [
                    'button[aria-label*="opiniones"]',
                    'button[aria-label*="reseñas"]',
                    'button[aria-label*="reseas"]',
                    'button[aria-label*="reviews"]',
                    'div.F7nice'
                ]
                for sel in summary_selectors:
                    loc = self.page.locator(sel).first
                    if await loc.count() > 0 and await loc.is_visible():
                        await loc.click(timeout=3_000)
                        await self._wait_for_tab_load()
                        if await self.page.locator(SORT_BUTTON_SEL).count() > 0:
                            return True
            except: pass
            
            await self.page.wait_for_timeout(1000)
            await self.page.mouse.wheel(0, 300)

        return False

    async def _wait_for_tab_load(self):
        try:
            await self.page.locator(f'{REVIEW_BLOCK_SEL}, {SORT_BUTTON_SEL}').first.wait_for(
                state='visible', timeout=5_000)
        except: pass

    async def _scroll(self):
        try:
            el = self.page.locator(SCROLL_DIV_SEL)
            await el.evaluate('el => el.scrollTop = el.scrollHeight')
        except: pass

    async def _wait_for_new_reviews(self, current_offset: int):
        try:
            await self.page.wait_for_function(
                f'document.querySelectorAll("{REVIEW_BLOCK_SEL}").length > {current_offset}',
                timeout=3_000,
            )
        except: pass

    async def _expand_reviews(self):
        for btn in await self.page.locator(EXPAND_BUTTON_SEL).all():
            try: await btn.click(timeout=300)
            except: pass

    async def _click_cookie_agreement(self):
        try: await self.page.locator('text=Rechazar todo').click(timeout=2_000)
        except: pass

    def _calculate_review_date(self, relative_date_str, retrieval_date):
        if not relative_date_str: return retrieval_date
        try:
            s = relative_date_str.replace('Editado ', '').strip()
            m = re.search(r'(\d+)', s)
            if m: value = int(m.group(1))
            elif 'un' in s.lower(): value = 1
            else: return retrieval_date
            sl = s.lower()
            deltas = {
                'segundo': timedelta(seconds=value), 'minuto': timedelta(minutes=value),
                'hora': timedelta(hours=value), 'día': timedelta(days=value),
                'semana': timedelta(weeks=value), 'mes': timedelta(days=value*30),
                'año': timedelta(days=value*365),
            }
            for key, delta in deltas.items():
                if key in sl: return retrieval_date - delta
            return retrieval_date
        except: return retrieval_date

    async def _parse_place(self, url: str) -> dict:
        data = await self.page.evaluate("""() => {
            const txt  = s => document.querySelector(s)?.textContent?.trim() || null;
            const attr = (s, a) => document.querySelector(s)?.getAttribute(a) || null;
            const infoDivs = document.querySelectorAll('div.Io6YTe.fontBodyMedium');
            return {
                name: txt('h1.DUwDvf.fontHeadlineLarge'),
                overall_rating: attr('div.F7nice span.ceNzKf', 'aria-label'),
                n_reviews_text: txt('div.F7nice'),
                category: txt('button[jsaction="pane.rating.category"]'),
                address: infoDivs[0]?.textContent || null,
            };
        }""")
        return data
