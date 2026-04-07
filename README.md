# Google Maps Reviews Scraper (ES)

A Selenium-based scraper that extracts reviews from Google Maps place pages. Given a list of Google Maps URLs it navigates to each place, selects the desired sort order, scrolls through the reviews panel, and writes all reviews to a CSV file. A companion `monitor.py` script incrementally stores new reviews in MongoDB for scheduled runs.

---

## Requirements

| Requirement | Version |
|---|---|
| Python | ≥ 3.9 |
| Google Chrome | installed and in PATH |
| ChromeDriver | must match installed Chrome version |

Install Chrome and ChromeDriver on Debian/Ubuntu:

```bash
# Chrome (if not already installed)
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
echo "deb http://dl.google.com/linux/chrome/deb/ stable main" | sudo tee /etc/apt/sources.list.d/google-chrome.list
sudo apt update && sudo apt install -y google-chrome-stable

# ChromeDriver — must match the Chrome version shown by `google-chrome --version`
# Find the matching version at https://chromedriver.chromium.org/downloads
# Then place the binary somewhere on your PATH, e.g. /usr/local/bin/chromedriver
```

---

## Installation

```bash
git clone <repo-url>
cd googlemaps-reviews-scraper-es

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

---

## Usage

### Review scraper

```bash
python3 scraper.py [options]
```

**Options:**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--i` | str | `urls.txt` | Input file: one Google Maps place URL per line |
| `--o` | str | `output.csv` | Output filename — written inside the `data/` folder |
| `--N` | int | `100` | Maximum number of reviews to fetch per URL |
| `--sort_by` | str | `newest` | Sort order (see Sort Modes below) |
| `--place` | flag | off | Extract place metadata instead of reviews |
| `--debug` | flag | off | Show the browser window while scraping |
| `--source` | flag | off | Append an extra `url_source` column to the CSV |

**Full example:**

```bash
python3 scraper.py --N 200 --i urls.txt --o reviews.csv --sort_by newest --source
```

### How to get a valid URL

1. Open [Google Maps](https://www.google.com/maps) in your browser.
2. Search for a place and open its page.
3. Copy the URL from the address bar once the place detail panel is open.
4. Paste it into `urls.txt`, one URL per line.

The URL must identify a specific place — it should contain the place name and coordinate segment (`@lat,lng,zoom`). URLs that contain long search-query parameters (`!15s...` or `!1m2!2m1!1s...`) come from search-result views and may be less reliable; prefer URLs obtained by opening a place's own panel directly.

---

## Output

Reviews are written to `data/<output_file>` as CSV. Fields:

| Field | Type | Description |
|---|---|---|
| `id_review` | str | Unique review identifier |
| `caption` | str | Review text (None if rating-only) |
| `relative_date` | str | Original relative date string from Google (e.g. "Hace 3 días") |
| `review_date` | datetime | Approximate absolute date (retrieval_date minus relative duration) |
| `retrieval_date` | datetime | Timestamp when the scrape ran |
| `rating` | float | Star rating (1.0–5.0) |
| `username` | str | Display name of the reviewer |
| `n_review_user` | int/str | Number of reviews the reviewer has posted |
| `n_photo_user` | — | Always empty; Google no longer exposes this publicly |
| `url_user` | str | Reviewer's Google Maps profile URL |

When `--source` is used, an extra `url_source` column is appended with the place URL.

---

## Sort modes

| `--sort_by` value | Google Maps option | What you get |
|---|---|---|
| `most_relevant` | Más relevantes | Google's ranked mix of recent and high-quality reviews |
| `newest` | Más recientes | Chronological, newest first |
| `highest_rating` | Calificación más alta | 5-star reviews first |
| `lowest_rating` | Calificación más baja | 1-star reviews first |

---

## Place metadata mode

With `--place`, the scraper calls `get_account()` instead and prints a dict of place attributes:

```
name, overall_rating, n_reviews, n_photos, category, description,
address, website, phone_number, plus_code, opening_hours, url, lat, long
```

---

## Monitor (incremental scraping)

`monitor.py` runs the scraper on a schedule and stores only new reviews in MongoDB, stopping when it hits a review it has already seen or one older than `--from-date`.

Requires a running MongoDB instance ([installation guide](https://www.mongodb.com/docs/manual/installation/)).

```bash
python3 monitor.py --i urls.txt --from-date 2025-01-01
```

| Flag | Default | Description |
|---|---|---|
| `--i` | `urls.txt` | Input file with place URLs |
| `--from-date` | required | Earliest review date to store (YYYY-MM-DD) |
| `--db-url` | `mongodb://localhost:27017/` | MongoDB connection string |

---

## Known limitations

- **Rate limiting / bot detection**: Google detects automation and may show a "limited view" that hides reviews. The scraper uses anti-detection Chrome flags (`--disable-blink-features=AutomationControlled`, no `navigator.webdriver`, spoofed user-agent) to mitigate this. Scraping hundreds of URLs in a single session will likely trigger throttling or CAPTCHAs.
- **Headless mode**: `--debug` (visible browser) is sometimes more reliable if headless scraping is blocked.
- **`review_date` is approximate**: It is computed by subtracting the relative duration from the retrieval timestamp, so it may be off by days depending on when within the stated period the review was actually posted.
- **`n_photo_user` is always empty**: Google removed this field from the public UI.
- **Class selectors will drift**: Google Maps obfuscates its CSS class names and changes them without notice. If scraping suddenly stops working, inspect the constants block at the top of `googlemaps.py` and update the selectors there.
- **No authentication**: Signing in is not supported. Reviews marked as requiring a Google account to view will not be scraped.
- **10 reviews per scroll**: Each `get_reviews()` call loads approximately 10 reviews. The `--N` loop calls it repeatedly until `N` reviews are collected or the page is exhausted.

---

## Changelog

### 2026-04-07

**Fixed: review sorting was completely broken**

- **Root cause 1 — bot detection**: Old `--headless` mode caused Google Maps to show a "vista limitada" (limited view) that hides all reviews and the sort button. Fixed by switching to `--headless=new` and adding standard anti-bot Chrome flags: `--disable-blink-features=AutomationControlled`, `excludeSwitches: ["enable-automation"]`, `useAutomationExtension: False`, spoofed user-agent, and `navigator.webdriver` override via CDP.
- **Root cause 2 — missing tab navigation**: The sort button only appears after the "Opiniones" (Reviews) tab is activated. The scraper was trying to click the sort button on the Overview tab where it does not exist. Fixed by adding `__open_reviews_tab()` which clicks `//button[@role="tab"]` matching "Opiniones" before any sort interaction.
- Sort button aria-label changed from `Ordenar` to `Ordenar opiniones` — `contains()` XPath already handles this correctly; no selector change needed.

**Refactored**

- Extracted all CSS/XPath selectors and timeouts to a constants block at the top of `googlemaps.py` — future Google DOM changes require editing only that block.
- Replaced deprecated `pandas.DataFrame.append()` with `pd.concat()` in `get_places()`.
- Added `item['n_photo_user'] = None` to `__parse()` so the output dict always has all HEADER fields in the correct order.
- Removed `import numpy as np` (unused direct import).
- Removed `crayons` from `requirements.txt` (unused; `termcolor` is what the code actually uses).
- Removed commented-out dead code throughout.
- Sort failures now log the specific URL and attempt count; no longer silently return -1 without context.
- Added `url.strip()` calls to handle trailing newlines when reading URLs from file.
- Added validation of `--sort_by` value at startup with a clear error message.
