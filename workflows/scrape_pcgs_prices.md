# Workflow: Scrape PCGS Coin Prices

## Objective
Scrape current coin prices from https://www.pcgs.com/prices and save them to a local
SQLite database and CSV for easy lookup and analysis.

## Required Inputs
- Python 3.11+ with dependencies installed (`pip install -r requirements.txt`)
- Internet access to pcgs.com (no API key required — public web scraping)

## Tools Used
- `tools/scrape_pcgs_prices.py` — scraper
- `tools/lookup_price.py` — price lookup CLI

## Outputs
| File | Description |
|------|-------------|
| `.tmp/pcgs_prices.db` | SQLite database (coins + prices tables) |
| `.tmp/pcgs_prices.csv` | Flat CSV export (one row per coin × grade) |

---

## Step-by-Step Instructions

### 1. Install dependencies (first run only)
```
pip install -r requirements.txt
```

### 2. Run the scraper
```
python tools/scrape_pcgs_prices.py
```

This will:
1. Fetch the PCGS price index to discover all category URLs
2. For each category, scrape three grade-bin pages (grades 1–20, 25–60, 61–70)
3. Merge the grade data per coin into unified records
4. Save everything to `.tmp/pcgs_prices.db`
5. Export `.tmp/pcgs_prices.csv`

**Expected runtime:** 2–4 hours for a full scrape (hundreds of categories × 3 pages × 2s delay).

### 3. Test with a limited run first
```
python tools/scrape_pcgs_prices.py --limit 3
```
Scrapes only the first 3 categories — confirms the scraper is working before committing to a full run.

### 4. Resume an interrupted scrape
```
python tools/scrape_pcgs_prices.py --resume
```
Skips categories already in the database. Use this if the scraper was interrupted.

### 5. Look up prices
```
python tools/lookup_price.py                    # interactive mode
python tools/lookup_price.py "1909 S VDB"       # direct query
python tools/lookup_price.py --pcgs 2050        # by PCGS number
python tools/lookup_price.py --list-categories  # show all categories
```

---

## Database Schema

**coins table**
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| pcgs_num | TEXT | PCGS certification number |
| description | TEXT | Full coin description (e.g. "1909 S VDB Lincoln Cent") |
| desig | TEXT | Designation: BN, RB, RD (copper coins only) |
| category | TEXT | PCGS category name |
| scraped_at | TEXT | ISO 8601 UTC timestamp of scrape |

**prices table**
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| coin_id | INTEGER FK | References coins.id |
| grade | TEXT | Grade label (e.g. "MS65", "VF20") |
| price | REAL | Price in USD (NULL = not available) |

---

## Edge Cases & Known Issues

### Rate limiting (HTTP 429)
PCGS will return 429 if requests are too frequent. The scraper uses a 2-second delay
between requests. If 429s occur, the tool waits 30 seconds and retries up to 3 times.
If 429s persist, increase `REQUEST_DELAY` in `scrape_pcgs_prices.py` to 5–10 seconds.

### Table structure changes
If PCGS redesigns their price pages, the CSS class `table-main` may no longer apply.
Look at a sample page's HTML source, find the table containing prices, and update the
`parse_grade_row()` function in `scrape_pcgs_prices.py` accordingly.

### Missing price data
Some coins have `—` (dash) instead of a price for a grade. These are stored as NULL
in the database. This is expected — not every coin has been graded at every level.

### Grade bin URL patterns
PCGS splits grades into bins:
- `?Grade=` (empty/default) — grades 61–70 for many categories, or all grades for smaller ones
- `?Grade=1` — grades 1–20
- `?Grade=61` — grades 61–70 (Mint State)
If prices appear incomplete, inspect the actual PCGS URLs for a category and update
the `GRADE_BINS` constant in `scrape_pcgs_prices.py`.

---

## Refresh Schedule
PCGS updates prices regularly. Re-run the full scrape monthly or before any major
pricing decision. Use `--resume` if the previous scrape was recent and you want to
update only new/changed categories (note: PCGS doesn't expose change dates, so
`--resume` skips categories entirely — for a full refresh, run without `--resume`).

---

## Learnings
- PCGS has no public API; all data is scraped from rendered HTML.
- The three-bin page structure requires merging data from three URLs per category.
- A 2-second per-page delay is sufficient to avoid 429s in testing; increase if needed.
- The `lxml` parser is faster than Python's built-in `html.parser` for large tables.
