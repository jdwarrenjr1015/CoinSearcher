"""
scrape_pcgs_prices.py
---------------------
Scrapes current coin prices from https://www.pcgs.com/prices and saves them
to a SQLite database (.tmp/pcgs_prices.db) and a CSV export (.tmp/pcgs_prices.csv).

Usage:
    python tools/scrape_pcgs_prices.py [--limit N] [--resume]

Options:
    --limit N    Only scrape the first N categories (useful for testing)
    --resume     Skip categories already present in the database
    --db PATH    Override default SQLite path (.tmp/pcgs_prices.db)

WAT Role: Tool (deterministic execution layer)
Workflow: workflows/scrape_pcgs_prices.md
"""

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent))
import db as _db

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://www.pcgs.com"
PRICES_INDEX = "https://www.pcgs.com/prices"

# PCGS splits grades into three path-based pages per coin category
# Base URL ends in /most-active — replace that segment with each bin
GRADE_BINS = ["grades-1-20", "grades-25-60", "grades-61-70"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

REQUEST_DELAY = 5.0   # seconds between page requests — be polite
RETRY_DELAY  = 30.0   # seconds to wait after a 429 / 5xx
MAX_RETRIES  = 3

DEFAULT_DB  = Path(".tmp/pcgs_prices.db")
DEFAULT_CSV = Path(".tmp/pcgs_prices.csv")

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

session = requests.Session()
session.headers.update(HEADERS)


def get_page(url: str) -> BeautifulSoup | None:
    """Fetch a URL with retry logic. Returns BeautifulSoup or None on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "lxml")
            if resp.status_code in (429, 503):
                print(f"  Rate-limited ({resp.status_code}) on attempt {attempt}. "
                      f"Waiting {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"  HTTP {resp.status_code} for {url} (attempt {attempt})")
                return None
        except requests.RequestException as exc:
            print(f"  Request error on attempt {attempt}: {exc}")
            time.sleep(RETRY_DELAY)
    print(f"  Giving up on {url} after {MAX_RETRIES} attempts.")
    return None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def get_category_links(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the /prices index page and return a list of
    {'name': str, 'url': str} dicts for each subcategory.
    """
    links = []
    # Category links live in anchor tags inside list items or divs under .price-guide-list
    for a in soup.select("a[href*='/prices/detail/']"):
        href = a.get("href", "")
        name = a.get_text(strip=True)
        # Skip non-category links (app store, etc.)
        if not href.startswith("/prices/detail/"):
            continue
        if href and name:
            full_url = BASE_URL + href if href.startswith("/") else href
            # Strip any existing grade query params — we'll add our own
            full_url = full_url.split("?")[0]
            links.append({"name": name, "url": full_url})
    # Deduplicate by URL
    seen = set()
    unique = []
    for link in links:
        if link["url"] not in seen:
            seen.add(link["url"])
            unique.append(link)
    return unique


def parse_price_table(table) -> list[dict]:
    """
    Parse a PCGS price table (BeautifulSoup tag).

    Table structure:
      Header row: ['PCGS #', 'Description', 'Desig', '1', '2', '3', ...]
      Data rows:  ['3730', 'Three Cent Nickel...', 'MS', '27', '28', ...]

    Returns a list of coin dicts with pcgs_num, description, desig, grades.
    """
    grade_labels: list[str] = []
    coins: list[dict] = []

    for row in table.find_all("tr"):
        tds = row.find_all(["td", "th"])
        if len(tds) < 4:
            continue

        # Use simple text for all cells
        cells = [c.get_text(strip=True) for c in tds]

        # Detect header rows by first cell content
        if cells[0] in ("PCGS #", "PCGS#"):
            grade_labels = cells[3:]
            continue

        # Skip rows that don't start with a numeric PCGS number
        pcgs_num = cells[0].replace(",", "").strip()
        if not re.match(r"^\d+$", pcgs_num):
            continue

        if not grade_labels:
            continue

        description = cells[1]
        # Desig cell may render "MS\n+" — normalize to just the base designation
        desig = tds[2].get_text(" ", strip=True).split()[0]

        grades: dict[str, float | None] = {}
        for i, label in enumerate(grade_labels):
            if i + 3 >= len(tds):
                break
            # Each price cell may have two <a> tags (regular + plus-grade).
            # Take only the FIRST <a> as the regular price.
            cell_tag = tds[i + 3]
            first_a = cell_tag.find("a")
            raw = (first_a.get_text(strip=True) if first_a
                   else cell_tag.get_text(strip=True))
            raw = raw.replace("$", "").replace(",", "").strip()
            price = None
            if raw and raw not in ("-", "\u2014", "N/A", ""):
                try:
                    price = float(raw)
                except ValueError:
                    pass
            grades[label] = price

        coins.append({
            "pcgs_num":    pcgs_num,
            "description": description,
            "desig":       desig,
            "grades":      grades,
        })

    return coins


def scrape_category(category_url: str) -> list[dict]:
    """
    Scrape all three grade-bin pages for a category URL and return
    a merged list of coin records.

    Category URLs look like: .../prices/detail/nickel-type-coins/-8/most-active
    Grade bin URLs replace the last path segment with: grades-1-20, grades-25-60, grades-61-70
    """
    # Replace the last path segment with each grade bin
    base_url = "/".join(category_url.rstrip("/").split("/")[:-1])

    coins: dict[str, dict] = {}  # keyed by pcgs_num + desig

    for bin_name in GRADE_BINS:
        url = f"{base_url}/{bin_name}"
        soup = get_page(url)
        time.sleep(REQUEST_DELAY)

        if soup is None:
            continue

        table = soup.find("table", class_="table-main")
        if table is None:
            table = soup.find("table")
        if table is None:
            continue

        records = parse_price_table(table)
        for record in records:
            key = f"{record['pcgs_num']}|{record['desig']}"
            if key not in coins:
                coins[key] = {
                    "pcgs_num":    record["pcgs_num"],
                    "description": record["description"],
                    "desig":       record["desig"],
                    "grades":      {},
                }
            coins[key]["grades"].update(record["grades"])

    return list(coins.values())


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def init_db(db_path: Path = None):
    """
    Create (or open) the database and ensure the schema exists.
    Uses Postgres if DATABASE_URL is set, otherwise SQLite at db_path.
    """
    if _db.is_postgres():
        conn = _db.open_conn()
    else:
        import sqlite3
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")

    for stmt in _db.db_schema_sql():
        _db.execute(conn, stmt)
    conn.commit()
    return conn


def insert_coin(conn, record: dict, category: str, scraped_at: str) -> int:
    p = _db.ph()
    if _db.is_postgres():
        cur = _db.execute(conn,
            f"INSERT INTO coins (pcgs_num, description, desig, category, scraped_at) "
            f"VALUES ({p},{p},{p},{p},{p}) RETURNING id",
            (record["pcgs_num"], record["description"], record["desig"], category, scraped_at),
        )
        coin_id = cur.fetchone()[0]
    else:
        cur = _db.execute(conn,
            f"INSERT INTO coins (pcgs_num, description, desig, category, scraped_at) "
            f"VALUES ({p},{p},{p},{p},{p})",
            (record["pcgs_num"], record["description"], record["desig"], category, scraped_at),
        )
        coin_id = cur.lastrowid

    price_rows = [
        (coin_id, grade, price)
        for grade, price in record["grades"].items()
    ]
    _db.executemany(conn,
        f"INSERT INTO prices (coin_id, grade, price) VALUES ({p},{p},{p})",
        price_rows,
    )
    return coin_id


def get_scraped_categories(conn) -> set[str]:
    rows = _db.fetchall(conn, "SELECT DISTINCT category FROM coins")
    return {r["category"] for r in rows}


# ---------------------------------------------------------------------------
# CSV export (SQLite only — for local use)
# ---------------------------------------------------------------------------

def export_csv(conn, csv_path: Path) -> int:
    """Export the full coins + prices table to a flat CSV (local SQLite only)."""
    if _db.is_postgres():
        print("  (CSV export skipped — not supported for Postgres)")
        return 0

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    rows = _db.fetchall(conn, """
        SELECT c.pcgs_num, c.description, c.desig, c.category,
               p.grade, p.price, c.scraped_at
        FROM coins c
        JOIN prices p ON p.coin_id = c.id
        ORDER BY c.pcgs_num, p.grade
    """)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["pcgs_num", "description", "desig", "category",
                         "grade", "price", "scraped_at"])
        writer.writerows([
            (r["pcgs_num"], r["description"], r["desig"], r["category"],
             r["grade"], r["price"], r["scraped_at"])
            for r in rows
        ])

    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape PCGS coin prices to SQLite or Postgres")
    parser.add_argument("--limit",  type=int, default=None, help="Max categories to scrape")
    parser.add_argument("--resume", action="store_true", help="Skip already-scraped categories")
    parser.add_argument("--db",     default=str(DEFAULT_DB), help="SQLite output path (ignored if DATABASE_URL set)")
    args = parser.parse_args()

    if _db.is_postgres():
        print("Database : Postgres (DATABASE_URL)")
        csv_path = None
    else:
        db_path  = Path(args.db)
        csv_path = db_path.with_suffix(".csv")
        print(f"Database : {db_path}")
        print(f"CSV      : {csv_path}")
    print()

    conn = init_db(None if _db.is_postgres() else Path(args.db))

    # --- Step 1: Get category index ---
    print("Fetching PCGS price index...")
    index_soup = get_page(PRICES_INDEX)
    if index_soup is None:
        print("ERROR: Could not load PCGS prices index. Aborting.")
        return

    categories = get_category_links(index_soup)
    print(f"Found {len(categories)} category links.")

    if args.resume:
        already_done = get_scraped_categories(conn)
        categories = [c for c in categories if c["name"] not in already_done]
        print(f"Resuming: {len(categories)} categories remaining.")

    if args.limit:
        categories = categories[: args.limit]
        print(f"Limiting to first {args.limit} categories.")

    print()

    # --- Step 2: Scrape each category ---
    scraped_at  = datetime.now(timezone.utc).isoformat()
    total_coins = 0

    for cat in tqdm(categories, desc="Categories", unit="cat"):
        tqdm.write(f"  Scraping: {cat['name']}")
        records = scrape_category(cat["url"])

        for record in records:
            insert_coin(conn, record, cat["name"], scraped_at)
            total_coins += 1

        conn.commit()  # checkpoint after each category

    # --- Step 3: Export CSV (SQLite only) ---
    print(f"\nScraping complete. {total_coins} coin records saved.")
    if csv_path:
        print("Exporting CSV...")
        csv_rows = export_csv(conn, csv_path)
        print(f"CSV written: {csv_rows} rows -> {csv_path}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
