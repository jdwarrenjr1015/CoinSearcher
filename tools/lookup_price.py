"""
lookup_price.py
---------------
Interactive CLI for querying the PCGS price database built by scrape_pcgs_prices.py.

Usage:
    python tools/lookup_price.py                        # interactive mode
    python tools/lookup_price.py "1909 S VDB Cent"      # single query
    python tools/lookup_price.py --pcgs 2050            # lookup by PCGS number
    python tools/lookup_price.py --list-categories      # show all scraped categories
    python tools/lookup_price.py --db PATH              # override db path

WAT Role: Tool (deterministic execution layer)
Workflow: workflows/scrape_pcgs_prices.md
"""

import argparse
import re
import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db as _db

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def list_categories(conn) -> list[str]:
    rows = _db.fetchall(conn, "SELECT DISTINCT category FROM coins ORDER BY category")
    return [r["category"] for r in rows]


def search_by_pcgs_num(conn, pcgs_num: str) -> list[dict]:
    return _db.fetchall(conn,
        f"SELECT * FROM coins WHERE pcgs_num = {_db.ph()} ORDER BY desig",
        (pcgs_num.strip(),),
    )


def search_by_description(conn, query: str) -> list[dict]:
    """Full-text style search using LIKE — case-insensitive."""
    tokens = query.strip().split()
    if not tokens:
        return []
    clauses = " AND ".join([f"LOWER(description) LIKE {_db.ph()}"] * len(tokens))
    params  = [f"%{t.lower()}%" for t in tokens]
    return _db.fetchall(conn,
        f"SELECT * FROM coins WHERE {clauses} ORDER BY pcgs_num, desig",
        params,
    )


def get_prices(conn, coin_id: int) -> list[dict]:
    return _db.fetchall(conn,
        f"SELECT grade, price FROM prices WHERE coin_id = {_db.ph()} ORDER BY "
        "CAST(REPLACE(grade, 'MS', '') AS REAL)",
        (coin_id,),
    )


def db_stats(conn) -> dict:
    coin_row  = _db.fetchone(conn, "SELECT COUNT(*) AS cnt FROM coins")
    price_row = _db.fetchone(conn, "SELECT COUNT(*) AS cnt FROM prices")
    last_row  = _db.fetchone(conn, "SELECT scraped_at FROM coins ORDER BY scraped_at DESC LIMIT 1")
    return {
        "coins":      coin_row["cnt"] if coin_row else 0,
        "prices":     price_row["cnt"] if price_row else 0,
        "scraped_at": last_row["scraped_at"] if last_row else "unknown",
    }


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def format_price(price) -> str:
    if price is None:
        return "n/a"
    return f"${price:,.0f}"


def print_coin(conn: sqlite3.Connection, coin: sqlite3.Row):
    print(f"\n  PCGS #   : {coin['pcgs_num']}")
    print(f"  Desc     : {coin['description']}")
    if coin["desig"]:
        print(f"  Desig    : {coin['desig']}")
    print(f"  Category : {coin['category']}")

    prices = get_prices(conn, coin["id"])
    if prices:
        print(f"  Grades   :")
        # Print in rows of 5
        chunk = []
        for p in prices:
            chunk.append(f"    {p['grade']:>6}: {format_price(p['price']):<12}")
            if len(chunk) == 5:
                print("".join(chunk))
                chunk = []
        if chunk:
            print("".join(chunk))
    else:
        print("  (No price data)")


def print_results(conn: sqlite3.Connection, coins: list[sqlite3.Row], query: str):
    if not coins:
        print(f"\nNo results for: {query!r}")
        return
    print(f"\nFound {len(coins)} result(s) for: {query!r}")
    for coin in coins[:20]:  # cap display at 20
        print_coin(conn, coin)
        print()
    if len(coins) > 20:
        print(f"  ... and {len(coins) - 20} more. Refine your search.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_query(conn: sqlite3.Connection, query: str):
    query = query.strip()
    if not query:
        return

    # If query looks like a pure number, treat as PCGS#
    if re.match(r"^\d+$", query):
        coins = search_by_pcgs_num(conn, query)
    else:
        coins = search_by_description(conn, query)

    print_results(conn, coins, query)


def interactive_mode(conn: sqlite3.Connection):
    stats = db_stats(conn)
    print(f"\nPCGS Price Lookup")
    print(f"  {stats['coins']:,} coins  |  {stats['prices']:,} price points  |  scraped {stats['scraped_at'][:10]}")
    print("  Enter a coin description (e.g. '1909 S VDB') or PCGS# to search.")
    print("  Type 'quit' or press Ctrl+C to exit.\n")

    while True:
        try:
            query = input("Search > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nBye.")
            break
        if query.lower() in ("quit", "exit", "q"):
            print("Bye.")
            break
        run_query(conn, query)


def main():
    parser = argparse.ArgumentParser(description="Query PCGS coin price database")
    parser.add_argument("query",            nargs="?",       help="Search term or PCGS number")
    parser.add_argument("--pcgs",           metavar="NUM",   help="Lookup by PCGS number directly")
    parser.add_argument("--list-categories",action="store_true", help="List all scraped categories")
    args = parser.parse_args()

    conn = _db.open_conn()

    if args.list_categories:
        cats = list_categories(conn)
        print(f"\n{len(cats)} categories in database:\n")
        for c in cats:
            print(f"  {c}")
        conn.close()
        return

    if args.pcgs:
        coins = search_by_pcgs_num(conn, args.pcgs)
        print_results(conn, coins, f"PCGS# {args.pcgs}")
        conn.close()
        return

    if args.query:
        run_query(conn, args.query)
        conn.close()
        return

    # Default: interactive mode
    interactive_mode(conn)
    conn.close()


if __name__ == "__main__":
    main()
