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
import sqlite3
from pathlib import Path

DEFAULT_DB = Path(".tmp/pcgs_prices.db")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def open_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}\n"
            "Run  python tools/scrape_pcgs_prices.py  first to build it."
        )
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def list_categories(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT category FROM coins ORDER BY category"
    ).fetchall()
    return [r["category"] for r in rows]


def search_by_pcgs_num(conn: sqlite3.Connection, pcgs_num: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM coins WHERE pcgs_num = ? ORDER BY desig",
        (pcgs_num.strip(),),
    ).fetchall()


def search_by_description(conn: sqlite3.Connection, query: str) -> list[sqlite3.Row]:
    """Full-text style search using LIKE — case-insensitive."""
    tokens = query.strip().split()
    if not tokens:
        return []
    # Build a WHERE clause that requires all tokens to appear somewhere in description
    clauses = " AND ".join(["LOWER(description) LIKE ?"] * len(tokens))
    params  = [f"%{t.lower()}%" for t in tokens]
    return conn.execute(
        f"SELECT * FROM coins WHERE {clauses} ORDER BY pcgs_num, desig",
        params,
    ).fetchall()


def get_prices(conn: sqlite3.Connection, coin_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT grade, price FROM prices WHERE coin_id = ? ORDER BY "
        # Sort numerically where possible
        "CAST(REPLACE(grade, 'MS', '') AS REAL)",
        (coin_id,),
    ).fetchall()


def db_stats(conn: sqlite3.Connection) -> dict:
    coin_count = conn.execute("SELECT COUNT(*) FROM coins").fetchone()[0]
    price_count = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    scraped_at = conn.execute(
        "SELECT scraped_at FROM coins ORDER BY scraped_at DESC LIMIT 1"
    ).fetchone()
    return {
        "coins":      coin_count,
        "prices":     price_count,
        "scraped_at": scraped_at[0] if scraped_at else "unknown",
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
    parser.add_argument("--db",             default=str(DEFAULT_DB), help="SQLite path")
    args = parser.parse_args()

    db_path = Path(args.db)
    try:
        conn = open_db(db_path)
    except FileNotFoundError as e:
        print(e)
        return

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
