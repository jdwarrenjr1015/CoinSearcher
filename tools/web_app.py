"""
web_app.py
----------
Flask web application for PCGS coin price lookup.

Routes:
  GET  /          - Home page with search form and photo upload
  POST /search    - Text search, returns JSON results
  POST /identify  - Image upload -> Claude vision -> DB search -> JSON results

WAT Role: Tool (deterministic execution layer)
Workflow: workflows/scrape_pcgs_prices.md

Run with:
  .venv/Scripts/python.exe tools/web_app.py
"""

import base64
import json
import os
import re
import sqlite3
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = Path(os.getenv("DB_PATH", str(BASE_DIR / ".tmp" / "pcgs_prices.db")))

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

app = Flask(__name__, template_folder="templates")

# ---------------------------------------------------------------------------
# Database helpers  (mirrors lookup_price.py — no import coupling)
# ---------------------------------------------------------------------------

def open_db() -> sqlite3.Connection | None:
    """Return a DB connection or None if the database file does not exist."""
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def db_stats(conn: sqlite3.Connection) -> dict:
    coin_count  = conn.execute("SELECT COUNT(*) FROM coins").fetchone()[0]
    price_count = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    scraped_at  = conn.execute(
        "SELECT scraped_at FROM coins ORDER BY scraped_at DESC LIMIT 1"
    ).fetchone()
    return {
        "coins":      coin_count,
        "prices":     price_count,
        "scraped_at": scraped_at[0][:10] if scraped_at else "unknown",
    }


def search_by_pcgs_num(conn: sqlite3.Connection, pcgs_num: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM coins WHERE pcgs_num = ? ORDER BY desig",
        (pcgs_num.strip(),),
    ).fetchall()
    return [dict(r) for r in rows]


def search_by_description(conn: sqlite3.Connection, query: str) -> list[dict]:
    """All-tokens-must-match LIKE search across description column."""
    tokens = query.strip().split()
    if not tokens:
        return []
    clauses = " AND ".join(["LOWER(description) LIKE ?"] * len(tokens))
    params  = [f"%{t.lower()}%" for t in tokens]
    rows = conn.execute(
        f"SELECT * FROM coins WHERE {clauses} ORDER BY pcgs_num, desig",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def get_prices(conn: sqlite3.Connection, coin_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT grade, price FROM prices WHERE coin_id = ? "
        "ORDER BY CAST(REPLACE(REPLACE(grade,'MS',''),'PF','') AS REAL)",
        (coin_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def coins_to_json(conn: sqlite3.Connection, coins: list[dict]) -> list[dict]:
    """Attach prices to each coin dict."""
    result = []
    for coin in coins[:50]:  # hard cap to avoid enormous payloads
        prices = get_prices(conn, coin["id"])
        result.append({
            "id":          coin["id"],
            "pcgs_num":    coin["pcgs_num"],
            "description": coin["description"],
            "desig":       coin["desig"],
            "category":    coin["category"],
            "scraped_at":  coin.get("scraped_at", ""),
            "prices":      prices,
        })
    return result

# ---------------------------------------------------------------------------
# Claude vision helper
# ---------------------------------------------------------------------------

def identify_coin_with_claude(image_bytes: bytes, media_type: str) -> dict:
    """
    Send the image to Claude and ask it to identify the coin.
    Returns a dict with keys:
      description_query, year, mint_mark, denomination,
      grade_estimate, notes
    Raises RuntimeError on failure.
    """
    if not ANTHROPIC_API_KEY:
        raise RuntimeError(
            "ANTHROPIC_API_KEY is not set in .env — "
            "photo identification is unavailable."
        )

    import anthropic  # local import so app still starts without the package

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    image_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")

    prompt = (
        "You are a numismatic expert. Examine this coin image and identify it.\n\n"
        "Return ONLY a JSON object (no markdown, no extra text) with exactly these fields:\n"
        "{\n"
        '  "description_query": "short search string, e.g. 1909 S VDB Lincoln Cent",\n'
        '  "year": "4-digit year or empty string",\n'
        '  "mint_mark": "mint mark letter(s) or empty string",\n'
        '  "denomination": "e.g. Cent, Nickel, Dime, Quarter, Half Dollar, Dollar",\n'
        '  "grade_estimate": "PCGS-style grade or range, e.g. MS-63, VF-20 to EF-40",\n'
        '  "notes": "any other notable details (errors, varieties, toning, etc.)"\n'
        "}\n\n"
        "If you cannot determine a field with confidence, use an empty string for that field.\n"
        "description_query should contain the most useful tokens for a keyword DB search "
        "(year, series name, mint mark if present — omit the grade)."
    )

    message = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=512,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type":  "image",
                        "source": {
                            "type":       "base64",
                            "media_type": media_type,
                            "data":       image_b64,
                        },
                    },
                    {"type": "text", "text": prompt},
                ],
            }
        ],
    )

    raw = message.content[0].text.strip()

    # Strip any accidental markdown fences
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)

    return json.loads(raw)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    conn  = open_db()
    stats = db_stats(conn) if conn else None
    if conn:
        conn.close()
    return render_template("index.html", stats=stats)


@app.route("/search", methods=["POST"])
def search():
    data  = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"error": "Empty query"}), 400

    conn = open_db()
    if conn is None:
        return jsonify({
            "error": "Database not built yet — run the scraper first.",
            "db_missing": True,
        }), 503

    try:
        if re.match(r"^\d+$", query):
            coins = search_by_pcgs_num(conn, query)
        else:
            coins = search_by_description(conn, query)

        results = coins_to_json(conn, coins)
        total   = len(coins)
    finally:
        conn.close()

    return jsonify({
        "query":   query,
        "total":   total,
        "results": results,
    })


@app.route("/identify", methods=["POST"])
def identify():
    if "photo" not in request.files:
        return jsonify({"error": "No photo file in request"}), 400

    photo = request.files["photo"]
    if photo.filename == "":
        return jsonify({"error": "Empty filename"}), 400

    # Determine media type
    filename   = photo.filename.lower()
    if filename.endswith(".png"):
        media_type = "image/png"
    elif filename.endswith((".jpg", ".jpeg")):
        media_type = "image/jpeg"
    elif filename.endswith(".webp"):
        media_type = "image/webp"
    elif filename.endswith(".gif"):
        media_type = "image/gif"
    else:
        return jsonify({"error": "Unsupported image format. Use JPG or PNG."}), 400

    image_bytes = photo.read()

    # Ask Claude to identify the coin
    try:
        identification = identify_coin_with_claude(image_bytes, media_type)
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503
    except Exception as e:
        return jsonify({"error": f"Claude API error: {e}"}), 502

    # Search the DB with the returned description_query
    description_query = identification.get("description_query", "").strip()

    conn = open_db()
    if conn is None:
        # Return identification even if DB is missing
        return jsonify({
            "identification": identification,
            "query":   description_query,
            "total":   0,
            "results": [],
            "warning": "Database not built yet — run the scraper first.",
        })

    try:
        if description_query:
            if re.match(r"^\d+$", description_query):
                coins = search_by_pcgs_num(conn, description_query)
            else:
                coins = search_by_description(conn, description_query)
        else:
            coins = []

        results = coins_to_json(conn, coins)
        total   = len(coins)
    finally:
        conn.close()

    return jsonify({
        "identification": identification,
        "query":   description_query,
        "total":   total,
        "results": results,
    })

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Database path : {DB_PATH}")
    print(f"DB exists     : {DB_PATH.exists()}")
    print(f"Claude API key: {'set' if ANTHROPIC_API_KEY else 'NOT SET (photo ID disabled)'}")
    app.run(host="0.0.0.0", port=5000, debug=True)
