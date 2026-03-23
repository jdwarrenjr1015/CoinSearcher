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
import sys
from pathlib import Path

# Ensure tools/ is on the path when imported from api/index.py
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

import db as _db

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

TOOLS_DIR = Path(__file__).resolve().parent
app = Flask(__name__, template_folder=str(TOOLS_DIR / "templates"))


@app.errorhandler(Exception)
def json_error(e):
    """Return JSON for all unhandled errors so JS never gets HTML."""
    import traceback
    code = getattr(e, "code", 500)
    return jsonify({"error": str(e), "trace": traceback.format_exc()[-500:]}), code


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def open_db():
    return _db.open_conn()


def db_stats(conn) -> dict:
    coin_row   = _db.fetchone(conn, "SELECT COUNT(*) AS n FROM coins")
    price_row  = _db.fetchone(conn, "SELECT COUNT(*) AS n FROM prices")
    latest_row = _db.fetchone(conn,
        "SELECT scraped_at FROM coins ORDER BY scraped_at DESC LIMIT 1")
    return {
        "coins":      coin_row["n"] if coin_row else 0,
        "prices":     price_row["n"] if price_row else 0,
        "scraped_at": (latest_row["scraped_at"][:10]
                       if latest_row and latest_row.get("scraped_at") else "unknown"),
    }


def search_by_pcgs_num(conn, pcgs_num: str) -> list[dict]:
    p = _db.ph()
    return _db.fetchall(conn,
        f"SELECT * FROM coins WHERE pcgs_num = {p} ORDER BY desig",
        (pcgs_num.strip(),),
    )


def search_by_description(conn, query: str) -> list[dict]:
    tokens = query.strip().split()
    if not tokens:
        return []
    p = _db.ph()
    clauses = " AND ".join([f"LOWER(description) LIKE {p}"] * len(tokens))
    params  = tuple(f"%{t.lower()}%" for t in tokens)
    return _db.fetchall(conn,
        f"SELECT * FROM coins WHERE {clauses} ORDER BY pcgs_num, desig",
        params,
    )


def get_prices(conn, coin_id: int) -> list[dict]:
    p = _db.ph()
    return _db.fetchall(conn,
        f"SELECT grade, price FROM prices WHERE coin_id = {p} "
        "ORDER BY CAST(REPLACE(REPLACE(grade,'MS',''),'PF','') AS REAL)",
        (coin_id,),
    )


def coins_to_json(conn, coins: list[dict]) -> list[dict]:
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
        model="claude-haiku-4-5-20251001",
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

@app.route("/debug")
def debug():
    db_url = _db._get_db_url()
    result = {
        "db_url_found": bool(db_url),
        "db_url_prefix": (db_url[:40] + "...") if db_url else None,
        "env_vars": {k: "set" for k in ["DATABASE_URL","POSTGRES_URL","NILEDB_POSTGRES_URL","NILEDB_URL"] if os.getenv(k)},
    }
    if db_url:
        try:
            conn = _db.open_conn()
            row = _db.fetchone(conn, "SELECT COUNT(*) AS cnt FROM coins")
            result["coins"] = row["cnt"]
            conn.close()
            result["status"] = "ok"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
    else:
        result["status"] = "no db url found"
    return result


@app.route("/")
def index():
    import os
    conn = None
    conn_error = None
    try:
        conn = open_db()
    except Exception as e:
        conn_error = str(e)
    stats = db_stats(conn) if conn else None
    if conn:
        conn.close()
    if conn_error:
        return f"<pre>DB connection error: {conn_error}\nDATABASE_URL set: {bool(os.getenv('DATABASE_URL'))}</pre>", 500
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

    # Determine media type — prefer Content-Type header, fall back to extension
    ct = photo.content_type or ""
    filename = photo.filename.lower()
    if "png" in ct or filename.endswith(".png"):
        media_type = "image/png"
    elif "webp" in ct or filename.endswith(".webp"):
        media_type = "image/webp"
    else:
        media_type = "image/jpeg"  # default for camera blobs and JPEGs

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
    if _db.is_postgres():
        print("Database      : Postgres (DATABASE_URL)")
    else:
        db_path = _db._sqlite_path()
        print(f"Database path : {db_path}")
        print(f"DB exists     : {db_path.exists()}")
    print(f"Claude API key: {'set' if ANTHROPIC_API_KEY else 'NOT SET (photo ID disabled)'}")
    app.run(host="0.0.0.0", port=5000, debug=True)
