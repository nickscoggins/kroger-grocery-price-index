import os
import psycopg
from contextlib import contextmanager

# NEW: bulk insert helper
from psycopg.extras import execute_values
from psycopg.types.json import Json

DB_URL = os.environ["SUPABASE_DB_URL"]

@contextmanager
def get_conn():
    # autocommit is fine for our short statements; you can also use explicit transactions if you prefer
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        # IMPORTANT: disable server-side prepared statements to avoid duplicate prepared name issues
        conn.prepare_threshold = None
        yield conn

def read_stores(conn):
    with conn.cursor() as cur:
        cur.execute("""
            select location_id
            from stores
            where is_active is not false
            order by location_id
        """)
        return [r[0] for r in cur.fetchall()]

def read_products(conn):
    with conn.cursor() as cur:
        cur.execute("""
            select upc, pid, coalesce(description, '')
            from products
            where pid is not null and pid <> ''
            order by upc
        """)
        return cur.fetchall()  # (upc, pid, description)

def upsert_prices(conn, rows):
    """
    rows: list of dicts with keys:
      location_id, upc, price_date, regular_price, promo_price, currency, price_source, raw_payload (JSON-able)
    Uses execute_values for fast, pipeline-safe bulk upsert.
    """
    if not rows:
        return

    # Build list of tuples for VALUES
    data = [
        (
            r["location_id"],
            r["upc"],
            r["price_date"],
            r["regular_price"],
            r["promo_price"],
            r.get("currency", "USD"),
            r.get("price_source", "kroger_api"),
            # wrap as Json to send as jsonb cleanly (handles dict/str)
            Json(r["raw_payload"] if isinstance(r["raw_payload"], (dict, list)) else r["raw_payload"])
        )
        for r in rows
    ]

    sql = """
    INSERT INTO daily_prices
      (location_id, upc, price_date, regular_price, promo_price, currency, price_source, raw_payload)
    VALUES %s
    ON CONFLICT (location_id, upc, price_date) DO UPDATE
      SET regular_price = EXCLUDED.regular_price,
          promo_price   = EXCLUDED.promo_price,
          currency      = EXCLUDED.currency,
          price_source  = EXCLUDED.price_source,
          raw_payload   = EXCLUDED.raw_payload
    """

    # Chunk very large batches to keep statement size reasonable
    CHUNK = 1000
    with conn.cursor() as cur:
        for i in range(0, len(data), CHUNK):
            execute_values(cur, sql, data[i:i+CHUNK], page_size=CHUNK)

def log_request(conn, op, target, status_code, ok, message):
    with conn.cursor() as cur:
        cur.execute("""
          INSERT INTO request_log (op, target, status_code, ok, message)
          VALUES (%s, %s, %s, %s, %s)
        """, (op, target, status_code, ok, message[:9000] if message else None))
