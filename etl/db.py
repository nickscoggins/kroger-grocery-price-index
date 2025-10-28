import os
import psycopg
from contextlib import contextmanager

# NEW: bulk insert helper
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
      location_id, upc, price_date, regular_price, promo_price, currency, price_source, raw_payload
    Performs chunked multi-values INSERT ... ON CONFLICT using psycopg3 (no extras).
    """
    if not rows:
        return

    # Convert dicts -> tuples in the right column order
    data = [
        (
            r["location_id"],
            r["upc"],
            r["price_date"],
            r["regular_price"],
            r["promo_price"],
            r.get("currency", "USD"),
            r.get("price_source", "kroger_api"),
            Json(r["raw_payload"] if isinstance(r["raw_payload"], (dict, list)) else r["raw_payload"]),
        )
        for r in rows
    ]

    base_sql = """
    INSERT INTO daily_prices
      (location_id, upc, price_date, regular_price, promo_price, currency, price_source, raw_payload)
    VALUES {values_clause}
    ON CONFLICT (location_id, upc, price_date) DO UPDATE
      SET regular_price = EXCLUDED.regular_price,
          promo_price   = EXCLUDED.promo_price,
          currency      = EXCLUDED.currency,
          price_source  = EXCLUDED.price_source,
          raw_payload   = EXCLUDED.raw_payload
    """

    # Build a single VALUES list with (%s,...) placeholders repeated per row
    def exec_chunk(cur, chunk):
        placeholders = "(" + ",".join(["%s"] * 8) + ")"
        values_clause = ",".join([placeholders] * len(chunk))
        params = []
        for row in chunk:
            params.extend(row)
        cur.execute(base_sql.format(values_clause=values_clause), params)

    CHUNK = 500  # keep statements a reasonable size
    with conn.cursor() as cur:
        for i in range(0, len(data), CHUNK):
            exec_chunk(cur, data[i:i+CHUNK])

def log_request(conn, op, target, status_code, ok, message):
    with conn.cursor() as cur:
        cur.execute("""
          INSERT INTO request_log (op, target, status_code, ok, message)
          VALUES (%s, %s, %s, %s, %s)
        """, (op, target, status_code, ok, message[:9000] if message else None))
