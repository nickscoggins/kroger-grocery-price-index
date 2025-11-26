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
    """
    Read location_id + Kroger ID (?) from stores table.
    """
    with conn.cursor() as cur:
        cur.execute("""
            select
              location_id,
              kroger_location_id
            from stores
            where kroger_location_id is not null
            order by location_id
        """)
        return cur.fetchall()  # (location_id, kroger_location_id)

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

    In addition to populating daily_prices, this will also maintain the
    latest_prices summary table (one row per location_id + upc) so that
    the frontend can query fast, pre-aggregated "latest" prices.
    """
    if not rows:
        return

    # Convert dicts -> tuples in the right column order for daily_prices
    daily_data = [
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

    daily_sql = """
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

    # For latest_prices we only need one row per (location_id, upc), and we only
    # want to update if the incoming price_date is newer or equal to the stored one.
    latest_sql = """
    INSERT INTO latest_prices
      (location_id, upc, price_date, regular_price, promo_price, currency)
    VALUES {values_clause}
    ON CONFLICT (location_id, upc) DO UPDATE
      SET price_date    = EXCLUDED.price_date,
          regular_price = EXCLUDED.regular_price,
          promo_price   = EXCLUDED.promo_price,
          currency      = EXCLUDED.currency
      WHERE EXCLUDED.price_date >= latest_prices.price_date
    """

    def exec_chunk(cur, daily_chunk):
        # --- upsert into daily_prices ---
        daily_placeholders = "(" + ",".join(["%s"] * 8) + ")"
        daily_values_clause = ",".join([daily_placeholders] * len(daily_chunk))
        daily_params = []
        for row in daily_chunk:
            daily_params.extend(row)
        cur.execute(daily_sql.format(values_clause=daily_values_clause), daily_params)

        # --- upsert into latest_prices ---
        # Slice the 8-tuple (loc, upc, date, reg, promo, curr, source, raw)
        # down to the 6 fields needed by latest_prices.
        latest_chunk = [
            (loc, upc, price_date, regular, promo, currency)
            for (loc, upc, price_date, regular, promo, currency, _source, _raw) in daily_chunk
        ]
        latest_placeholders = "(" + ",".join(["%s"] * 6) + ")"
        latest_values_clause = ",".join([latest_placeholders] * len(latest_chunk))
        latest_params = []
        for row in latest_chunk:
            latest_params.extend(row)
        cur.execute(latest_sql.format(values_clause=latest_values_clause), latest_params)

    CHUNK = 500  # keep statements a reasonable size
    with conn.cursor() as cur:
        for i in range(0, len(daily_data), CHUNK):
            exec_chunk(cur, daily_data[i:i+CHUNK])

def log_request(conn, op, target, status_code, ok, message):
    with conn.cursor() as cur:
        cur.execute("""
          INSERT INTO request_log (op, target, status_code, ok, message)
          VALUES (%s, %s, %s, %s, %s)
        """, (op, target, status_code, ok, message[:9000] if message else None))
