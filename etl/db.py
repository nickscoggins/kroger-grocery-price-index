import os
import psycopg
from contextlib import contextmanager

DB_URL = os.environ["SUPABASE_DB_URL"]

@contextmanager
def get_conn():
    with psycopg.connect(DB_URL, autocommit=True) as conn:
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
        return cur.fetchall()  # list of (upc, pid, description)

def upsert_prices(conn, rows):
    """
    rows: list of dicts with keys:
      location_id, upc, price_date, regular_price, promo_price, currency, price_source, raw_payload
    """
    if not rows:
        return
    with conn.cursor() as cur:
        cur.execute("set session characteristics as transaction read write")
        sql = """
        insert into daily_prices
          (location_id, upc, price_date, regular_price, promo_price, currency, price_source, raw_payload)
        values (%(location_id)s, %(upc)s, %(price_date)s, %(regular_price)s, %(promo_price)s, %(currency)s, %(price_source)s, %(raw_payload)s)
        on conflict (location_id, upc, price_date) do update
        set regular_price = excluded.regular_price,
            promo_price   = excluded.promo_price,
            currency      = excluded.currency,
            price_source  = excluded.price_source,
            raw_payload   = excluded.raw_payload
        """
        cur.executemany(sql, rows)

def log_request(conn, op, target, status_code, ok, message):
    with conn.cursor() as cur:
        cur.execute("""
          insert into request_log (op, target, status_code, ok, message)
          values (%s, %s, %s, %s, %s)
        """, (op, target, status_code, ok, message[:9000] if message else None))
