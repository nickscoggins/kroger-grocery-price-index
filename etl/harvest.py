# /etl/harvest.py
import os
import json
import math
import time
import hashlib
import datetime
import requests
import pytz
from typing import List, Tuple, Dict, Any, Iterable

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from etl.db import (
    get_conn,
    read_stores,
    read_products,   # returns (upc, pid, description) after your db.py tweak
    upsert_prices,
    log_request,
)
from etl.kroger_auth import get_token


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

API_BASE = "https://api.kroger.com/v1"
PRODUCTS_ENDPOINT = f"{API_BASE}/products"


# ---------------------------
# Utility / logging helpers
# ---------------------------

def info(msg: str) -> None:
    if LOG_LEVEL in ("INFO", "DEBUG"):
        print(msg, flush=True)

def debug(msg: str) -> None:
    if LOG_LEVEL == "DEBUG":
        print(msg, flush=True)

def today_et(tz_name: str = "America/New_York") -> datetime.date:
    tz = pytz.timezone(tz_name)
    return datetime.datetime.now(tz).date()

def upc_day_bucket(upc: str, buckets: int = 3) -> int:
    """
    Stable 0..(buckets-1) bucket for a given UPC.
    Used to deterministically split products across days.
    """
    h = hashlib.blake2b(upc.encode(), digest_size=4).digest()
    return int.from_bytes(h, "big") % buckets

def chunked(seq: Iterable, n: int) -> Iterable[List]:
    a = list(seq)
    for i in range(0, len(a), n):
        yield a[i:i+n]


# ---------------------------
# HTTP retry wrapper
# ---------------------------

class HttpRetryable(Exception):
    pass

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type(HttpRetryable),
)
def _get_with_retries(url: str, headers: Dict[str, str], params: Dict[str, str]) -> requests.Response:
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code in (429,) or 500 <= r.status_code < 600:
        # Retry on typical throttling/server errors
        raise HttpRetryable(f"retryable status {r.status_code}")
    return r


# ---------------------------
# Kroger fetch (by productId)
# ---------------------------

def fetch_store_prices_for_pids(
    token: str,
    location_id: str,
    pid_upc_pairs: List[Tuple[str, str]],
    batch_size: int = 40
) -> List[Tuple[str, Any, Any, Dict[str, Any]]]:
    """
    Calls GET /v1/products with:
      - filter.locationId=<store>
      - filter.productId=<pid1,pid2,...>  (comma-separated)
    Returns a list of tuples: (upc, regular_price, promo_price, raw_item)
    """
    headers = {"Authorization": f"Bearer {token}"}
    rows: List[Tuple[str, Any, Any, Dict[str, Any]]] = []

    for group in chunked(pid_upc_pairs, batch_size):
        pids = ",".join(pid for (pid, _upc) in group)
        params = {
            "filter.locationId": location_id,
            "filter.productId": pids,
        }

        resp = _get_with_retries(PRODUCTS_ENDPOINT, headers, params)
        raw_text = resp.text
        ok = resp.ok

        # Best-effort request logging
        try:
            with get_conn() as conn:
                log_request(
                    conn,
                    op="fetch_prices",
                    target=f"loc={location_id} pids={len(group)}",
                    status_code=resp.status_code,
                    ok=ok,
                    message=raw_text[:2000],
                )
        except Exception:
            # Don’t fail the ETL if logging fails
            pass

        if not ok:
            continue

        try:
            payload = resp.json()
        except Exception:
            payload = {"_parse_error": True, "_raw": raw_text}

        items = payload.get("data") or payload.get("items") or []
        # Map PID -> UPC for this group, in case response lacks UPC
        pid_to_upc = {pid: upc for (pid, upc) in group}

        for it in items:
            # Try to read identifiers flexibly
            pid = it.get("productId") or it.get("productID")
            upc = it.get("upc") or (pid_to_upc.get(pid) if pid else None)

            # Price may be under items[0].price or directly under price
            price_info = None
            if isinstance(it.get("items"), list) and it["items"]:
                price_info = it["items"][0].get("price")
            if price_info is None:
                price_info = it.get("price")

            regular = promo = None
            if isinstance(price_info, dict):
                regular = price_info.get("regular")
                promo = price_info.get("promo") or price_info.get("sale")
            elif isinstance(price_info, (int, float, str)):
                # If Kroger returns a scalar for some reason
                regular = price_info

            rows.append((upc, regular, promo, it))

    return rows


# ---------------------------
# Main
# ---------------------------

def main() -> None:
    price_date = today_et()
    info(f"[ETL] Starting harvest for {price_date}")

    # Safety switches (useful for first run / debugging)
    store_limit = int(os.environ.get("STORE_LIMIT", "0")) or None
    stop_after_requests = int(os.environ.get("STOP_AFTER_REQUESTS", "0")) or None
    dry_run = os.environ.get("DRY_RUN") == "1"

    token, token_type = get_token()
    info("[ETL] Got access token")

    # Read stores and products from DB
    with get_conn() as conn:
        stores = read_stores(conn)                 # List[str] of location_id
        prod_rows = read_products(conn)            # List[(upc, pid, description)]

    if store_limit:
        stores = stores[:store_limit]
        info(f"[ETL] STORE_LIMIT active → {len(stores)} stores will be processed")

    # Build today's product bucket (3-day split) *by UPC*, but we request with PID
    bucket_today = (price_date.toordinal() % 3)
    pid_upc_bucket: List[Tuple[str, str]] = []
    for (upc, pid, _desc) in prod_rows:
        if upc and pid and upc_day_bucket(upc) == bucket_today:
            pid_upc_bucket.append((pid, upc))

    calls_per_store = math.ceil(len(pid_upc_bucket) / 40) if pid_upc_bucket else 0
    info(f"[ETL] Products in today’s bucket: {len(pid_upc_bucket)} of {len(prod_rows)} total | ~{calls_per_store} calls/store")

    total_requests = 0
    total_upserts = 0

    # Process each store
    for i, loc in enumerate(stores, 1):
        if stop_after_requests and total_requests >= stop_after_requests:
            info(f"[ETL] STOP_AFTER_REQUESTS hit ({total_requests}); ending early")
            break

        pulled = fetch_store_prices_for_pids(token, loc, pid_upc_bucket, batch_size=40)
        # Estimate request count for bookkeeping
        total_requests += math.ceil(len(pid_upc_bucket) / 40)

        # Transform to DB rows
        to_upsert = []
        for (upc, regular, promo, raw) in pulled:
            if not upc:
                continue

            # Be defensive converting numeric-like strings
            def to_float_or_none(v):
                if v is None:
                    return None
                if isinstance(v, (int, float)):
                    return float(v)
                try:
                    return float(str(v))
                except Exception:
                    return None

            to_upsert.append({
                "location_id": loc,
                "upc": upc,
                "price_date": price_date,
                "regular_price": to_float_or_none(regular),
                "promo_price": to_float_or_none(promo),
                "currency": "USD",
                "price_source": "kroger_api",
                "raw_payload": json.dumps(raw),
            })

        # Write to DB unless dry run
        if to_upsert and not dry_run:
            with get_conn() as conn:
                upsert_prices(conn, to_upsert)
                total_upserts += len(to_upsert)

        if i % 50 == 0:
            info(f"[ETL] {i}/{len(stores)} stores | ~requests so far: {total_requests} | rows upserted: {total_upserts}")

        # Gentle pacing; adjust if needed
        time.sleep(0.05)

    info(
        f"[ETL] Done. Stores processed: {len(stores)} | "
        f"Est. requests: ~{total_requests} | Rows upserted: {total_upserts} | Dry-run={dry_run}"
    )


if __name__ == "__main__":
    main()
