import os, json, math, time, hashlib, datetime
import pytz
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from etl.db import get_conn, read_stores, read_products, upsert_prices, log_request
from etl.kroger_auth import get_token

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

API_BASE = "https://api.kroger.com/v1"
PRODUCTS_ENDPOINT = f"{API_BASE}/products"

# --- Helpers ---

def today_et(tz_name="America/New_York"):
    tz = pytz.timezone(tz_name)
    return datetime.datetime.now(tz).date()

def upc_day_bucket(upc: str, buckets=3) -> int:
    # stable bucket 0,1,2 for a given UPC
    h = hashlib.blake2b(upc.encode(), digest_size=4).digest()
    return int.from_bytes(h, "big") % buckets

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

class HttpRetryable(Exception): pass

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type(HttpRetryable),
)
def _get_with_retries(url, headers, params):
    r = requests.get(url, headers=headers, params=params, timeout=30)
    # Retry on 429 and 5xx
    if r.status_code in (429,) or 500 <= r.status_code < 600:
        raise HttpRetryable(f"retryable status {r.status_code}")
    return r

def fetch_store_prices_for_upcs(token: str, location_id: str, upcs):
    """
    Calls GET /v1/products with filter.locationId and multiple filter.upc params.
    Up to 49 UPCs per request.
    Returns list of parsed price rows.
    """
    headers = {"Authorization": f"Bearer {token}"}
    rows = []
    for group in chunked(upcs, 49):
        params = [("filter.locationId", location_id)]
        for u in group:
            params.append(("filter.upc", u))
        # You can also include "filter.limit" but with explicit UPCs it's ignored.
        resp = _get_with_retries(PRODUCTS_ENDPOINT, headers, params)
        data_text = resp.text
        ok = resp.ok
        try:
            data = resp.json()
        except Exception:
            data = {"_parse_error": True, "_raw": data_text}

        # Log the request (best-effort)
        try:
            with get_conn() as conn:
                log_request(conn, "fetch_prices", f"loc={location_id} upcs={len(group)}",
                            resp.status_code, ok, data_text[:2000])
        except Exception:
            pass  # don’t fail ETL on logging error

        if not ok:
            continue

        items = data.get("data") or data.get("items") or []  # API varies in examples
        for it in items:
            # Defensive parsing for price fields; adjust keys if your payload differs
            upc = it.get("upc") or (it.get("productId") or "").strip()
            price_info = it.get("price") or it.get("items", [{}])[0].get("price") if it.get("items") else {}
            regular = None
            promo   = None
            if isinstance(price_info, dict):
                regular = price_info.get("regular")
                promo   = price_info.get("promo") or price_info.get("sale")
            elif isinstance(price_info, (int, float, str)):
                # fallback if API returns a scalar
                regular = price_info

            rows.append((upc, regular, promo, it))
    return rows

def info(msg): 
    if LOG_LEVEL in ("INFO","DEBUG"):
        print(msg, flush=True)

def debug(msg):
    if LOG_LEVEL == "DEBUG":
        print(msg, flush=True)

# --- Main ---

def main():
    price_date = today_et()
    info(f"[ETL] Starting harvest for {price_date}")

    # NEW: temp safety switches
    store_limit = int(os.environ.get("STORE_LIMIT", "0")) or None
    stop_after_requests = int(os.environ.get("STOP_AFTER_REQUESTS", "0")) or None
    dry_run = os.environ.get("DRY_RUN") == "1"

    token, token_type = get_token()
    info("[ETL] Got access token")

    with get_conn() as conn:
        stores = read_stores(conn)            # list of location_id
        prod_rows = read_products(conn)       # list of (upc, description)

    if store_limit:
        stores = stores[:store_limit]
        info(f"[ETL] STORE_LIMIT active → {len(stores)} stores will be processed")

    # 3-day product sharding
    bucket_today = (price_date.toordinal() % 3)
    upcs_today = [u for (u, _desc) in prod_rows if upc_day_bucket(u) == bucket_today]
    calls_per_store = math.ceil(len(upcs_today)/49) if upcs_today else 0
    info(f"[ETL] Today’s bucket: {len(upcs_today)} UPCs; ~{calls_per_store} calls/store")

    total_requests = 0
    total_upserts  = 0

    for i, loc in enumerate(stores, 1):
        if stop_after_requests and total_requests >= stop_after_requests:
            info(f"[ETL] STOP_AFTER_REQUESTS hit ({total_requests}); ending early")
            break

        pulled = fetch_store_prices_for_upcs(token, loc, upcs_today)
        total_requests += math.ceil(len(upcs_today)/49)

        to_upsert = []
        for (upc, regular, promo, raw) in pulled:
            if not upc:
                continue
            to_upsert.append({
                "location_id": loc,
                "upc": upc,
                "price_date": price_date,
                "regular_price": regular if regular is None or isinstance(regular, (int, float)) else float(regular),
                "promo_price":   promo   if promo   is None or isinstance(promo,   (int, float)) else float(promo),
                "currency": "USD",
                "price_source": "kroger_api",
                "raw_payload": json.dumps(raw),
            })

        if to_upsert and not dry_run:
            with get_conn() as conn:
                upsert_prices(conn, to_upsert)
                total_upserts += len(to_upsert)

        if i % 1 == 0:
            info(f"[ETL] {i}/{len(stores)} stores | ~requests: {total_requests} | upserts: {total_upserts}")

        time.sleep(0.05)  # gentle pacing

    info(f"[ETL] Done. Stores: {min(len(stores), store_limit or len(stores))} | Est. requests: ~{total_requests} | Rows upserted: {total_upserts} | Dry-run={dry_run}")

if __name__ == "__main__":
    main()
