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
    read_products,   # returns (upc, pid, description)
    upsert_prices,
    log_request,
)
from etl.kroger_auth import TokenManager


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

API_BASE = "https://api.kroger.com/v1"
PRODUCTS_ENDPOINT = f"{API_BASE}/products"
BATCH_SIZE = 49  # <= 49 keeps us under Kroger’s per-request max and yields ~3 calls/store

def info(msg: str) -> None:
    if LOG_LEVEL in ("INFO", "DEBUG"):
        print(msg, flush=True)

def debug(msg: str) -> None:
    if LOG_LEVEL == "DEBUG":
        print(msg, flush=True)

def today_et(tz_name: str = "US/Eastern") -> datetime.date:
    """
    Return "today" in Eastern Time as a date object.
    """
    tz = pytz.timezone(tz_name)
    return datetime.datetime.now(tz).date()

def chunked(seq: Iterable, n: int) -> Iterable[List]:
    a = list(seq)
    for i in range(0, len(a), n):
        yield a[i:i+n]

class HttpRetryable(Exception): pass

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type(HttpRetryable),
)
def _get_with_retries(url: str, headers: Dict[str, str], params: Dict[str, str]) -> requests.Response:
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code in (429,) or 500 <= r.status_code < 600:
        raise HttpRetryable(f"retryable status {r.status_code}")
    return r

def fetch_store_prices_for_pids(
    tm: TokenManager,
    location_id: str,
    pid_upc_pairs: List[Tuple[str, str]],
    batch_size: int = BATCH_SIZE
) -> List[Tuple[str, Any, Any, Dict[str, Any]]]:
    """
    GET /v1/products?filter.locationId=<store>&filter.productId=p1,p2,...
    Returns list of (upc, regular_price, promo_price, raw_item)
    """
    rows: List[Tuple[str, Any, Any, Dict[str, Any]]] = []

    for group in chunked(pid_upc_pairs, batch_size):
        pids = [pid for (pid, _upc) in group]
        if not pids:
            continue

        token = tm.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        params = {
            "filter.locationId": location_id,
            "filter.productId": ",".join(pids),
            "filter.limit": str(len(pids)),
        }

        total_requests = 0

        try:
            resp = _get_with_retries(PRODUCTS_ENDPOINT, headers=headers, params=params)
            total_requests += 1
        except HttpRetryable as exc:
            info(f"[WARN] HTTP retryable error for store={location_id}: {exc}")
            raise
        except Exception as exc:
            info(f"[ERROR] Non-retryable error for store={location_id}: {exc}")
            continue

        status = resp.status_code
        raw_text = resp.text

        if status != 200:
            info(f"[WARN] Non-200 response for store={location_id}: {status}")
            try:
                payload = resp.json()
            except Exception:
                payload = {"_parse_error": True, "_raw": raw_text}

            with get_conn() as conn:
                log_request(
                    conn,
                    op="fetch_store_prices",
                    target=f"store={location_id},pids={len(pids)}",
                    status_code=status,
                    ok=False,
                    message=json.dumps(payload)[:9000],
                )
            continue

        try:
            payload = resp.json()
        except Exception:
            payload = {"_parse_error": True, "_raw": raw_text}

        items = payload.get("data") or payload.get("items") or []
        pid_to_upc = {pid: upc for (pid, upc) in group}

        for it in items:
            pid = it.get("productId") or it.get("productID")
            upc = it.get("upc") or (pid_to_upc.get(pid) if pid else None)

            price_info = None
            if isinstance(it.get("items"), list) and it["items"]:
                price_info = it["items"][0].get("price")
            if price_info is None:
                price_info = it.get("price")

            regular = promo = None
            if isinstance(price_info, dict):
                regular = price_info.get("regular")
                promo   = price_info.get("promo") or price_info.get("sale")
            elif isinstance(price_info, (int, float, str)):
                regular = price_info

            rows.append((upc, regular, promo, it))

    return rows

def stable_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def main():
    # date for which we're harvesting prices
    price_date = today_et()

    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"
    tm = TokenManager()

    info(f"[ETL] Starting harvest for {price_date} (dry_run={dry_run})")

    with get_conn() as conn:
        stores = read_stores(conn)
        products = read_products(conn)

    info(f"[ETL] Loaded {len(stores)} stores and {len(products)} products with PIDs")

    # simple deterministic sharding: we may only want to fetch a subset of products per day
    # For example: shard into 3 cohorts and pick one cohort per day-of-month:
    # day 1 => cohort 0, day 2 => cohort 1, day 3 => cohort 2, etc.
    shard_count = int(os.environ.get("SHARD_COUNT", "3"))  # e.g., 3 cohorts
    shard_index = int(os.environ.get("SHARD_INDEX", "0"))  # which cohort this job handles

    info(f"[ETL] Using shard_count={shard_count}, shard_index={shard_index}")

    # partition products into cohorts based on hash(upc)
    cohorts: Dict[int, List[Tuple[str, str]]] = {i: [] for i in range(shard_count)}
    for (upc, pid, desc) in products:
        # stable hash of UPC → integer → mod shard_count
        h = int(stable_hash(upc), 16)
        cohort_id = h % shard_count
        cohorts[cohort_id].append((upc, pid))

    selected_cohort_index = shard_index % shard_count
    selected_products = cohorts[selected_cohort_index]

    info(f"[ETL] Selected cohort {selected_cohort_index} with {len(selected_products)} products")

    total_requests = 0
    total_upserts = 0

    for i, (loc, location_id) in enumerate(stores, start=1):
        # build pairs of (pid, upc) for this store
        # we don't need description here
        pid_upc_pairs = [(pid, upc) for (upc, pid) in selected_products if pid]

        if not pid_upc_pairs:
            debug(f"[ETL] Store={loc} has no products in this cohort.")
            continue

        # fetch prices for this store's product cohort
        try:
            pulled = fetch_store_prices_for_pids(tm, location_id, pid_upc_pairs)
        except Exception as exc:
            info(f"[ERROR] Failed to fetch prices for store={loc}: {exc}")
            with get_conn() as conn:
                log_request(
                    conn,
                    op="fetch_store_prices_for_pids",
                    target=f"store={loc}",
                    status_code=None,
                    ok=False,
                    message=str(exc),
                )
            continue

        total_requests += math.ceil(len(pid_upc_pairs) / BATCH_SIZE)

        to_upsert = []
        for (upc, regular, promo, raw) in pulled:
            if not upc:
                continue
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

        if to_upsert and not dry_run:
            with get_conn() as conn:
                upsert_prices(conn, to_upsert)
                total_upserts += len(to_upsert)

        if i % 1 == 0:
            info(f"[ETL] {i}/{len(stores)} stores | ~requests so far: {total_requests} | rows upserted: {total_upserts}")

        time.sleep(0.05)  # small pacing

    info(
        f"[ETL] Done. Stores processed: {len(stores)} | "
        f"Est. requests: ~{total_requests} | Rows upserted: {total_upserts} | Dry-run={dry_run}"
    )

if __name__ == "__main__":
    main()
