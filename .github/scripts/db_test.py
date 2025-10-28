import os, psycopg
url = os.environ["SUPABASE_DB_URL"]
safe = url.split("@")[1].split("?")[0] if "@" in url else "<hidden>"
print(f"Connecting to: {safe}")
with psycopg.connect(url, autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute("select current_date, current_user")
        print("Basic query:", cur.fetchone())
        for t in ("stores","products","daily_prices","request_log"):
            cur.execute(f"select to_regclass('{t}')")
            print(f"Table {t} exists?:", cur.fetchone()[0] is not None)
        for t in ("stores","products"):
            cur.execute(f"select count(*) from {t}")
            print(f"Count {t}:", cur.fetchone()[0])
print("DB connection OK")
