import os, base64, requests
cid = os.environ["KROGER_CLIENT_ID"]
cs  = os.environ["KROGER_CLIENT_SECRET"]
auth = base64.b64encode(f"{cid}:{cs}".encode()).decode()
headers = {
    "Authorization": f"Basic {auth}",
    "Content-Type": "application/x-www-form-urlencoded",
}
data = {"grant_type":"client_credentials", "scope":"product.compact"}
r = requests.post("https://api.kroger.com/v1/connect/oauth2/token",
                  headers=headers, data=data, timeout=20)
print("HTTP status:", r.status_code)
r.raise_for_status()
j = r.json()
print("Received token_type:", j.get("token_type"), "| expires_in (s):", j.get("expires_in"))
print("Kroger auth OK")
