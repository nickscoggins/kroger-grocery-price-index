import os
import base64
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

TOKEN_URL = "https://api.kroger.com/v1/connect/oauth2/token"

CLIENT_ID = os.environ["KROGER_CLIENT_ID"]
CLIENT_SECRET = os.environ["KROGER_CLIENT_SECRET"]

# Kroger uses Basic auth header + form grant_type=client_credentials & scope
BASIC = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def get_token():
    headers = {
        "Authorization": f"Basic {BASIC}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        # Some apps require an explicit scope. If your app has a default, you can omit.
        # If you have a specific products scope, set it here (e.g., "product.compact")
        "scope": "product.compact"
    }
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=20)
    resp.raise_for_status()
    j = resp.json()
    return j["access_token"], j.get("token_type", "Bearer")

