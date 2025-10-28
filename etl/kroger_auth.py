# /etl/kroger_auth.py
import os
import time
import base64
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

TOKEN_URL = "https://api.kroger.com/v1/connect/oauth2/token"

CLIENT_ID = os.environ["KROGER_CLIENT_ID"]
CLIENT_SECRET = os.environ["KROGER_CLIENT_SECRET"]

BASIC = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def _request_token():
    headers = {
        "Authorization": f"Basic {BASIC}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "product.compact"
    }
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=20)
    resp.raise_for_status()
    j = resp.json()
    # returns (access_token, token_type, expires_in_seconds)
    return j["access_token"], j.get("token_type", "Bearer"), int(j.get("expires_in", 1800))

def get_token():
    """Kept for backward compatibility; returns (token, type)."""
    tok, typ, _exp = _request_token()
    return tok, typ

class TokenManager:
    """
    Lightweight token refresher for client_credentials.
    - Refreshes automatically when <=5 minutes remain
    - Can be forced to refresh after a 401
    """
    def __init__(self, refresh_buffer_seconds: int = 300):
        self.access_token = None
        self.token_type = "Bearer"
        self.expiry_ts = 0.0
        self.refresh_buffer = refresh_buffer_seconds

    def _needs_refresh(self) -> bool:
        return (not self.access_token) or (time.time() >= (self.expiry_ts - self.refresh_buffer))

    def refresh(self):
        tok, typ, exp = _request_token()
        self.access_token = tok
        self.token_type = typ or "Bearer"
        self.expiry_ts = time.time() + max(60, int(exp))  # never trust tiny exp
        return self.access_token

    def get(self) -> str:
        if self._needs_refresh():
            self.refresh()
        return self.access_token
