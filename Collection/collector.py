import os
import time
import json
import gzip
import requests
from datetime import datetime, timezone, timedelta

# Settings
CLIENT_ID = os.environ["TDX_CLIENT_ID"]
CLIENT_SECRET = os.environ["TDX_CLIENT_SECRET"]

CITIES = ["Taipei", "NewTaipei"]
INTERVAL = 5
RAW_DIR = "raw"

AUTH_URL = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
API_BASE = "https://tdx.transportdata.tw/api/basic/v2/Bus/RealTimeByFrequency/City"

TPE = timezone(timedelta(hours=8))

# Token cache
_token = None
_token_expire_at = 0

def get_token():
    global _token, _token_expire_at
    if _token and time.time() < _token_expire_at - 60:
        return _token

    resp = requests.post(
        AUTH_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        },
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    _token = payload["access_token"]
    _token_expire_at = time.time() + payload["expires_in"]
    log("token refreshed")
    return _token

def log(msg):
    line = f"[{datetime.now(TPE).isoformat(timespec='seconds')}] {msg}" 
    print(line, flush=True)
    with open("collector.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")

def fetch(city):
    resp = requests.get(
        f"{API_BASE}/{city}",
        params={"$format": "JSON"},
        headers={
            "Authorization": f"Bearer {get_token()}",
            "Accept-Encoding": "gzip"
        },
        timeout=30,
    )

    resp.raise_for_status()
    return resp.json()

def save(city, data):
    now = datetime.now(TPE)
    folder = os.path.join(RAW_DIR, city, now.strftime("%Y-%m-%d"))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, now.strftime("%H%M%S") + ".json.gz")
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return path, len(data)

def main():
    log(f"start collecting: {CITIES}, every {INTERVAL}s")
    i = 0
    while True:
        city = CITIES[i % len(CITIES)]
        cycle_start = time.time()
        
        try:
            data = fetch(city)
            path, n = save(city, data)
            log(f"{city}: {n} records -> {path}")
        except requests.HTTPError as e:
            log(f"{city}: HTTP {e.response.status_code} {e.response.text[:200]}")
        except Exception as e:
            log(f"{city}: FAILED {type(e).__name__}: {e}")

        i += 1
        elapsed = time.time() - cycle_start
        time.sleep(max(0, INTERVAL - elapsed))
if __name__ == "__main__":
    main()