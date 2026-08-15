"""
TDX 公車即時位置 - 原始資料收集器

執行：
    uv run python -m trafficproject.collector
"""

import os
import time
import json
import gzip
import shutil
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from trafficproject.paths import PROJECT_ROOT, RAW_DIR
from trafficproject.logging_util import make_logger

log = make_logger("collector")

# ---------- 設定 ----------
load_dotenv(PROJECT_ROOT / ".env")
CLIENT_ID = os.environ["TDX_CLIENT_ID"]
CLIENT_SECRET = os.environ["TDX_CLIENT_SECRET"]

CITIES = ["Taipei", "NewTaipei"]
INTERVAL = 5

DISK_WARN_GB = 10          # 低於此值告警
DISK_CHECK_EVERY = 360     # 每幾輪檢查一次（5 秒一輪 → 約 30 分鐘）
PAGE_SIZE = 3000
ORDER_BY = {"StopOfRoute": "SubRouteUID", "Station": "StationID"}

AUTH_URL = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
API_STOP_OF_ROUTE = "https://tdx.transportdata.tw/api/basic/v2/Bus/StopOfRoute/City"
API_STATION = "https://tdx.transportdata.tw/api/basic/v2/Bus/Station/City"
API_BUS = "https://tdx.transportdata.tw/api/basic/v2/Bus/"

TPE = timezone(timedelta(hours=8))

# ---------- Token 快取 ----------
_token = None
_token_expire_at = 0


def get_token():
    """只在快過期時才重新換 token，保留 60 秒緩衝避開邊界。"""
    global _token, _token_expire_at
    print(f"this is the token: {_token}")
    if _token and time.time() < _token_expire_at - 60:
        return _token

    resp = requests.post(
        AUTH_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    _token = payload["access_token"]
    _token_expire_at = time.time() + payload["expires_in"]
    log("token refreshed")
    return _token

def fetch(city, endpoint, top, skip, orderby):
    print(f"{API_BUS}{endpoint}/city/{city}")
    resp = requests.get(
        f"{API_BUS}{endpoint}/City/{city}",
        params={"$format": "JSON", "top": top, "$skip": skip, "$orderby": orderby},
        headers={
            "Authorization": f"Bearer {get_token()}",
            "Accept-Encoding": "gzip",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

def fetch_all(city, endpoint):
    records, skip = [], 0
    while True:
        batch = fetch(city, endpoint, top=PAGE_SIZE, skip=skip,
                      orderby=ORDER_BY[endpoint])
        records.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        if skip > 100_000:
            raise RuntimeError(f"{city}/{endpoint} 分頁異常")
    return records

def save(city, prefix, data):
    now = datetime.now(TPE)
    folder = RAW_DIR / "static" / now.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)      # 每圈都建，跨日才不會死
    path = folder / (f"{prefix}_{city}" + ".json.gz")
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return path, len(data)


def check_disk():
    """磁碟滿不會自己好，必須告警而非安靜重試。"""
    free_gb = shutil.disk_usage(RAW_DIR).free / 1e9
    if free_gb < DISK_WARN_GB:
        log(f"DISK LOW: {free_gb:.1f} GB free")
    return free_gb


def main():
    # log(f"start collecting: {CITIES}, every {INTERVAL}s -> {RAW_DIR}")
    # log(f"disk free: {shutil.disk_usage(RAW_DIR).free / 1e9:.1f} GB")
    
    for city in CITIES:

        try:
            data = fetch_all(city, "StopOfRoute")
            path, n = save(city, "StopOfRoute", data)
            log(f"{city}: {n} records -> {path.relative_to(RAW_DIR.parent)}")

            data = fetch_all(city, "Station")
            path, n = save(city, "Station", data)
            log(f"{city}: {n} records -> {path.relative_to(RAW_DIR.parent)}")
        except requests.HTTPError as e:
            log(f"{city}: HTTP {e.response.status_code} {e.response.text[:200]}")
        except OSError as e:
            # 寫檔失敗（磁碟滿、權限）—— 不會自己好，要吵
            log(f"{city}: DISK/IO ERROR {type(e).__name__}: {e}")
            check_disk()
        except Exception as e:
            log(f"{city}: FAILED {type(e).__name__}: {e}")



if __name__ == "__main__":
    main()