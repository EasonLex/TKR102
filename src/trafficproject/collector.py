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
import signal
import traceback
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from confluent_kafka import Producer
import threading

from trafficproject.paths import PROJECT_ROOT, RAW_DIR, LOG_DIR
from trafficproject.logging_util import make_logger

log = make_logger("collector")

# ---------- 設定 ----------

load_dotenv(PROJECT_ROOT / ".env")
CLIENT_ID = os.environ["TDX_CLIENT_ID"]
CLIENT_SECRET = os.environ["TDX_CLIENT_SECRET"]
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]

CITIES = ["Taipei", "NewTaipei"]
INTERVAL = 5
NIGHT_INTERVAL = 15

DISK_WARN_GB = 10          # 低於此值告警
DISK_CHECK_EVERY = 360     # 每幾輪檢查一次（5 秒一輪 → 約 30 分鐘）

AUTH_URL = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
API_BASE = "https://tdx.transportdata.tw/api/basic/v2/Bus/RealTimeByFrequency/City"

TPE = timezone(timedelta(hours=8))
LOG_PATH = LOG_DIR / "collector.log"

TOPIC = "bus.position.raw"
# TOPIC_B = "bus.position.raw.default"

def _error_cb(err):
    log(f"Kafka error: {err}")

config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,     # 從 .env 讀
    'client.id': 'collector-a',
    'compression.type': 'zstd',               # 跟 topic 設定一致，避免 broker 重壓
    'batch.size': 1048576,                    # 位元組，不是 batch.num.messages
    'linger.ms': 50,
    'error_cb': _error_cb,
    'message.timeout.ms': 300000,     # 測試期間縮短，才不用等五分鐘
}
producer = Producer(config)

# config_b = {
#     'bootstrap.servers': KAFKA_BOOTSTRAP,
#     'client.id': 'collector-b',
#     'compression.type': 'zstd',
#     'batch.size': 16384,      # 16 KB —— 明確設小，才有對照
#     'linger.ms': 0,
# }
# producer_b = Producer(config_b)

# ---------- Token 快取 ----------
_token = None
_token_expire_at = 0
_delivered = 0
_failed = 0

_stop_event = threading.Event()

def _stop(signum, frame):
    _stop_event.set()
    log(f"收到訊號 {signum}，準備關閉")

def current_interval():
    hour = datetime.now(TPE).hour
    return NIGHT_INTERVAL if (hour >= 22 or hour < 6) else INTERVAL

def get_token():
    """只在快過期時才重新換 token，保留 60 秒緩衝避開邊界。"""
    global _token, _token_expire_at
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


def fetch(city):
    resp = requests.get(
        f"{API_BASE}/{city}",
        params={"$format": "JSON"},
        headers={
            "Authorization": f"Bearer {get_token()}",
            "Accept-Encoding": "gzip",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json(), datetime.now(TPE).isoformat(timespec="milliseconds")


def save(city, data):
    now = datetime.now(TPE)
    folder = RAW_DIR / city / now.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)      # 每圈都建，跨日才不會死
    path = folder / (now.strftime("%H%M%S") + ".json.gz")
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return path, len(data)

def send_producer(city, data, fetch_time):
    produce_time = datetime.now(TPE).isoformat(timespec="milliseconds")
    for rec in data:
        # 不要改 rec 本身 —— 見下方說明
        payload = json.dumps(
            {**rec, "city": city,
             "fetch_time": fetch_time,
             "produce_time": produce_time},
            ensure_ascii=False,
        ).encode()
        key = rec.get("PlateNumb") or None

        try:
            producer.produce(TOPIC, key=key, value=payload, on_delivery=delivery_report)
        except BufferError:
            producer.poll(0.5)
            producer.produce(TOPIC, key=key, value=payload, on_delivery=delivery_report)

def delivery_report(err, msg):
    global _delivered, _failed
    if err is not None:
        _failed += 1
        if _failed % 100 == 1:          # 節流，不要洗版
            log(f"訊息發送失敗（累計 {_failed}）: {err}")
    else:
        _delivered += 1

def check_disk():
    """磁碟滿不會自己好，必須告警而非安靜重試。"""
    free_gb = shutil.disk_usage(RAW_DIR).free / 1e9
    if free_gb < DISK_WARN_GB:
        log(f"DISK LOW: {free_gb:.1f} GB free")
    return free_gb


def main():
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    log(f"kafka bootstrap = {KAFKA_BOOTSTRAP}")
    log(f"start collecting: {CITIES}, day={INTERVAL}s night={NIGHT_INTERVAL}s")
    log(f"disk free: {shutil.disk_usage(RAW_DIR).free / 1e9:.1f} GB")

    i = 0

    try:
        while not _stop_event.is_set():
            interval = current_interval()
            city = CITIES[i % len(CITIES)]
            cycle_start = time.time()
    
            if i % DISK_CHECK_EVERY == 0:
                check_disk()
    
            try:
                data, fetch_time = fetch(city)
                path, n = save(city, data)
                send_producer(city, data, fetch_time)
                log(f"{city}: {n} records -> {path.relative_to(RAW_DIR.parent)} "
                    f"| kafka ok={_delivered} fail={_failed}")
            except requests.HTTPError as e:
                log(f"{city}: HTTP {e.response.status_code} {e.response.text[:200]}")
            except OSError as e:
                # 寫檔失敗（磁碟滿、權限）—— 不會自己好，要吵
                log(f"{city}: DISK/IO ERROR {type(e).__name__}: {e}")
                check_disk()
            except Exception as e:
                log(f"{city}: FAILED {type(e).__name__}: {e}\n{traceback.format_exc()}")
    
            i += 1
            elapsed = time.time() - cycle_start
            _stop_event.wait(max(0, interval - elapsed))
            producer.poll(0)
    finally:
        remaining = producer.flush(30)
        if remaining:
            log(f"⚠️ 關閉時仍有 {remaining} 則未送出")
        log("collector stopped")


if __name__ == "__main__":
    main()