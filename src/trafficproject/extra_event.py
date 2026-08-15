"""
到站事件抽取 —— 從 GPS 觀測推導出「某台車某時刻到達某站」

輸入：output/parquet/{city}/{date}.parquet   （原始觀測）
輸出：output/events/{city}/{date}.parquet    （到站事件）

判定規則 v1（2026-08-12 定案）
------------------------------------------------------------
  1. duty_status == 1               僅營運中的觀測
  2. 距最近站牌 <= 100m             到站門檻
  3. 連續同站序合併為一事件；間隔 >120s 則切分
  4. 站序大幅下降(>5) 或 間隔 >30min → 新的一趟

已知限制（實測 EAL-2088 / TPE101320 / 2026-08-06，89% 準確）
------------------------------------------------------------
  * 站間距 <200m 處會重複計算（如 seq 17/18 相距 156m）
  * 過站不停 + 10 秒取樣造成隨機漏抓（如 seq 11/13）
  * 待改進：改用「每趟每站的距離局部最小值」，可免除門檻

效能
------------------------------------------------------------
  瓶頸在讀檔而非計算：逐路線讀檔 1027 次 = 34 分鐘，
  改為讀一次後分組處理 + 只讀必要欄位 → 目標 1 分鐘內。
"""

import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pymongo import MongoClient

from trafficproject.paths import PARQUET_DIR, OUTPUT_DIR
from trafficproject.logging_util import make_logger

log = make_logger("extract_events")
TPE = "Asia/Taipei"

# ---- 判定參數（集中在此，方便日後調整與記錄） ----
DIST_THRESHOLD_M = 100      # 到站距離門檻
EVENT_GAP_S = 120           # 同站序間隔超過此值視為不同事件
TRIP_SEQ_DROP = 5           # 站序下降超過此值視為新的一趟
TRIP_GAP_MIN = 30           # 事件間隔超過此值視為新的一趟

# 只讀必要欄位 —— Parquet 欄式儲存的核心優勢
NEEDED_COLS = [
    "plate_numb", "sub_route_uid", "direction",
    "gps_time", "lat", "lon", "speed", "duty_status",
]


# ------------------------------------------------------------
# 站序資料
# ------------------------------------------------------------
def load_all_stops():
    """一次把所有現行版本的站序讀進記憶體，避免逐條查 MongoDB。"""
    db = MongoClient(os.environ["MONGO_URI"]).tdx
    out = {}
    for doc in db.route_stops.find(
        {"valid_to": None},
        {"sub_route_uid": 1, "direction": 1, "version_id": 1, "stops": 1, "_id": 0},
    ):
        stops = sorted(doc["stops"], key=lambda s: s["seq"])
        out[(doc["sub_route_uid"], doc["direction"])] = {
            "version_id": doc["version_id"],
            "seq": np.array([s["seq"] for s in stops]),
            "station_id": np.array([s["station_id"] for s in stops]),
            "name": np.array([s["name"] for s in stops]),
            "boarding": np.array([s["boarding"] for s in stops]),
            "lat": np.array([s["lat"] for s in stops], dtype=float),
            "lon": np.array([s["lon"] for s in stops], dtype=float),
        }
    log(f"載入站序 {len(out)} 條子路線")
    return out


# ------------------------------------------------------------
# 距離
# ------------------------------------------------------------
def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


# ------------------------------------------------------------
# 單一 (子路線, 方向) 的事件抽取
# ------------------------------------------------------------
def extract_route(group, stops):
    """
    group: 同一 (sub_route_uid, direction) 的所有觀測，已濾 duty_status
    stops: load_all_stops() 的其中一項
    """
    if group.empty:
        return None

    # 每筆觀測 → 最近站牌（向量化：n_obs × n_stops）
    d = haversine_m(
        group["lat"].values[:, None], group["lon"].values[:, None],
        stops["lat"][None, :], stops["lon"][None, :],
    )
    idx = d.argmin(axis=1)
    near_dist = d[np.arange(len(group)), idx]

    g = group.assign(
        near_idx=idx,
        near_seq=stops["seq"][idx],
        near_dist=near_dist,
    )

    # 門檻篩選
    g = g[g["near_dist"] <= DIST_THRESHOLD_M]
    if g.empty:
        return None

    out = []
    for plate, tr in g.groupby("plate_numb", sort=False):
        tr = tr.sort_values("t")

        # --- 合併連續觀測為事件 ---
        new_event = (
            (tr["near_seq"] != tr["near_seq"].shift())
            | (tr["t"].diff().dt.total_seconds() > EVENT_GAP_S)
        )
        tr = tr.assign(event=new_event.cumsum())

        ev = tr.groupby("event").agg(
            near_idx=("near_idx", "first"),
            seq=("near_seq", "first"),
            arrival_time=("t", "min"),
            last_t=("t", "max"),
            n_obs=("t", "size"),
            min_dist=("near_dist", "min"),
            min_speed=("speed", "min"),
        ).reset_index(drop=True)

        if ev.empty:
            continue

        # --- 切分趟次 ---
        seq_drop = ev["seq"].diff() < -TRIP_SEQ_DROP
        time_gap = ev["arrival_time"].diff().dt.total_seconds() > TRIP_GAP_MIN * 60
        ev["trip"] = (seq_drop | time_gap).cumsum()

        ev["plate_numb"] = plate
        ev["dwell_s"] = (ev["last_t"] - ev["arrival_time"]).dt.total_seconds()
        ev["station_id"] = stops["station_id"][ev["near_idx"].values]
        ev["stop_name"] = stops["name"][ev["near_idx"].values]
        ev["boarding"] = stops["boarding"][ev["near_idx"].values]
        out.append(ev.drop(columns=["near_idx", "last_t"]))

    return pd.concat(out, ignore_index=True) if out else None


# ------------------------------------------------------------
def extract_day(city, date, all_stops=None):
    t0 = time.time()
    all_stops = all_stops or load_all_stops()

    path = PARQUET_DIR / city / f"{date}.parquet"
    df = pd.read_parquet(path, columns=NEEDED_COLS)      # ← 只讀 8 欄
    log(f"{city} {date}: 讀入 {len(df):,} 筆 ({time.time()-t0:.1f}s)")

    # 僅營運中；去重；轉台北時間
    df = df[df["duty_status"] == 1]
    df = df.drop_duplicates(subset=["plate_numb", "sub_route_uid",
                                    "direction", "gps_time"])
    df["t"] = df["gps_time"].dt.tz_convert(TPE)
    log(f"  營運中且去重後 {len(df):,} 筆")

    results, missing = [], set()
    for (sru, direction), group in df.groupby(["sub_route_uid", "direction"],
                                              sort=False):
        stops = all_stops.get((sru, direction))
        if stops is None:
            missing.add((sru, direction))       # 站序資料缺漏，記下來
            continue
        ev = extract_route(group, stops)
        if ev is None:
            continue
        ev["sub_route_uid"] = sru
        ev["direction"] = direction
        ev["version_id"] = stops["version_id"]
        results.append(ev)

    if not results:
        log(f"  ⚠️ {city} {date} 沒有產生任何事件")
        return None

    out = pd.concat(results, ignore_index=True)
    out["city"] = city
    out["data_date"] = date
    out["trip_id"] = (out["plate_numb"] + "_" + out["sub_route_uid"]
                      + "_" + out["direction"].astype(str)
                      + "_" + date + "_" + out["trip"].astype(str))

    out = out[out["arrival_time"].dt.date.astype(str) == out["data_date"]]
    out_dir = OUTPUT_DIR / "events" / city
    out_dir.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_dir / f"{date}.parquet", compression="zstd", index=False)

    log(f"  事件 {len(out):,} 筆 / 趟次 {out['trip_id'].nunique():,} "
        f"/ 站位 {out['station_id'].nunique():,}")
    if missing:
        log(f"  ⚠️ {len(missing)} 條子路線在 MongoDB 找不到站序")
    log(f"  完成，耗時 {time.time()-t0:.1f}s")
    return out


# ------------------------------------------------------------
if __name__ == "__main__":
    from dotenv import load_dotenv
    from trafficproject.paths import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")

    cities = ["Taipei", "NewTaipei"]
    date = (datetime.now(ZoneInfo(TPE)) - timedelta(days=1)).strftime("%Y-%m-%d")
    if len(sys.argv) > 2:
        cities, date = [sys.argv[1]], sys.argv[2]
    elif len(sys.argv) > 1:
        cities = [sys.argv[1]]

    all_stops = load_all_stops()        # 只載一次，兩個城市共用
    for c in cities:
        extract_day(c, date, all_stops)