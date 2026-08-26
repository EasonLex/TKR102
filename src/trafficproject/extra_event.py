"""
到站事件抽取 —— 從 GPS 觀測推導出「某台車某時刻到達某站」

輸入：gs://{bucket}/silver/positions/city={city}/dt={date}/positions.parquet
輸出：gs://{bucket}/silver/events_v2/city={city}/dt={date}/events.parquet

判定規則 v2（2026-08-26 定案，取代 v1）
------------------------------------------------------------
  1. duty_status == 1                僅營運中的觀測
  2. 站牌指派加上「順序約束」        候選只在 [cur_idx-1, cur_idx+8] 這個窗內找
  3. 距最近站牌 <= 100m              到站門檻（在窗內判斷）
  4. 連續同站序合併為一事件；間隔 >120s 或 換趟 則切分
  5. 趟次由指派過程直接決定，不再事後用站序下降推斷

v1 → v2 改了什麼、為什麼
------------------------------------------------------------
  v1 對每筆觀測獨立取全線最近站（argmin over all stops）。
  在「同一方向內折返經過同一走廊」的路線上，這個指派是**結構性歧義**
  而非雜訊——例如 TPE11881 dir 0 的 seq 4/5 與 seq 28/29 是不同 station_id
  但相距 <100m，argmin 會在兩者之間跳動。

  後果被趟次切分放大：一次假倒退就把一整趟切成兩趟。
  實測 TPE11881 dir 0 產生 1,326 趟（67 站 / 32 車，應約 260 趟）。
  全域損害：台北 2.43% / 新北 4.00% 的「站間步」不合理
  （倒退、原地、或每站不到 10 秒）。

  v2 依時間順序處理每台車的觀測，維持「目前進度」cur_idx，
  候選站牌只在窗內找。剛過 seq 4 的車下一站只能是 5 附近，不可能是 28。

趟次結束的兩個條件（缺一不可）
------------------------------------------------------------
  主要：cur_idx 已在最後一站，且該筆觀測已離開它 → 該趟完成，立刻重新播種。
        只靠時間門檻會出錯：車停在終點時仍持續匹配，last_hit 一直更新，
        折返後要等滿門檻才重新播種，等於吃掉新趟的前 T 分鐘。
  備援：超過 ESCAPE_NO_MATCH_MIN 沒有任何匹配 → 結束並重新播種。
        給沒跑到終點就收班、或脫離路線的車用。有主要條件在，
        備援可以放寬而不傷害正常趟次。

  「重新播種」= 對全線做不受限 argmin。這是 v2 中唯一還可能指派錯的地方，
  因此播種次數被統計並寫進 log，作為錯誤率的上界指標
  （取代因結構性禁止而趨近恆真的「倒退步」）。

  播種依成因拆成三類，混在一起的話這個數字什麼都說明不了：
    初次  每個 (路線, 方向, 車輛) 的第一筆，不可避免
    終點  正常換趟，應該與真實趟次數同量級
    逾時  備援觸發，多半是停在調度場、duty_status=1 但遠離站牌的車
  另記「空段」= 開出來但一個事件都沒有的段（閒置車的必然產物，無害），
  用來解釋「播種數 ÷ 趟次數」為何恆大於 1。

已知限制
------------------------------------------------------------
  * 過站不停 + 10 秒取樣造成隨機漏抓（如 seq 11/13）—— v1 即有，未解
  * 每台車第一筆、以及每次備援播種，指派不受約束，可能落在錯誤分支
  * v1 的「站間距 <200m 重複計算」已由順序約束解除
  * v1 的「待改進：每趟每站取距離局部最小值」已可行（趟與站已可靠歸屬），
    但本版仍用門檻，未改；改了可再免除 DIST_THRESHOLD_M

效能
------------------------------------------------------------
  順序約束無法向量化，每台車是 Python 迴圈；但窗只有 10 站，每步很便宜。
  距離矩陣仍然是整個 (路線,方向) 一次算完的向量化運算。
"""

import os
from pathlib import Path
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pymongo import MongoClient

# PROJECT_ROOT 這個 import 有副作用：paths.py 會 load_dotenv()。
# 底下的 os.environ["GCS_BUCKET"] 依賴它，不要因為「看起來沒用到」就刪掉。
from trafficproject.paths import PROJECT_ROOT  # noqa: F401
from trafficproject.logging_util import make_logger

import gcsfs

log = make_logger("extract_events")
TPE = "Asia/Taipei"

# ---- 判定參數（集中在此，方便日後調整與記錄） ----
DIST_THRESHOLD_M = 100        # 到站距離門檻
EVENT_GAP_S = 120             # 同站序間隔超過此值視為不同事件
SEQ_WINDOW = 8                # 順序約束：候選在 [cur_idx-1, cur_idx+SEQ_WINDOW]
ESCAPE_NO_MATCH_MIN = 20      # 備援：連續無匹配超過此值 → 結束該趟並重新播種
RULE_VERSION = "v2"

# 只讀必要欄位 —— Parquet 欄式儲存的核心優勢
NEEDED_COLS = [
    "plate_numb", "sub_route_uid", "direction",
    "gps_time", "lat", "lon", "speed", "duty_status",
]

TMP_DIR = Path("/tmp/extract_events")
TMP_DIR.mkdir(parents=True, exist_ok=True)

GCS_BUCKET = os.environ["GCS_BUCKET"]

# 寫到帶版本的 prefix，不覆蓋 v1 的輸出——前後比對需要兩份並存
EVENTS_PREFIX = f"silver/events_{RULE_VERSION}"

fs = gcsfs.GCSFileSystem()


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
# 順序約束指派（單一車輛，已按時間排序）
# ------------------------------------------------------------
def assign_sequential(dv, elapsed_s):
    """
    dv        : (n_obs, n_stops) 這台車每筆觀測到各站的距離，列已按時間排序
    elapsed_s : (n_obs,) 相對秒數（只用差值，避免時區與 int64 轉換的坑）

    回傳 (matched_idx, trip_no, seed)
      matched_idx : 匹配到的站牌 index，未匹配為 -1
      trip_no     : 每筆觀測所屬的趟次序號（同一台車內遞增）
      seed        : {"init", "terminus", "timeout"} 三種播種成因的次數
                    三者之和 = 開出來的段數 = trip_no.max() + 1
    """
    n_obs, n_stops = dv.shape
    matched = np.full(n_obs, -1, dtype=np.int64)
    trips = np.zeros(n_obs, dtype=np.int64)
    seed = {"init": 0, "terminus": 0, "timeout": 0}

    escape_s = ESCAPE_NO_MATCH_MIN * 60
    last_idx = n_stops - 1

    cur = -1            # -1 = 尚未播種
    trip = -1
    last_hit = 0.0

    for i in range(n_obs):
        row = dv[i]

        # ---- 步驟 1：需要重新播種嗎（必須在窗內匹配之前判斷）----
        cause = None
        if cur < 0:
            cause = "init"
        else:
            lo = max(0, cur - 1)
            hi = min(n_stops, cur + SEQ_WINDOW + 1)
            j = lo + int(row[lo:hi].argmin())
            if (cur == last_idx) and (row[j] > DIST_THRESHOLD_M):
                cause = "terminus"          # 已在末站且離開 → 該趟完成
            elif (elapsed_s[i] - last_hit) > escape_s:
                cause = "timeout"           # 備援：長時間無匹配

        if cause is not None:
            trip += 1
            cur = int(row.argmin())
            seed[cause] += 1
            last_hit = elapsed_s[i]      # 播種即重設計時，否則備援永遠不會再觸發

        # ---- 步驟 2：窗內匹配 ----
        lo = max(0, cur - 1)
        hi = min(n_stops, cur + SEQ_WINDOW + 1)
        j = lo + int(row[lo:hi].argmin())

        trips[i] = trip
        if row[j] <= DIST_THRESHOLD_M:
            matched[i] = j
            cur = j
            last_hit = elapsed_s[i]
        # 未匹配：cur 與 last_hit 都不動（車在站間或偏離路線）

    return matched, trips, seed


# ------------------------------------------------------------
# 單一 (子路線, 方向) 的事件抽取
# ------------------------------------------------------------
def extract_route(group, stops):
    """
    group: 同一 (sub_route_uid, direction) 的所有觀測，已濾 duty_status
    stops: load_all_stops() 的其中一項

    回傳 (事件表 or None, 統計 dict)
    """
    stat = {"obs": len(group), "matched": 0,
            "seg": 0, "seg_empty": 0,
            "seed_init": 0, "seed_terminus": 0, "seed_timeout": 0}
    if group.empty:
        return None, stat

    # 每筆觀測 → 到各站的距離（向量化：n_obs × n_stops）
    # v1 在這裡直接 argmin；v2 保留整個矩陣，交給順序約束逐車判斷
    d = haversine_m(
        group["lat"].values[:, None], group["lon"].values[:, None],
        stops["lat"][None, :], stops["lon"][None, :],
    )

    # 位置索引：分車之後才切得到 d 的對應列
    group = group.assign(_row=np.arange(len(group)))

    out = []
    for plate, tr in group.groupby("plate_numb", sort=False):
        tr = tr.sort_values("t")
        rows = tr["_row"].to_numpy()

        # 相對秒數即可，備援門檻只用差值
        elapsed = (tr["t"] - tr["t"].iloc[0]).dt.total_seconds().to_numpy()

        matched, trip_no, seed = assign_sequential(d[rows], elapsed)
        for cause, n in seed.items():
            stat[f"seed_{cause}"] += n

        # 空段：開出來但一個事件都沒有的段。必須在 continue 之前算，
        # 否則「整台車都沒匹配」這種最典型的空段會漏統計。
        n_seg = int(trip_no[-1]) + 1 if len(trip_no) else 0
        n_kept = len(np.unique(trip_no[matched >= 0]))
        stat["seg"] += n_seg
        stat["seg_empty"] += n_seg - n_kept

        tr = tr.assign(near_idx=matched, trip=trip_no)
        tr = tr[tr["near_idx"] >= 0]
        if tr.empty:
            continue
        stat["matched"] += len(tr)

        near_idx = tr["near_idx"].to_numpy()
        tr = tr.assign(
            near_seq=stops["seq"][near_idx],
            near_dist=d[tr["_row"].to_numpy(), near_idx],
        )

        # --- 合併連續觀測為事件 ---
        # 「換趟」必須是切分條件之一：重新播種後的第一站可能剛好等於
        # 上一趟的最後一站（同一總站），只看站序與時間會把兩趟併成一個事件。
        new_event = (
            (tr["near_seq"] != tr["near_seq"].shift())
            | (tr["trip"] != tr["trip"].shift())
            | (tr["t"].diff().dt.total_seconds() > EVENT_GAP_S)
        )
        tr = tr.assign(event=new_event.cumsum())

        ev = tr.groupby("event").agg(
            near_idx=("near_idx", "first"),
            seq=("near_seq", "first"),
            trip=("trip", "first"),
            arrival_time=("t", "min"),
            last_t=("t", "max"),
            n_obs=("t", "size"),
            min_dist=("near_dist", "min"),
            min_speed=("speed", "min"),
        ).reset_index(drop=True)

        if ev.empty:
            continue

        # v1 的「站序下降 >5 或 間隔 >30min → 新趟」已移除：
        # 趟次在 assign_sequential 裡就決定了，事後推斷會與它衝突，
        # 而且順序約束之下站序下降永遠不會發生，留著只會誤導讀者。

        ev["plate_numb"] = plate
        ev["dwell_s"] = (ev["last_t"] - ev["arrival_time"]).dt.total_seconds()
        ev["station_id"] = stops["station_id"][ev["near_idx"].values]
        ev["stop_name"] = stops["name"][ev["near_idx"].values]
        ev["boarding"] = stops["boarding"][ev["near_idx"].values]
        out.append(ev.drop(columns=["near_idx", "last_t"]))

    return (pd.concat(out, ignore_index=True) if out else None), stat


# ------------------------------------------------------------
def send_to_gcs(df, city, date):
    local_tmp = TMP_DIR / f"events_{city}_{date}.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, local_tmp, compression="zstd")
    dst = f"{GCS_BUCKET}/{EVENTS_PREFIX}/city={city}/dt={date}/events.parquet"
    fs.put(str(local_tmp), dst)
    local_tmp.unlink()
    return dst


# ------------------------------------------------------------
def extract_day(city, date, all_stops=None):
    t0 = time.time()
    all_stops = all_stops or load_all_stops()

    src = (f"gs://{GCS_BUCKET}/silver/positions/"
           f"city={city}/dt={date}/positions.parquet")
    df = pd.read_parquet(src, columns=NEEDED_COLS)       # ← 只讀 8 欄
    n_read = len(df)
    log(f"{city} {date}: 讀入 {n_read:,} 筆 ({time.time()-t0:.1f}s)")

    # 僅營運中；去重；轉台北時間
    df = df[df["duty_status"] == 1]

    # 座標缺值必須先剔除。v1 只是讓那筆過不了門檻；v2 有狀態，
    # NaN 會讓 argmin 落在 NaN 位置並把錯誤的 cur_idx 帶給後續觀測。
    n_before = len(df)
    df = df.dropna(subset=["lat", "lon"])
    n_nullpos = n_before - len(df)

    df = df.drop_duplicates(subset=["plate_numb", "sub_route_uid",
                                    "direction", "gps_time"])
    df["t"] = df["gps_time"].dt.tz_convert(TPE)
    log(f"  營運中且去重後 {len(df):,} 筆"
        + (f"（座標缺值剔除 {n_nullpos:,}）" if n_nullpos else ""))

    results, missing = [], set()
    tot = {"obs": 0, "matched": 0, "seg": 0, "seg_empty": 0,
           "seed_init": 0, "seed_terminus": 0, "seed_timeout": 0}
    for (sru, direction), group in df.groupby(["sub_route_uid", "direction"],
                                              sort=False):
        stops = all_stops.get((sru, direction))
        if stops is None:
            missing.add((sru, direction))       # 站序資料缺漏，記下來
            continue
        ev, stat = extract_route(group, stops)
        for k in tot:
            tot[k] += stat[k]
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
    out["rule_version"] = RULE_VERSION
    out["trip_id"] = (out["plate_numb"] + "_" + out["sub_route_uid"]
                      + "_" + out["direction"].astype(str)
                      + "_" + date + "_" + out["trip"].astype(str))

    out = out[out["arrival_time"].dt.date.astype(str) == out["data_date"]]
    dst = send_to_gcs(out, city, date)

    # 層間漏斗：讀入 → 可指派觀測 → 匹配 → 事件 → 趟次
    match_pct = 100 * tot["matched"] / tot["obs"] if tot["obs"] else 0
    log(f"  漏斗 讀入 {n_read:,} → 可指派 {tot['obs']:,} "
        f"→ 匹配 {tot['matched']:,} ({match_pct:.1f}%)")
    n_trip = out["trip_id"].nunique()
    log(f"  事件 {len(out):,} 筆 / 趟次 {n_trip:,} "
        f"/ 站位 {out['station_id'].nunique():,}")

    # 播種 = 不受順序約束的指派，是本版錯誤率的上界指標。
    # 依成因拆開才有診斷力：終點應與真實趟次同量級，逾時多為閒置車。
    empty_pct = 100 * tot["seg_empty"] / tot["seg"] if tot["seg"] else 0
    log(f"  分段 {tot['seg']:,}（空段 {tot['seg_empty']:,} = {empty_pct:.1f}%，"
        f"有事件 {n_trip:,}）")
    log(f"  播種 初次 {tot['seed_init']:,} / 終點 {tot['seed_terminus']:,} "
        f"/ 逾時 {tot['seed_timeout']:,}")
    if missing:
        log(f"  ⚠️ {len(missing)} 條子路線在 MongoDB 找不到站序")
    log(f"  寫出 {dst}")
    log(f"  完成，耗時 {time.time()-t0:.1f}s")
    return out


# ------------------------------------------------------------
if __name__ == "__main__":
    from dotenv import load_dotenv
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