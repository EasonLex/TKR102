import os
import sys
import time
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from trafficproject.paths import EVENT_DIR, TRANSFER_DIR
from trafficproject.logging_util import make_logger

log = make_logger("transfer_stats")
TPE = "Asia/Taipei"

def find_next_departure(a_time, b_times):
    idx = np.searchsorted(b_times, a_time, side = 'right')
    if idx == len(b_times):          # 判斷有沒有超出範圍
        return None
    return b_times[idx]

def pair_transfers(events):
    """
    輸入：某站位一天的所有到站事件
    輸出：每一次轉乘機會（明細）
          route_a, route_b, a_time, b_time, wait_sec
    """
    # 1. 依路線分組，取出到站時刻（記得排序）
    times = {
        route: g["arrival_time"].dt.tz_localize(None).values
        for route, g in events.sort_values("arrival_time").groupby("sub_route_uid")
    }
    # print(times)

    # 2. 算出可當 A、可當 B 的路線清單
    a_route = events.loc[events["boarding"] <= 0, "sub_route_uid"].unique()
    b_route = events.loc[events["boarding"] >= 0, "sub_route_uid"].unique()

    # 3. 雙層迴圈配對
    rows = []
    for a in a_route:
        for b in b_route:
            if a == b:
                continue
            for t in times[a]:
                nxt = find_next_departure(t, times[b])
                if nxt is None: continue
                wait_sec = (nxt - t) / np.timedelta64(1, 's')
                rows.append({   "route_a": a,
                                "route_b": b,
                                "a_time": t,
                                "b_time": nxt,
                                "wait_sec": wait_sec})

    # 4. 組成 DataFrame 回傳
    return pd.DataFrame(rows)


def aggregate_transfers(pairs, station_id, max_wait_sec=1200, bin_width_sec=30):
    """
    把轉乘明細聚合成統計表。

    輸入
    ----
    pairs : DataFrame   pair_transfers 的輸出
                        欄位 route_a, route_b, a_time, b_time, wait_sec
    
    輸出
    ----
    DataFrame，每一列是一個 (station_id, route_a, route_b, bucket, wait_bin, count) 的統計
    欄位：
        station_id
        route_a, route_b
        bucket              時段分類
        n_total             總機會數
        n_over              超過1200秒的次數
    """
    pairs = pairs.copy()
    pairs["bucket"] = pairs["a_time"].apply(time_bucket)
    pairs["ok"] = pairs["wait_sec"] <= max_wait_sec
    pairs["wait_bin"] = pairs["wait_sec"].astype(int) // bin_width_sec

    g_hist = pairs[pairs["ok"]].groupby(["route_a", "route_b", "bucket", "wait_bin"])
    hist = g_hist.size().reset_index(name="count")

    g_tot = pairs.groupby(["route_a", "route_b", "bucket"])
    total = g_tot.agg(
        n_total = ('ok', 'count'),
        n_over = ('ok', lambda x: (~x).sum()),
    ).reset_index()

    result = hist.merge(
        total,
        on=["route_a", "route_b", "bucket"]
    )
    result['station_id'] = station_id
    result["city"] = city
    result["data_date"] = date
    
    return result

def time_bucket(ts):
    h, wd = ts.hour, ts.weekday()
    if h < 6 or h >= 23:
        return "night"
    if wd >= 5:
        return "weekend"
    if 7 <= h < 9 or 17 <= h < 19:
        return "weekday_peak"
    return "weekday_offpeak"

def build_transfers(city, date):
    # 1. 讀事件表
    t0 = time.time()
    path = EVENT_DIR / city / f"{date}.parquet"
    df = pd.read_parquet(path)
    log(f"{city} {date}: 讀入 {len(df):,} 筆 ({time.time()-t0:.1f}s)")

    # 2. 依 station_id 分組
    results = []
    n_stations = df["station_id"].nunique()
    route_per_station = df.groupby("station_id")["sub_route_uid"].nunique()
    valid = route_per_station[route_per_station >= 2].index
    print(valid)
    for i, (station_id, group) in enumerate(df.groupby("station_id")):
        # 3. 每組跑 pair_transfers → aggregate_transfers
        pair_result = pair_transfers(group)
        if station_id not in valid or pair_result.empty:
            continue
        agg = aggregate_transfers(pair_result, station_id)
        results.append(agg)
        if i % 500 == 0:
            log(f"  進度 {i}/{n_stations} ({time.time()-t0:.0f}s)")
    # 4. 全部合併，寫檔
    out_dir = TRANSFER_DIR / city
    out_dir.mkdir(parents=True, exist_ok=True)
    out = pd.concat(results, ignore_index=True)
    out.to_parquet(out_dir / f"{date}.parquet", compression="zstd", index=False)

    log(f"  完成，耗時 {time.time()-t0:.1f}s")
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

    for city in cities:
        build_transfers(city, date)