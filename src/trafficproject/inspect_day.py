"""
資料體檢腳本 - 讀一天的原始 gz 檔，找出資料問題

用法：
    uv add pandas matplotlib
    uv run python inspect_day.py NewTaipei 2026-08-03

目的不是做分析，是找出資料哪裡有問題。
"""

import sys
import gzip
import json
import glob
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import numpy as np

from trafficproject.paths import RAW_DIR, PARQUET_DIR, REPORT_DIR

KEY = ["plate_numb", "route_uid", "direction", "gps_time"]

def load_day(city, date):
    path = PARQUET_DIR / city / f"{date}.parquet"
    df = pd.read_parquet(path)
    print(f"讀取 {len(df):,} 筆 <- {path.name}")
    return df

def health_check_log(city, date, warningList = []):
    warningString = f" {len(warningList)}項異常" if len(warningList) > 0 else ""
    line = f"[{date} {city}{warningString}]"
    # 
    # TBD: 異常項目列表
    # 
    print(line, flush=True)
    with open(REPORT_DIR / "daily_report.txt", "a", encoding="utf-8") as f:
        f.write(line + "\n")

def metric_log(msg):
    line = msg
    print(line, flush=True)
    with open(REPORT_DIR / "metrics.jsonl", "a", encoding="utf-8") as f:
        f.write(line + "\n")

def health_check(df, city, date):
    metrics = {}
    metrics["schema_version"] = 1
    metrics["date"] = date
    metrics["city"] = city
    
    print("\n" + "=" * 50)
    print("【基本數量】")
    

    metrics["raw_records"] = len(df)
    print(f"原始筆數        : {len(df):,}")

    dedup = df.drop_duplicates(subset=KEY)
    metrics["dedup_records"] = len(dedup)
    print(f"去重後筆數      : {len(dedup):,}")

    metrics["deduplication_ratio"] = len(dedup) / len(df)
    print(f"去重比例        : {len(dedup) / len(df):.3f}  (預期接近 0.5)")

    metrics["distinct_vehicles"] = df['plate_numb'].nunique()
    print(f"不重複車輛數    : {df['plate_numb'].nunique():,}")

    metrics["route_uid"] = df['route_uid'].nunique()
    print(f"不重複路線數    : {df['route_uid'].nunique():,}")
    
    print("\n【空值檢查】")
    nulls = df.isna().sum()
    nulls = nulls[nulls > 0]
    metrics["null_counts"] = nulls.to_dict()
    metrics["null_total"] = int(df.isna().sum().sum())   # 兩個 sum
    print(nulls if len(nulls) else "  沒有空值")

    print("\n【狀態碼分布】")
    for col in ["duty_status", "bus_status", "direction"]:
        if col in df.columns:
            print(f"  {col}: {df[col].value_counts().to_dict()}")
    metrics["duty_status_2_count"] = int((df["duty_status"] == 2).sum())
    metrics["bus_status_99_count"] = int((df["bus_status"] == 99).sum())
    metrics["bus_status_other_count"] = int((~df["bus_status"].isin([0, 99])).sum())
    metrics["bus_status_abnormal"] = int(df["bus_status"].isin([1,2,4,98,101]).sum())   # 要排除的
    metrics["bus_status_traffic"] = int(df["bus_status"].isin([3,100]).sum())           # 有價值的
    metrics["bus_status_2_vehicles"] = int(df[df["bus_status"]==2]["plate_numb"].nunique())

    print("\n【時間欄位】")
    gps = pd.to_datetime(df["gps_time"])
    upd = pd.to_datetime(df["update_time"])
    metrics["gps_min"] = str(gps.min().tz_convert("Asia/Taipei"))
    metrics["gps_max"] = str(gps.max().tz_convert("Asia/Taipei"))
    print(f"  GPSTime 範圍  : {gps.min().tz_convert("Asia/Taipei")}  ~  {gps.max().tz_convert("Asia/Taipei")}")
    
    delay = (upd - gps).dt.total_seconds()
    metrics["delay_median_sec"] = float(delay.median())
    metrics["delay_p95_sec"] = float(delay.quantile(0.95))
    metrics["delay_max_sec"] = float(delay.max())
    print(f"  端到端延遲(秒): 中位數 {delay.median():.1f} / P95 {delay.quantile(0.95):.1f} / 最大 {delay.max():.1f}")

    print("\n【快照與抓取間隔】")
    snap = df.groupby("snapshot_time").size()      # 取代原本傳進來的 snap
    metrics["snapshot_count"] = int(len(snap))
    metrics["snapshot_records_min"] = int(snap.min())
    metrics["snapshot_records_median"] = float(snap.median())
    metrics["snapshot_records_max"] = int(snap.max())
    print(f"  快照數        : {len(snap):,}")
    print(f"  每快照筆數    : 最少 {snap.min()} / 中位數 {snap.median():.0f} / 最多 {snap.max()}")

    gaps = pd.Series(snap.index).diff().dt.total_seconds()
    metrics["interval_median_sec"] = float(gaps.median())
    metrics["max_gap_sec"] = float(gaps.max())
    metrics["gap_over_60s_count"] = int((gaps > 60).sum())
    print(f"  抓取間隔      : 中位數 {gaps.median():.1f} 秒 / 最大 {gaps.max():.0f} 秒")
    print(f"  間隔>60秒     : {metrics['gap_over_60s_count']} 次")

    print("\n【座標範圍】檢查有沒有離群點")
    lat = df["lat"]
    lon = df["lon"]
    print(f"  Lat: {lat.min():.4f} ~ {lat.max():.4f}")
    print(f"  Lon: {lon.min():.4f} ~ {lon.max():.4f}")
    bad = df[(lat < 24) | (lat > 26) | (lon < 120) | (lon > 123)]
    print(f"  離群座標筆數  : {len(bad):,}  ({len(bad)/len(df)*100:.3f}%)")
    print(f"  涉及車輛數    : {bad['plate_numb'].nunique()}")
    metrics["bad_coordinates"] = {str(k): int(v) for k, v in bad['plate_numb'].value_counts().head(5).items()}
    if len(bad):
        metrics["bad_count"] = len(bad)
        print(f"  最多的幾台    : {bad['plate_numb'].value_counts().head(5).to_dict()}")

    bad_speed = df[df["speed"] > 100]
    metrics["bad_speed_count"] = int(len(bad_speed))
    metrics["bad_speed_vehicles"] = int(bad_speed["plate_numb"].nunique())
    print(bad_speed["plate_numb"].value_counts().head())

    print("=" * 50 + "\n")

    health_check_log(city, date)
    metric_log(json.dumps(metrics))

    return dedup, snap

def _break_gaps(s, max_gap_sec=60):
    """在中斷處插入 NaN，讓折線斷開而不是連成一條假的斜線。"""
    idx, vals = list(s.index), list(s.values)
    out_i, out_v = [], []
    for i, (t, v) in enumerate(zip(idx, vals)):
        if i > 0 and (t - idx[i - 1]).total_seconds() > max_gap_sec:
            out_i.append(idx[i - 1] + pd.Timedelta(seconds=1))
            out_v.append(np.nan)
        out_i.append(t)
        out_v.append(v)
    return pd.Series(out_v, index=out_i)

def plot(dedup, snap, city, date, out_dir="."):
    # snap 是 Series，index 為 snapshot_time(UTC)
    s = snap.copy()
    s.index = s.index.tz_convert("Asia/Taipei")
    s = _break_gaps(s)

    fig, axes = plt.subplots(2, 1, figsize=(13, 8))

    # 上圖：每個快照回傳幾筆 —— 基準線
    axes[0].plot(s.index, s.values, linewidth=0.8)
    axes[0].set_title(f"{city} {date} - records per snapshot (baseline)")
    axes[0].set_ylabel("records")
    axes[0].grid(alpha=0.3)

    # 下圖：每小時不重複車輛數
    gps_tpe = dedup["gps_time"].dt.tz_convert("Asia/Taipei")
    hourly = dedup.assign(hour=gps_tpe.dt.hour).groupby("hour")["plate_numb"].nunique()

    # 標記不完整的小時（快照數明顯偏少）—— 避免半小時的長條看起來跟整小時一樣高
    snap_per_hour = snap.groupby(snap.index.tz_convert("Asia/Taipei").hour).size()
    expected = snap_per_hour.median()
    incomplete = snap_per_hour[snap_per_hour < expected * 0.9].index

    colors = ["lightgray" if h in incomplete else "tab:blue" for h in hourly.index]
    axes[1].bar(hourly.index, hourly.values, color=colors)
    axes[1].set_title("distinct active buses by hour (gray = incomplete hour)")
    axes[1].set_xlabel("hour")
    axes[1].set_ylabel("buses")
    axes[1].set_xticks(range(24))
    axes[1].grid(alpha=0.3, axis="y")

    plt.tight_layout()
    out = REPORT_DIR / f"inspect_{city}_{date}.png"
    plt.savefig(out, dpi=120)
    plt.close(fig)
    print(f"圖已存成 {out}")


# if __name__ == "__main__":
#     today = datetime.now(ZoneInfo("Asia/Taipei"))
#     # 
#     # Default city: Taipei
#     # Default date: yesterday
#     # 
#     cities = ["Taipei", "NewTaipei"]
#     date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

#     if len(sys.argv) > 2:
#         cities = [sys.argv[1]]
#         date = sys.argv[2]
#     elif len(sys.argv) > 1:
#         cities = [sys.argv[1]]

#     for i in range(len(cities)):
#         df = load_day(cities[i], date)
#         dedup, snap = health_check(df, cities[i], date)
#         plot(dedup, snap, cities[i], date)
