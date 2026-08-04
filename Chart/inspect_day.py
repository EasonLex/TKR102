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

KEY = ["PlateNumb", "RouteUID", "Direction", "GPSTime"]


def load_day(city, date):
    """把一天的所有 gz 檔讀成一個 DataFrame，並記錄每個快照的筆數。"""
    files = sorted(glob.glob(f"../raw/{city}/{date}/*.json.gz"))
    print(f"找到 {len(files)} 個檔案")

    rows, snapshots = [], []
    for path in files:
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  讀取失敗 {path}: {e}")
            continue

        snapshots.append({
            "file_time": path.split("/")[-1].replace(".json.gz", ""),
            "count": len(data),
        })
        rows.extend(data)

    df = pd.json_normalize(rows)
    snap = pd.DataFrame(snapshots)
    return df, snap

def health_check_log(city, date, warningList = []):
    warningString = f" {len(warningList)}項異常" if len(warningList) > 0 else ""
    line = f"[{date} {city}{warningString}]"
    # 
    # TBD: 異常項目列表
    # 
    print(line, flush=True)
    with open("daily_report.txt", "a", encoding="utf-8") as f:
        f.write(line + "\n")

def metric_log(msg):
    line = msg
    print(line, flush=True)
    with open("metrics.jsonl", "a", encoding="utf-8") as f:
        f.write(line + "\n")

def health_check(df, snap, city, date):
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

    metrics["distinct_vehicles"] = df['PlateNumb'].nunique()
    print(f"不重複車輛數    : {df['PlateNumb'].nunique():,}")

    metrics["route_uid"] = df['RouteUID'].nunique()
    print(f"不重複路線數    : {df['RouteUID'].nunique():,}")
    
    print("\n【空值檢查】")
    nulls = df.isna().sum()
    nulls = nulls[nulls > 0]
    metrics["null_counts"] = nulls.to_dict()
    metrics["null_total"] = int(df.isna().sum().sum())   # 兩個 sum
    print(nulls if len(nulls) else "  沒有空值")

    print("\n【狀態碼分布】")
    for col in ["DutyStatus", "BusStatus", "Direction"]:
        if col in df.columns:
            print(f"  {col}: {df[col].value_counts().to_dict()}")
    metrics["duty_status_2_count"] = int((df["DutyStatus"] == 2).sum())
    metrics["bus_status_99_count"] = int((df["BusStatus"] == 99).sum())
    metrics["bus_status_other_count"] = int((~df["BusStatus"].isin([0, 99])).sum())
    metrics["bus_status_abnormal"] = int(df["BusStatus"].isin([1,2,4,98,101]).sum())   # 要排除的
    metrics["bus_status_traffic"] = int(df["BusStatus"].isin([3,100]).sum())           # 有價值的
    metrics["bus_status_2_vehicles"] = int(df[df["BusStatus"]==2]["PlateNumb"].nunique())

    # print(f"value counts: {df[~df["BusStatus"].isin([0, 99])]["BusStatus"].value_counts()}")
    print("\n【時間欄位】")
    gps = pd.to_datetime(df["GPSTime"])
    upd = pd.to_datetime(df["UpdateTime"])
    metrics["gps_min"] = str(gps.min())
    metrics["gps_max"] = str(gps.max())
    print(f"  GPSTime 範圍  : {gps.min()}  ~  {gps.max()}")
    
    delay = (upd - gps).dt.total_seconds()
    metrics["delay_median_sec"] = float(delay.median())
    metrics["delay_p95_sec"] = float(delay.quantile(0.95))
    metrics["delay_max_sec"] = float(delay.max())
    print(f"  端到端延遲(秒): 中位數 {delay.median():.1f} / P95 {delay.quantile(0.95):.1f} / 最大 {delay.max():.1f}")

    metrics["snapshot_count"] = len(snap)
    print("\n【快照筆數】")
    print(f"  最少 {snap['count'].min()} / 中位數 {snap['count'].median():.0f} / 最多 {snap['count'].max()}")
    empty = (snap["count"] == 0).sum()
    print(f"  空快照數      : {empty}   (不是 0 就要查)")

    print("\n【座標範圍】檢查有沒有離群點")
    lat = df["BusPosition.PositionLat"]
    lon = df["BusPosition.PositionLon"]
    print(f"  Lat: {lat.min():.4f} ~ {lat.max():.4f}")
    print(f"  Lon: {lon.min():.4f} ~ {lon.max():.4f}")
    bad = df[(lat < 24) | (lat > 26) | (lon < 120) | (lon > 123)]
    print(f"  離群座標筆數  : {len(bad):,}  ({len(bad)/len(df)*100:.3f}%)")
    print(f"  涉及車輛數    : {bad['PlateNumb'].nunique()}")
    metrics["bad_coordinates"] = {str(k): int(v) for k, v in bad['PlateNumb'].value_counts().head(5).items()}
    if len(bad):
        metrics["bad_count"] = len(bad)
        print(f"  最多的幾台    : {bad['PlateNumb'].value_counts().head(5).to_dict()}")

    print("=" * 50 + "\n")

    health_check_log(city, date)
    metric_log(json.dumps(metrics))
    return dedup


def plot(dedup, snap, city, date):
    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=False)

    # 上圖：每個快照回傳幾筆 —— 這是你的基準線
    t = pd.to_datetime(date + " " + snap["file_time"], format="%Y-%m-%d %H%M%S")
    axes[0].plot(t, snap["count"], linewidth=0.8)
    axes[0].set_title(f"{city} {date} - records per snapshot (baseline)")
    axes[0].set_ylabel("records")
    axes[0].grid(alpha=0.3)

    # 下圖：每小時有多少不重複車輛在跑
    gps = pd.to_datetime(dedup["GPSTime"])
    hourly = dedup.assign(hour=gps.dt.hour).groupby("hour")["PlateNumb"].nunique()
    axes[1].bar(hourly.index, hourly.values)
    axes[1].set_title("distinct active buses by hour")
    axes[1].set_xlabel("hour")
    axes[1].set_ylabel("buses")
    axes[1].set_xticks(range(24))
    axes[1].grid(alpha=0.3, axis="y")

    plt.tight_layout()
    out = f"inspect_{city}_{date}.png"
    plt.savefig(out, dpi=120)
    print(f"圖已存成 {out}")


if __name__ == "__main__":
    today = datetime.now(ZoneInfo("Asia/Taipei"))
    # 
    # Default city: Taipei
    # Default date: yesterday
    # 
    cities = ["Taipei", "NewTaipei"]
    date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    if len(sys.argv) > 2:
        cities = [sys.argv[1]]
        dates = [sys.argv[2]]
    elif len(sys.argv) > 1:
        cities = [sys.argv[1]]

    for i in range(len(cities)):
        df, snap = load_day(cities[i], date)
        dedup = health_check(df, snap, cities[i], date)
        plot(dedup, snap, cities[i], date)
