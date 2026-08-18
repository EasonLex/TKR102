import os
import sys
import time
import glob
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from trafficproject.paths import TRANSFER_DIR, MART_DIR
from trafficproject.logging_util import make_logger

log = make_logger("transfer_stats")
TPE = "Asia/Taipei"

def estimate_percentile(hist, q, bin_width=30):
    hit = hist[hist["cum"] >= hist["tot"] * q].groupby(["station_id", "route_a", "route_b", "bucket"]).first()
    hit_sec = hit["wait_bin"] * bin_width + bin_width/2
    return hit_sec.reset_index().rename(columns={"wait_bin": f"wait_p{int(q*100)}"})

def build_marts(city, days=None, min_samples=20):
    """
    讀多天的 transfers，跨天累加直方圖，算出 p50/p90。
    
    輸出：output/marts/{city}/transfer_stats.parquet
    欄位：station_id, route_a, route_b, bucket,
          n_total, n_success, success_rate,
          wait_p50, wait_p90, n_days
    """
    t0 = time.time()
    path = TRANSFER_DIR / city

    # 一、讀多天的檔案並合併
    file_path_list = glob.glob(f"{path}/*.parquet")
    df = pd.concat((pd.read_parquet(file_path) for file_path in file_path_list), ignore_index=True)

    # 二、跨天累加
    # 按 (station_id, route_a, route_b, bucket, wait_bin) 加總 count
    hist = df.groupby(
        ["station_id", "route_a", "route_b", "bucket", "wait_bin"]
    )["count"].sum().reset_index().sort_values(by=["station_id", "route_a", "route_b", "bucket", "wait_bin"])
    
    tot = df.drop_duplicates(
        subset=["station_id", "route_a", "route_b", "bucket", "data_date"]
    ).groupby(
        ["station_id", "route_a", "route_b", "bucket"]
    ).agg(
        n_total=("n_total", "sum"),
        n_over=("n_over", "sum"),
        n_days=("data_date", "nunique")
    ).reset_index()

    hist["cum"] = hist.groupby(["station_id", "route_a", "route_b", "bucket"])["count"].cumsum()
    hist["tot"] = hist.groupby(["station_id", "route_a", "route_b", "bucket"])["count"].transform("sum")
    p50 = estimate_percentile(hist, 0.5)
    p90 = estimate_percentile(hist, 0.9)

    result = tot.merge(
        p50, on=["station_id", "route_a", "route_b", "bucket"]
    ).merge(
        p90, on=["station_id", "route_a", "route_b", "bucket"]
    )

    result["n_success"] = result["n_total"] - result["n_over"]
    result["success_rate"] = result["n_success"] / result["n_total"]
    result = result[result["n_success"] >= min_samples]

    out_dir = MART_DIR / city
    out_dir.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out_dir / f"marts.parquet", compression="zstd", index=False)

    log(f"  完成，耗時 {time.time()-t0:.1f}s")

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
        build_marts(city)