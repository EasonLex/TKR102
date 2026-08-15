"""
單車軌跡檢視 —— 觀察「到站」在真實資料裡長什麼樣

目的不是寫演算法，是先看清楚形狀再決定規則。
用法（notebook）：
    stops = get_stops("TPE101320", 0)
    df    = load_vehicle("Taipei", "2026-08-05", "TPE101320", 0)
    trips = list_vehicles(df)          # 看哪台車跑得完整
    plot_trajectory(df, stops, plate="XXX-1234")
"""

import os
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

from trafficproject.paths import PARQUET_DIR

TPE = "Asia/Taipei"


# ------------------------------------------------------------
def get_stops(sub_route_uid, direction):
    """從 MongoDB 取出這條子路線的站序。"""
    db = MongoClient(os.environ["MONGO_URI"]).tdx
    doc = db.route_stops.find_one({
        "sub_route_uid": sub_route_uid,
        "direction": direction,
        "valid_to": None,
    })
    if doc is None:
        raise ValueError(f"找不到 {sub_route_uid} dir={direction}")
    return pd.DataFrame(doc["stops"]).sort_values("seq").reset_index(drop=True)


def load_vehicle(city, date, sub_route_uid, direction):
    """讀一天的 Parquet，篩出這條子路線的觀測。"""
    df = pd.read_parquet(PARQUET_DIR / city / f"{date}.parquet")
    df = df[(df["sub_route_uid"] == sub_route_uid)
            & (df["direction"] == direction)].copy()
    # 去重：同一台車同一個 GPSTime 只留一筆
    df = df.drop_duplicates(subset=["plate_numb", "gps_time"])
    df["t"] = df["gps_time"].dt.tz_convert(TPE)
    return df.sort_values(["plate_numb", "t"]).reset_index(drop=True)


def list_vehicles(df, top=15):
    """哪些車觀測筆數多、時間跨度大 —— 適合當樣本。"""
    g = df.groupby("plate_numb").agg(
        n=("t", "size"),
        start=("t", "min"),
        end=("t", "max"),
    )
    g["span_min"] = (g["end"] - g["start"]).dt.total_seconds() / 60
    return g.sort_values("n", ascending=False).head(top)


# ------------------------------------------------------------
def haversine_m(lat1, lon1, lat2, lon2):
    """向量化的球面距離（公尺）。"""
    R = 6371000.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


def nearest_stop(track, stops):
    """
    每一筆觀測 → 最近的站牌是哪一站、距離多遠。

    刻意用暴力法：一台車一天約 5000 筆 × 40 站 = 20 萬次計算，
    numpy 向量化後不到一秒。先看清楚形狀，效能之後再說。
    """
    d = haversine_m(
        track["lat"].values[:, None], track["lon"].values[:, None],
        stops["lat"].values[None, :], stops["lon"].values[None, :],
    )                                          # shape: (n_obs, n_stops)
    idx = d.argmin(axis=1)
    out = track.copy()
    out["near_seq"] = stops["seq"].values[idx]
    out["near_name"] = stops["name"].values[idx]
    out["near_dist"] = d[np.arange(len(track)), idx]
    return out


# ------------------------------------------------------------
def plot_trajectory(df, stops, plate, threshold=30):
    """
    三張圖，一起看才有意義：
      上：到最近站牌的距離   —— 「到站」應該長成谷底
      中：站序隨時間推進     —— 正常應該是階梯狀單調上升
      下：速度               —— 停靠時應該掉到 0
    """
    track = df[df["plate_numb"] == plate].sort_values("t")
    if track.empty:
        raise ValueError(f"{plate} 沒有資料")
    track = nearest_stop(track, stops)

    fig, axes = plt.subplots(3, 1, figsize=(15, 10), sharex=True)

    axes[0].plot(track["t"], track["near_dist"], linewidth=0.8)
    axes[0].axhline(threshold, color="red", ls="--", lw=1,
                    label=f"{threshold}m")
    axes[0].set_ylabel("dist to nearest stop (m)")
    axes[0].set_ylim(0, 500)
    axes[0].legend(loc="upper right")
    axes[0].grid(alpha=0.3)
    axes[0].set_title(f"{plate}  ({len(track)} obs)")

    axes[1].plot(track["t"], track["near_seq"], linewidth=0.8, marker=".",
                 markersize=2)
    axes[1].set_ylabel("nearest stop seq")
    axes[1].grid(alpha=0.3)

    axes[2].plot(track["t"], track["speed"], linewidth=0.8)
    axes[2].set_ylabel("speed (kph)")
    axes[2].set_xlabel("time (Asia/Taipei)")
    axes[2].grid(alpha=0.3)

    plt.tight_layout()
    plt.show()
    return track


def show_candidates(track, threshold=30):
    """把 threshold 以內的連續片段列出來 —— 這是「到站事件」的候選。"""
    near = track[track["near_dist"] <= threshold].copy()
    if near.empty:
        print(f"{threshold}m 內沒有任何觀測")
        return None

    # 相鄰筆若換了站或間隔過久，視為不同事件
    new_grp = (near["near_seq"] != near["near_seq"].shift()) | \
              (near["t"].diff().dt.total_seconds() > 120)
    near["event"] = new_grp.cumsum()

    ev = near.groupby("event").agg(
        seq=("near_seq", "first"),
        name=("near_name", "first"),
        first_t=("t", "min"),
        last_t=("t", "max"),
        n=("t", "size"),
        min_dist=("near_dist", "min"),
        min_speed=("speed", "min"),
    )
    ev["dwell_s"] = (ev["last_t"] - ev["first_t"]).dt.total_seconds()
    return ev