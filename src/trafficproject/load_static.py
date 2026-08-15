"""
靜態資料寫入 MongoDB

兩個 collection：
  route_stops — 路線站序，判斷「車輛到站」用
  stations    — 實體站位，轉乘分析的錨點（合併兩市）

設計原則：
  1. 版本化：來源改版時保留舊版，不覆蓋
     （8 月的觀測資料必須用 8 月的站序比對）
  2. 冪等：同一份資料重跑多次，結果相同
  3. 合併而非覆蓋：交集站位的 stops 來自兩市，互斥需聯集
"""

import os
import gzip
import json
import hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys
from trafficproject.paths import PROJECT_ROOT, RAW_DIR


from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne

load_dotenv(PROJECT_ROOT / ".env")
CITIES = ["Taipei", "NewTaipei"]


# ============================================================
# 文件結構
# ============================================================
#
# route_stops
# ------------------------------------------------------------
# {
#   _id:            "TPE101320_0_7813",     # subRouteUid_direction_versionId
#   sub_route_uid:  "TPE101320",
#   direction:      0,
#   version_id:     7813,
#   route_uid:      "TPE10132",
#   route_name:     "234",
#   authority:      "Taipei",               # 業管機關（非地理位置）
#   operators:      [{id, name, code, no}],
#   stops: [
#     { seq: 1, stop_uid, stop_id, name,
#       station_id: "2717",                 # ← 跨機關唯一，轉乘 join key
#       boarding: 0,                        # -1 可下車 / 0 可上下 / 1 可上車
#       lat, lon, geohash,
#       location_city: "NWT" }              # ← 站牌實際所在城市
#   ],
#   valid_from:     ISODate,
#   valid_to:       null,                   # 被新版取代時填入
#   source_updated: ISODate
# }
#
# stations
# ------------------------------------------------------------
# {
#   _id:          "1755_a3f2c1",            # stationId_contentHash
#   station_id:   "1755",
#   content_hash: "a3f2c1",
#   name:         "西門國小(臺大醫院北護分院)",
#   address:      "康定路46號對面(向北)",
#   bearing:      "N",                      # 對向配對用
#   lat, lon, geohash,
#   location_city: "TPE",
#   routes: [                               # ← 兩市合併後的聯集
#     { stop_uid: "TPE33244", route_uid: "TPE10132",
#       route_name: "234", authority: "Taipei" },
#     { stop_uid: "NWT20183", route_uid: "NWT10116",
#       route_name: "242", authority: "NewTaipei" },
#     ...
#   ],
#   route_count:  10,
#   valid_from:   ISODate,
#   valid_to:     null
# }
#
# 為何 stations 用 content_hash 而非 version_id：
#   合併兩市後，來源的 VersionID 各自獨立（TPE 7813 / NWT 8282），
#   無法用單一版號代表合併後的狀態，改以內容雜湊偵測變動。
# ============================================================


def _hash(obj):
    """內容雜湊，用於偵測文件是否真的變動。"""
    s = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode()).hexdigest()[:12]


def _read_raw(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


# ------------------------------------------------------------
# route_stops
# ------------------------------------------------------------
def build_route_stops(raw, authority):
    docs = []
    for r in raw:
        stops = [
            {
                "seq": s["StopSequence"],
                "stop_uid": s["StopUID"],
                "stop_id": s["StopID"],
                "name": s["StopName"]["Zh_tw"],
                "station_id": s["StationID"],
                "boarding": s["StopBoarding"],
                "lat": s["StopPosition"]["PositionLat"],
                "lon": s["StopPosition"]["PositionLon"],
                "geohash": s["StopPosition"].get("GeoHash"),
                "location_city": s.get("LocationCityCode"),
            }
            for s in sorted(r["Stops"], key=lambda x: x["StopSequence"])
        ]

        docs.append({
            "_id": f'{r["SubRouteUID"]}_{r["Direction"]}_{r["VersionID"]}',
            "sub_route_uid": r["SubRouteUID"],
            "direction": r["Direction"],
            "version_id": r["VersionID"],
            "route_uid": r["RouteUID"],
            "route_name": r["RouteName"]["Zh_tw"],
            "sub_route_name": r["SubRouteName"]["Zh_tw"],
            "authority": authority,
            "operators": [
                {"id": o["OperatorID"], "no": o["OperatorNo"],
                 "name": o["OperatorName"]["Zh_tw"], "code": o.get("OperatorCode")}
                for o in r.get("Operators", [])
            ],
            "stops": stops,
            "stop_count": len(stops),
            "source_updated": r["UpdateTime"],
        })
    return docs


# ------------------------------------------------------------
# stations —— 關鍵：合併而非覆蓋
# ------------------------------------------------------------
def build_stations(raw_by_city):
    """
    兩市對同一 StationID 回傳的 Stops 是互斥的（各機關只報自己的路線），
    因此必須聯集，不能後者覆蓋前者。
    """
    merged = {}

    for authority, raw in raw_by_city.items():
        for r in raw:
            sid = r["StationID"]
            pos = r["StationPosition"]

            if sid not in merged:
                merged[sid] = {
                    "station_id": sid,
                    "name": r["StationName"]["Zh_tw"],
                    "address": r.get("StationAddress"),
                    "bearing": r.get("Bearing"),          # 8 筆為 None
                    "lat": pos["PositionLat"],
                    "lon": pos["PositionLon"],
                    "geohash": pos.get("GeoHash"),
                    "location_city": r.get("LocationCityCode"),
                    "routes": [],
                    "_seen_stop_uids": set(),
                    "location": {
                        "type": "Point",
                        "coordinates": [pos["PositionLon"], pos["PositionLat"]],   # 注意順序！
                    },
                }

            for s in (r.get("Stops") or []):
                uid = s["StopUID"]
                if uid in merged[sid]["_seen_stop_uids"]:
                    continue                              # 冪等：重跑不重複
                merged[sid]["_seen_stop_uids"].add(uid)
                merged[sid]["routes"].append({
                    "stop_uid": uid,
                    "stop_id": s["StopID"],
                    "route_uid": s["RouteUID"],
                    "route_name": s["RouteName"]["Zh_tw"],
                    "authority": authority,
                })

    docs = []
    for sid, d in merged.items():
        d.pop("_seen_stop_uids")
        d["routes"].sort(key=lambda x: x["stop_uid"])
        d["route_count"] = len(d["routes"])                          # 站牌記錄數
        d["unique_routes"] = sorted({r["route_uid"] for r in d["routes"]})
        d["unique_route_count"] = len(d["unique_routes"])            # ← 真正的路線數
        d["content_hash"] = _hash(d)
        d["_id"] = f'{sid}_{d["content_hash"]}'
        docs.append(d)
    return docs


# ------------------------------------------------------------
# 版本化寫入
# ------------------------------------------------------------
def upsert_versioned(coll, docs, key_field, now=None):
    """
    只在內容真的變動時新增版本：
      - _id 已存在 → 完全沒變，跳過
      - 同 key 有舊版 → 舊版標記 valid_to，新版 valid_from
    """
    now = now or datetime.now(timezone.utc)
    existing_ids = {
        d["_id"] for d in coll.find({"_id": {"$in": [x["_id"] for x in docs]}}, {"_id": 1})
    }

    new_docs = [d for d in docs if d["_id"] not in existing_ids]
    if not new_docs:
        return {"inserted": 0, "closed": 0, "unchanged": len(docs)}

    # 舊版封版
    keys = [d[key_field] for d in new_docs]
    closed = coll.update_many(
        {key_field: {"$in": keys}, "valid_to": None},
        {"$set": {"valid_to": now}},
    ).modified_count

    for d in new_docs:
        d["valid_from"] = now
        d["valid_to"] = None
    coll.insert_many(new_docs)

    return {"inserted": len(new_docs), "closed": closed,
            "unchanged": len(docs) - len(new_docs)}


def ensure_indexes(db):
    db.route_stops.create_index(
        [("sub_route_uid", ASCENDING), ("direction", ASCENDING),
         ("version_id", DESCENDING)])
    db.route_stops.create_index([("valid_to", ASCENDING)])
    db.route_stops.create_index([("stops.station_id", ASCENDING)])

    # 轉乘分析的核心：某站位有哪些路線經過
    db.stations.create_index([("station_id", ASCENDING), ("valid_to", ASCENDING)])
    db.stations.create_index([("routes.route_uid", ASCENDING)])
    db.stations.create_index([("geohash", ASCENDING)])       # 對向配對用
    db.stations.create_index([("bearing", ASCENDING)])
    db.stations.create_index([("unique_route_count", DESCENDING)])
    db.stations.create_index([("location", "2dsphere")])


# ------------------------------------------------------------
def main(raw_dir):
    client = MongoClient(os.environ["MONGO_URI"])
    db = client.tdx
    ensure_indexes(db)
    
    # route_stops：兩市各自獨立，不需合併
    for city in CITIES:
        raw = _read_raw(f"{raw_dir}/StopOfRoute_{city}.json.gz")
        docs = build_route_stops(raw, city)
        r = upsert_versioned(db.route_stops, docs, "sub_route_uid")
        print(f"route_stops/{city}: {r}")

    # stations：必須合併兩市
    raw_by_city = {
        c: _read_raw(f"{raw_dir}/Station_{c}.json.gz") for c in CITIES
    }
    docs = build_stations(raw_by_city)
    r = upsert_versioned(db.stations, docs, "station_id")
    print(f"stations: {r}")

    # 驗證
    print("\n--- 驗證 ---")
    print("stations 現行版本:", db.stations.count_documents({"valid_to": None}))
    print("預期: 11436")
    sample = db.stations.find_one({"station_id": "1755", "valid_to": None})
    print(f'StationID 1755 路線數: {sample["route_count"]}  (預期 10)')


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    main(RAW_DIR / "static" / date)