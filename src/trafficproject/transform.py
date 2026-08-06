"""
原始 JSON → 標準化 DataFrame 的轉換層

職責：
  1. 攤平巢狀欄位
  2. 統一命名（snake_case）
  3. 時間字串 → UTC timestamp
  4. schema 漂移偵測

型別由 SCHEMA 強制，不讓 pandas 自行推斷。
識別碼一律 string——避免 OperatorNo "0303" 被推斷成 int 而遺失前導零。
"""

import pandas as pd
import pyarrow as pa

# ---------- 目標 schema ----------
SCHEMA = pa.schema([
    # 分區資訊
    ("city",            pa.string()),
    ("data_date",       pa.string()),

    # 識別碼 —— 一律字串
    ("plate_numb",      pa.string()),
    ("operator_id",     pa.string()),
    ("operator_no",     pa.string()),
    ("route_uid",       pa.string()),
    ("route_id",        pa.string()),
    ("route_name",      pa.string()),
    ("sub_route_uid",   pa.string()),
    ("sub_route_id",    pa.string()),
    ("sub_route_name",  pa.string()),

    # 代碼
    ("direction",       pa.int8()),
    ("duty_status",     pa.int8()),
    ("bus_status",      pa.int16()),   # 有 255，int8 會溢位

    # 量測值
    ("lat",             pa.float64()),
    ("lon",             pa.float64()),
    ("geohash",         pa.string()),
    ("speed",           pa.int16()),
    ("azimuth",         pa.int32()),   # 0-65535

    # 時間 —— 存 UTC，讀取時再轉台北
    ("gps_time",        pa.timestamp("s", tz="UTC")),
    ("src_update_time", pa.timestamp("s", tz="UTC")),
    ("update_time",     pa.timestamp("s", tz="UTC")),
    ("snapshot_time",   pa.timestamp("s", tz="UTC")),
])

# ---------- 原始欄位對照 ----------
# 保留這份對照表，日後才知道 lat 原本叫什麼
FLAT_RENAME = {
    "PlateNumb":     "plate_numb",
    "OperatorID":    "operator_id",
    "OperatorNo":    "operator_no",
    "RouteUID":      "route_uid",
    "RouteID":       "route_id",
    "SubRouteUID":   "sub_route_uid",
    "SubRouteID":    "sub_route_id",
    "Direction":     "direction",
    "Speed":         "speed",
    "Azimuth":       "azimuth",
    "DutyStatus":    "duty_status",
    "BusStatus":     "bus_status",
}

# 巢狀欄位：{原始欄位: {子鍵: 新欄名}}
NESTED_RENAME = {
    "RouteName":    {"Zh_tw": "route_name"},
    "SubRouteName": {"Zh_tw": "sub_route_name"},
    "BusPosition":  {
        "PositionLat": "lat",
        "PositionLon": "lon",
        "GeoHash":     "geohash",
    },
}

TIME_COLS = {
    "GPSTime":       "gps_time",
    "SrcUpdateTime": "src_update_time",
    "UpdateTime":    "update_time",
    "SnapshotTime":  "snapshot_time",
}

INTERNAL_COLS = {"SnapshotTime"}

# 預期上游會有的欄位，用於漂移偵測
EXPECTED_RAW_COLS = (
    set(FLAT_RENAME) | set(NESTED_RENAME) | set(TIME_COLS)
) - INTERNAL_COLS


def check_schema_drift(df, strict=False):
    """比對上游實際欄位與預期欄位，回傳差異描述（無差異則回傳 None）。"""
    actual = set(df.columns)
    missing = EXPECTED_RAW_COLS - actual
    extra = actual - EXPECTED_RAW_COLS - INTERNAL_COLS

    if not missing and not extra:
        return None

    msg = []
    if missing:
        msg.append(f"缺少欄位: {sorted(missing)}")
    if extra:
        msg.append(f"新增欄位: {sorted(extra)}")
    result = " / ".join(msg)

    if strict:
        raise ValueError(f"SCHEMA DRIFT: {result}")
    return result


def _expand_nested(df, col, mapping):
    """把 dict 欄位攤平成獨立欄位。缺欄時補 None，避免整批炸掉。"""
    if col not in df.columns:
        for new_name in mapping.values():
            df[new_name] = None
        return df

    sub = pd.json_normalize(df[col])
    for child_key, new_name in mapping.items():
        df[new_name] = sub[child_key].values if child_key in sub.columns else None
    return df


def transform(df, city, data_date, strict=False):
    """
    原始 DataFrame → 符合 SCHEMA 的 DataFrame

    city / data_date 由呼叫端傳入（來自目錄結構），
    不從資料內容推斷——因為營運日的定義由收集當下決定。
    """
    drift = check_schema_drift(df, strict=strict)
    if drift:
        print(f"  ⚠️ SCHEMA DRIFT: {drift}")

    df = df.copy()

    # 1. 攤平巢狀
    for col, mapping in NESTED_RENAME.items():
        df = _expand_nested(df, col, mapping)

    # 2. 扁平欄位改名
    df = df.rename(columns=FLAT_RENAME)

    # 3. 時間 → UTC timestamp
    for old, new in TIME_COLS.items():
        if old in df.columns:
            df[new] = (
                pd.to_datetime(df[old], format="ISO8601", utc=True)
                  .dt.floor("s")
            )
        else:
            df[new] = pd.NaT

    # 4. 補上分區欄位
    df["city"] = city
    df["data_date"] = data_date
    df["data_date"] = data_date

    # 5. 只保留 SCHEMA 定義的欄位，並固定順序
    cols = [f.name for f in SCHEMA]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]


def to_table(df, city, data_date, strict=False):
    """轉換並套用 SCHEMA。型別對不上會直接拋錯——這正是我們要的。"""
    out = transform(df, city, data_date, strict=strict)
    return pa.Table.from_pandas(out, schema=SCHEMA, preserve_index=False)