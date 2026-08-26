"""
歷史 positions 回填 —— 把 Mac mini 上舊管線的 parquet 補進 GCS silver

輸入：output/parquet/{city}/{date}.parquet      （舊批次管線的產物）
輸出：gs://{bucket}/silver/positions/city={city}/dt={date}/positions.parquet

為什麼要做
------------------------------------------------------------
  Kafka 路徑從 2026-08-20 20:11 才開始，silver 只有 6 天。
  基準表的統計效力上限由天數決定：站牌對 + 時段桶目前中位樣本 41，
  n>=50 只佔 46%，p90 算不準——而 p90 才是「可靠度」的核心指標。
  舊管線從 08-04 就在跑，補進來平日樣本增為約四倍。

schema 對齊：從正典層讀，不要在這裡重述
------------------------------------------------------------
  舊 parquet 沒有 kafka_partition / kafka_offset / consume_time /
  pipeline_version 這幾欄。BigQuery 外部表對同一 prefix 下的 parquet
  schema 差異很敏感，混進去可能整張表壞掉。

  對齊的方式是**讀一個現有的 silver 檔當範本**，而不是在這支程式裡
  把 27 個欄位再抄一遍。抄一遍就等於製造了第二份真相，
  哪天 archiver 加欄位，這裡會無聲地開始產出不相容的檔案。

08-20 的特殊處理
------------------------------------------------------------
  那天兩邊都有：GCS 只有 cutover 之後約 3.8 小時，本地是完整一天。
  作法不是「合併後去重」（2000 萬列的去重在筆電上很痛），
  而是**以 GCS 檔的最早時間為界切開**：本地取界線之前，GCS 取全部。
  這樣結構上就不會重疊，不需要去重。

已知不完整的日期
------------------------------------------------------------
  08-16（週日）兩市都只有正常週日的一半，是收集中斷。
  本程式仍然照常上傳（那是真實資料，不該丟），
  但會在摘要裡標出來，由基準計算階段決定要不要排除。
"""

import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import numpy as np
import gcsfs

from trafficproject.paths import PARQUET_DIR
from trafficproject.logging_util import make_logger

log = make_logger("backfill_positions")

GCS_BUCKET = os.environ["GCS_BUCKET"]
SILVER = f"{GCS_BUCKET}/silver/positions"

# Kafka 路徑已經產出、且已驗證等價的日期——絕不覆蓋
PROTECTED = {"2026-08-21", "2026-08-22", "2026-08-23", "2026-08-24", "2026-08-25"}

# 那天兩邊都有資料，走切界合併而不是覆蓋
CUTOVER_DATE = "2026-08-20"

# 讀 schema 範本用的檔案（任一個確定健康的 silver 檔）
REF_CITY, REF_DATE = "Taipei", "2026-08-21"

# 已知不完整，仍然上傳但標記
KNOWN_INCOMPLETE = {"2026-08-16"}

# 補給舊資料的常數欄位。pipeline_version 一定要標，
# 之後查「某段日期統計異常」時，第一個問題就是「這段是哪條路徑進來的」
CONST_COLS = {"pipeline_version": "legacy-batch"}

# 判斷 cutover 界線用的欄位，依偏好順序，取兩邊都有的第一個
BOUNDARY_CANDIDATES = ["fetch_time", "snapshot_time", "SnapshotTime",
                       "update_time", "gps_time"]

TMP_DIR = Path("/tmp/backfill_positions")
TMP_DIR.mkdir(parents=True, exist_ok=True)

fs = gcsfs.GCSFileSystem()


def dst_path(city, date):
    return f"{SILVER}/city={city}/dt={date}/positions.parquet"


# ------------------------------------------------------------
def load_ref_schema():
    """從既有的 silver 檔讀出正典 schema。"""
    path = dst_path(REF_CITY, REF_DATE)
    if not fs.exists(path):
        raise SystemExit(f"找不到 schema 範本 {path}，無法對齊")
    with fs.open(path, "rb") as f:
        schema = pq.ParquetFile(f).schema_arrow
    log(f"schema 範本 {REF_CITY} {REF_DATE}：{len(schema)} 欄")
    return schema


def align(table, ref_schema, consts=None, quiet=False):
    """
    把 parquet 對齊到正典 schema：缺的補 null，多的丟掉，型別轉正。

    consts=None 用預設的 CONST_COLS（回填舊資料時標 legacy-batch）；
    對齊「已經在 GCS 裡的檔」時要傳 {}，否則會把 Kafka 路徑寫進來的列
    誤標成 legacy-batch——那比不對齊還糟，因為它污染的是溯源資訊。
    """
    consts = CONST_COLS if consts is None else consts
    have = set(table.schema.names)
    dropped = sorted(have - set(ref_schema.names) - set(consts))
    missing, arrays = [], []

    for field in ref_schema:
        if field.name in consts:
            v = consts[field.name]
            arrays.append(pa.array(np.full(len(table), v, dtype=object),
                                   type=field.type))
        elif field.name in have:
            arrays.append(table.column(field.name).cast(field.type))
        else:
            missing.append(field.name)
            arrays.append(pa.nulls(len(table), type=field.type))

    if dropped and not quiet:
        log(f"  ⚠️ 舊檔有、正典沒有的欄位被丟棄：{dropped}")
    if missing and not quiet:
        log(f"  補 null 欄位 {len(missing)} 個：{missing}")
    return pa.Table.from_arrays(arrays, schema=ref_schema)


# ------------------------------------------------------------
def pick_boundary(a, b):
    for name in BOUNDARY_CANDIDATES:
        if name in a.schema.names and name in b.schema.names:
            if a.column(name).null_count < len(a):
                return name
    raise SystemExit(f"兩邊沒有共同可用的時間欄位，候選：{BOUNDARY_CANDIDATES}")


def merge_cutover(local_tbl, gcs_tbl):
    """
    以 GCS 檔的最早時間為界切開，不做去重。
    界線來自資料本身而不是寫死的 20:11——寫死的話，
    哪天回頭看會分不清那個數字是量到的還是猜的。
    """
    col = pick_boundary(local_tbl, gcs_tbl)
    t0 = pc.min(gcs_tbl.column(col)).as_py()
    log(f"  合併界線：{col} < {t0}")

    keep = pc.less(local_tbl.column(col), pa.scalar(t0, type=gcs_tbl.schema.field(col).type))
    n_null = pc.sum(pc.is_null(keep)).as_py() or 0
    if n_null:
        # 界線欄位為 null 的列無法判斷屬於哪一邊，保留而不丟棄；
        # 萬一真的重疊，extract_day 的 drop_duplicates 會再擋一次
        log(f"  ⚠️ 界線欄位有 {n_null:,} 列為 null，一律保留")
        keep = pc.fill_null(keep, True)

    before = local_tbl.filter(keep)
    log(f"  本地 {len(local_tbl):,} → 界線前 {len(before):,}；"
        f"GCS {len(gcs_tbl):,}")
    return pa.concat_tables([before, gcs_tbl])


# ------------------------------------------------------------
def upload_verify(table, city, date):
    tmp = TMP_DIR / f"positions_{city}_{date}.parquet"
    pq.write_table(table, tmp, compression="zstd")
    dst = dst_path(city, date)
    fs.put(str(tmp), dst)
    tmp.unlink()

    # 驗證讀的是上傳後的遠端檔，不是本地那份——本地那份必然是對的，
    # 驗它等於什麼都沒驗
    with fs.open(dst, "rb") as f:
        meta = pq.ParquetFile(f)
        n_remote = meta.metadata.num_rows
        names_remote = meta.schema_arrow.names
    if n_remote != len(table):
        raise SystemExit(f"列數不符：本地 {len(table):,} 遠端 {n_remote:,}")
    if list(names_remote) != list(table.schema.names):
        raise SystemExit(f"欄位不符：遠端 {names_remote}")
    return dst, n_remote


# ------------------------------------------------------------
def backfill_day(city, date, ref_schema, force=False):
    t0 = time.time()
    if date in PROTECTED:
        log(f"{city} {date}: 跳過（Kafka 路徑產出，已驗證等價）")
        return None

    src = PARQUET_DIR / city / f"{date}.parquet"
    if not src.exists():
        log(f"{city} {date}: ⚠️ 本地找不到 {src}")
        return None

    dst = dst_path(city, date)
    if fs.exists(dst) and date != CUTOVER_DATE and not force:
        log(f"{city} {date}: 跳過（GCS 已存在，要覆蓋請加 --force）")
        return None

    table = align(pq.read_table(src), ref_schema)
    log(f"{city} {date}: 讀入 {len(table):,} 列 ({time.time()-t0:.1f}s)")

    if date == CUTOVER_DATE and fs.exists(dst):
        with fs.open(dst, "rb") as f:
            gcs_tbl = pq.read_table(f)
        # GCS 那張也要對齊到同一個 ref。它跟範本檔是 archiver 在不同時間
        # 寫的，型別若有一點漂移，concat_tables 會丟出很難讀的錯。
        # consts={} 是關鍵：這些列來自 Kafka，不能被標成 legacy-batch。
        table = merge_cutover(table, align(gcs_tbl, ref_schema,
                                           consts={}, quiet=True))

    dst, n = upload_verify(table, city, date)
    flag = "  ⚠️ 已知不完整" if date in KNOWN_INCOMPLETE else ""
    log(f"  寫出 {n:,} 列 → {dst}  ({time.time()-t0:.1f}s){flag}")
    return {"city": city, "date": date, "rows": n}


# ------------------------------------------------------------
if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    force = "--force" in sys.argv

    cities = [args[0]] if args else ["Taipei", "NewTaipei"]
    if len(args) > 1:
        dates = args[1:]
    else:
        # 本地有什麼就補什麼，PROTECTED 由 backfill_day 逐日擋掉
        dates = sorted(p.stem for p in (PARQUET_DIR / cities[0]).glob("*.parquet"))

    log(f"回填 {len(cities)} 城 × {len(dates)} 天，force={force}")
    ref_schema = load_ref_schema()

    done = []
    for c in cities:
        for d in dates:
            r = backfill_day(c, d, ref_schema, force=force)
            if r:
                done.append(r)

    log("=" * 56)
    log(f"完成 {len(done)} 個分區")
    for r in done:
        mark = " ⚠️" if r["date"] in KNOWN_INCOMPLETE else ""
        log(f"  {r['city']:10s} {r['date']}  {r['rows']:>12,}{mark}")