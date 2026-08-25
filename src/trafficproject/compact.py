# ------------------------------------------------------------
from datetime import datetime, timedelta
import sys
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
from pathlib import Path
import traceback
from trafficproject.logging_util import make_logger

import pyarrow as pa
import pyarrow.parquet as pq

import gcsfs

TPE = "Asia/Taipei"

TMP_DIR = Path("/tmp/compact")
TMP_DIR.mkdir(parents=True, exist_ok=True)

log = make_logger("compact")
fs = gcsfs.GCSFileSystem()

def build_keep_mask(file_list):
    key_list = []
    count_list = []

    for file_path in file_list:
        df = pd.read_parquet(f"gs://{file_path}", columns=["kafka_partition", "kafka_offset"])
        partition = df["kafka_partition"].to_numpy(dtype="int64")
        offset    = df["kafka_offset"].to_numpy(dtype="int64")
        key = (partition << 48) | offset

        key_list.append(key)
        count_list.append(len(key))

    all_keys = np.concatenate(key_list) 
    del key_list 
    keep = ~pd.Series(all_keys).duplicated().to_numpy()

    log(f"檔數        {len(file_list)}")
    log(f"總列數      {len(all_keys):,}")
    log(f"去重後列數  {keep.sum():,}")
    log(f"重複筆數    {len(all_keys) - keep.sum():,}")

    return count_list, keep


def stage_generator(file_list, city, date):
    local_tmp = TMP_DIR / f"{city}_{date}.parquet"

    count_list, keep = build_keep_mask(file_list)

    writer = None
    start = 0

    for file_path, count in zip(file_list, count_list):
        table = pq.read_table(f"gs://{file_path}")
        mask = keep[start:start + count]
        start += count
        assert len(mask) == table.num_rows

        filtered = table.filter(pa.array(mask))
        if filtered.num_rows == 0:
            continue                                    # 整檔都是重複，跳過

        if writer is None:
            writer = pq.ParquetWriter(local_tmp, filtered.schema, compression="zstd")
        try:
            writer.write_table(filtered)
        except Exception:
            log(f"schema 不符：{file_path}")
            raise

    if writer is None:
        return "", 0
    else:
        writer.close()
        return local_tmp, keep.sum()

def upload(local_tmp, city, date): 
    name = f"silver/positions/city={city}/dt={date}/positions.parquet" 
    fs.put(str(local_tmp), f"tkr102-traffic-data/{name}")
    return name

def verify(file_name, expected_rows):
    gcs_path = f"gs://tkr102-traffic-data/{file_name}"
    df = pd.read_parquet(gcs_path, columns=["kafka_partition", "kafka_offset"])
    assert len(df) == expected_rows, f"列數不符 {len(df)} != {expected_rows}"
    key = (df["kafka_partition"].to_numpy("int64") << 48) | df["kafka_offset"].to_numpy("int64")
    assert len(key) == np.unique(key).size, f"silver 檔內有重複 {len(key)} != {np.unique(key).size}"

def compact_action(city, date):
    today = datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d")
    if date >= today:
        raise ValueError(f"{date} 尚未結束，不可壓實")

    target_glob = f"gs://tkr102-traffic-data/staging/positions/city={city}/dt={date}/*.parquet"
    silver_key  = f"tkr102-traffic-data/silver/positions/city={city}/dt={date}/positions.parquet"

    file_list = sorted(fs.glob(target_glob))

    if not file_list:
        if fs.exists(silver_key):
            log(f"{city} {date} 已完成，略過")
            return
        raise FileNotFoundError(f"找不到 staging 檔案：{target_glob}")

    if fs.exists(silver_key):
        log(f"⚠️ {city} {date} silver 已存在，但 staging 還有 {len(file_list)} 個檔")
        log(f"   可能是重放或補積壓寫入。需人工確認，本次略過。")
        return

    tmp_parquet_path, expected_rows = stage_generator(file_list, city, date)
    if tmp_parquet_path == "":
        raise ValueError(f"{date} 檔案錯誤")

    file_name = upload(tmp_parquet_path, city, date)
    verify(file_name, expected_rows)

    fs.rm(file_list)
    Path(tmp_parquet_path).unlink()

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

    failed = 0
    for c in cities:
        try:
            compact_action(c, date)
        except Exception as e:
            log(f"{c} {date} 壓實失敗: {type(e).__name__}: {e}\n{traceback.format_exc()}")
            failed += 1
    if failed:
        sys.exit(1)  