# ------------------------------------------------------------
from datetime import datetime, timedelta
import sys
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
from trafficproject.logging_util import make_logger

import gcsfs

TPE = "Asia/Taipei"

log = make_logger("compact")
fs = gcsfs.GCSFileSystem()

def compact_action(city, date):
    target_glob = f"gs://tkr102-traffic-data/staging/positions/city={city}/dt={date}/*.parquet"
    file_count = 0
    total_bytes = 0

    file_list = sorted(fs.glob(target_glob))

    key_list = []
    count_list = []

    fs.glob(target_glob, detail=True)
    for file_path in file_list:
        info = fs.info(file_path)
        file_count += 1
        total_bytes += info["size"]
        df = pd.read_parquet(f"gs://{file_path}", columns=["kafka_partition", "kafka_offset"])
        partition = df["kafka_partition"].to_numpy(dtype="int64")
        offset    = df["kafka_offset"].to_numpy(dtype="int64")
        key = (partition << 48) | offset

        key_list.append(key)
        count_list.append(len(key))
        # print("列數:", len(key), " 不重複:", np.unique(key).size)

        # print(f"📄 {file_path} ({info['size'] / 1024 / 1024:.2f} MB)")

    all_keys = np.concatenate(key_list) 
    keep = ~pd.Series(all_keys).duplicated().to_numpy()

    mask_list = []
    previous_count = 0
    for count in count_list:
        tmp_mask = []
        mask_list.append(keep[previous_count:previous_count + count])

        previous_count += count

    # total_mb = total_bytes / 1024 / 1024

    print(f"檔數        {len(file_list)}")
    print(f"總列數      {len(all_keys):,}")
    print(f"去重後列數  {keep.sum():,}")
    print(f"重複筆數    {len(all_keys) - keep.sum():,}")

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

    # all_stops = load_all_stops()        # 只載一次，兩個城市共用
    for c in cities:
        compact_action(c, date)