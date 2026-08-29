import gc
import sys
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import glob
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import shutil

from trafficproject.transform import to_table
from trafficproject.paths import RAW_DIR, PARQUET_DIR

batch_size = 100

def parse_snapshot_time(filename, date):
    """104252.json.gz + 2026-08-03 → Timestamp(台北)"""
    hhmmss = filename.replace(".json.gz", "")
    return pd.Timestamp(
        f"{date} {hhmmss[:2]}:{hhmmss[2:4]}:{hhmmss[4:6]}",
        tz="Asia/Taipei",
    )

def temp_save(temp_dir, source_dir, city, date):
    temp_parquet_files = []
    all_files = sorted(source_dir.glob("*.json.gz"))

    for i in range(0, len(all_files), batch_size):
        batch_files = all_files[i:i + batch_size]
        batch_num = i // batch_size
        temp_file_path = temp_dir / f"batch_{batch_num:04d}.parquet"
        print(f"正在處理第{batch_num}批，共{len(batch_files)}個檔案…")

        df_list = []

        for filePath in batch_files:
            try:
                df_single = pd.read_json(
                    filePath,
                    compression="gzip",
                    dtype={
                        "OperatorID": str,
                        "OperatorNo": str,
                        "RouteID": str,
                        "SubRouteID": str,
                        "PlateNumb": str,
                        "RouteUID": str,
                        "SubRouteUID": str,
                    },
                )
                df_single["SnapshotTime"] = parse_snapshot_time(filePath.name, date)
                df_list.append(df_single)
            except Exception as e:
                print(f"  讀取失敗 {filePath}: {e}")

        if not df_list:
            print(f"  第{batch_num}批沒有成功讀取任何檔案，跳過。")
            continue

        # 合併 DataFrame
        try:
            combined_df = pd.concat(df_list, ignore_index=True)
            table = to_table(combined_df, city, date)
            pq.write_table(table, temp_file_path, compression="zstd")
            
            temp_parquet_files.append(temp_file_path)
        except Exception as e:
            print(f"  第{batch_num}批暫存失敗: {e}")
            continue
        finally:
            del df_list, combined_df
            gc.collect()

    return temp_parquet_files

def merge_temp_parquet(temp_parquet_files, output_dir, date):
    if temp_parquet_files:
        parquest_wirte = None
        output_path = output_dir / f"{date}.parquet"
        for i, temp_file in enumerate(temp_parquet_files):
            try:
                table = pq.read_table(temp_file)
                if parquest_wirte is None:
                    parquest_wirte = pq.ParquetWriter(output_path, table.schema, compression="zstd")

                parquest_wirte.write_table(table)
                print(f"{temp_file.name} 合併完成")
            except Exception as e:
                print(f"{temp_file} 合併失敗: {e}")
                # print("預期:", parquet_writer.schema)
                # print("實際:", table.schema)
                raise

        if parquest_wirte:
            parquest_wirte.close()
    else:
        print(f"無檔案可合併")

def convert_to_parquet(city, date):
    # """把一天的所有 gz 檔讀成一個 DataFrame，並存成 parquet。"""
    source_dir = RAW_DIR / city / date
    output_dir = PARQUET_DIR / city
    temp_dir = PARQUET_DIR / city / "temp"

    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)

    # 先把所有的 json.gz 檔案分批讀取，轉成 parquet，存到暫存資料夾
    temp_parquet_files = temp_save(temp_dir, source_dir, city, date)

    # 合併所有暫存parquet
    merge_temp_parquet(temp_parquet_files, output_dir, date)

    # 移除暫存檔
    if temp_dir.exists():
        # 移除整個目錄，無論該目錄是否有內容
        shutil.rmtree(temp_dir)
        print(f"{temp_dir.name} 已刪除")


if __name__ == "__main__":
    today = datetime.now(ZoneInfo("Asia/Taipei"))
    # 
    # Default city: Taipei
    # Default date: yesterday
    # 
    cities = ["Taipei", "NewTaipei"]
    date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    if len(sys.argv) > 1:
        cities = [sys.argv[1]]
        date = sys.argv[2]
    elif len(sys.argv) > 0:
        cities = [sys.argv[1]]
    print(date)
    for i in range(len(cities)):
        convert_to_parquet(cities[i], date)
