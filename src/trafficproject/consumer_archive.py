"""
Consumer A —— 封存器
 
從 Kafka 消費原始車輛位置，套用既有的 transform SCHEMA，
寫成 parquet 上傳到 GCS 的 staging 區。
 
執行：
    uv run python -m trafficproject.consumer_archive
 
設計要點（詳見「Consumer A 設計」文件）
------------------------------------------------------------
1. 先寫檔、後提交 offset。順序反了 = 崩潰即永久遺失。
2. 每筆記錄帶 (kafka_partition, kafka_offset)，壓實時據此去重。
   所以檔名不需要是決定性的 —— 冪等性在記錄層，不在檔案層。
3. 一次 flush 可能跨越日界，寫檔前必須依 (city, data_date) 分組。
"""
 
import io
import os
import json
import time
import signal
import socket
import threading
import traceback
from datetime import datetime, timezone, timedelta
 
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.cloud import storage
 
from trafficproject.paths import PROJECT_ROOT
from trafficproject.transform import to_table
from trafficproject.logging_util import make_logger
 
log = make_logger("consumer_archive")
load_dotenv(PROJECT_ROOT / ".env")
 
# ---------- 設定 ----------
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
 
TOPIC = "bus.position.raw"
GROUP_ID = "archiver-dev"
PIPELINE_VERSION = "archiver-1.0.0"
 
FLUSH_RECORDS = 200_000           # 筆數門檻
FLUSH_SECONDS = 600              # 時間門檻（低流量時段確保會落地）
CONSUME_BATCH = 5_000            # 每次 consume() 最多取幾則
CONSUME_TIMEOUT = 1.0            # 秒
 
TPE = timezone(timedelta(hours=8))
 
# transform 會強制轉字串的欄位 —— 串流路徑要自己做，
# 否則看起來像數字的 ID 會被 pandas 推斷成 int64，前導零消失，
# 產出的值就跟批次路徑對不起來。
ID_COLS = ("OperatorID", "OperatorNo", "RouteID", "SubRouteID",
           "PlateNumb", "RouteUID", "SubRouteUID")
 
# ---------- 狀態 ----------
_stop_event = threading.Event()
_stat = {"consumed": 0, "flushed": 0, "files": 0, "bytes": 0,
         "decode_err": 0, "upload_err": 0}
 
 
def _stop(signum, frame):
    _stop_event.set()
    log(f"收到訊號 {signum}，準備關閉")
 
 
# ------------------------------------------------------------
# Kafka
# ------------------------------------------------------------
def make_consumer():
    """
    enable.auto.commit=False 是本程式正確性的核心。
    自動提交會在背景「已經讀到哪裡」就提交，跟「已經安全寫入 GCS 沒有」
    完全脫鉤 —— 崩潰時就是永久資料遺失。
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": f"archiver-{socket.gethostname()}",
        "enable.auto.commit": False,        # ← 手動提交
        "auto.offset.reset": "earliest",    # 首次啟動從最舊的訊息開始
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 900000,     # flush 可能耗時，放寬避免被踢出群組
        "error_cb": lambda e: log(f"Kafka error: {e}"),
    }
    return Consumer(conf)
 
 
def decode(msg):
    """Kafka 訊息 → dict，附上 Kafka 座標與消費時刻。"""
    rec = json.loads(msg.value())
    rec["kafka_partition"] = msg.partition()
    rec["kafka_offset"] = msg.offset()
    rec["consume_time"] = datetime.now(TPE).isoformat(timespec="milliseconds")
    return rec
 
 
# ------------------------------------------------------------
# 轉換
# ------------------------------------------------------------
def build_table(df, city, data_date):
    """
    套用既有 SCHEMA，再把串流特有的欄位接回去。
 
    不改 transform.py —— 那套跑了兩週、型別驗證過，
    在它產出的 Table 之後 append_column 比較安全。
    """
    meta = df[["kafka_partition", "kafka_offset", "consume_time"]].copy()
 
    # transform 需要 SnapshotTime，但 Kafka 訊息裡沒有
    # （批次路徑是從檔名 parse 出來的）。fetch_time 語意等價。
    df = df.copy()
    df["SnapshotTime"] = df["fetch_time"]
    # 串流特有欄位不該進入 drift 檢查
    df = df.drop(columns=["kafka_partition", "kafka_offset", "consume_time",
                          "fetch_time", "produce_time", "city", "data_date"],
                 errors="ignore")
    
    for c in ID_COLS:
        if c in df.columns:
            df[c] = df[c].astype("string")
 
    table = to_table(df, city, data_date)
 
    n = table.num_rows
    table = table.append_column(
        "kafka_partition", pa.array(meta["kafka_partition"].values, pa.int32()))
    table = table.append_column(
        "kafka_offset", pa.array(meta["kafka_offset"].values, pa.int64()))
    table = table.append_column(
        "consume_time", pa.array(meta["consume_time"].values, pa.string()))
    table = table.append_column(
        "pipeline_version", pa.array([PIPELINE_VERSION] * n, pa.string()))
    return table
 
 
def derive_data_date(fetch_time_series):
    """營運日由觀測時刻決定，轉台北時區後取日期。"""
    return (pd.to_datetime(fetch_time_series, format="ISO8601", utc=True)
              .dt.tz_convert("Asia/Taipei")
              .dt.strftime("%Y-%m-%d"))
 
 
# ------------------------------------------------------------
# GCS
# ------------------------------------------------------------
_gcs = storage.Client()
_bucket = _gcs.bucket(GCS_BUCKET)
 
 
def upload(table, city, data_date):
    """寫進記憶體 buffer 再上傳。回傳 (blob 路徑, 位元組數)。"""
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    data = buf.getvalue()
 
    stamp = datetime.now(TPE).strftime("%Y%m%dT%H%M%S")
    # 檔名不需要決定性 —— 重複由 (kafka_partition, kafka_offset) 在壓實時消除
    name = (f"staging/positions/city={city}/dt={data_date}/"
            f"part-{stamp}-{os.getpid()}-{_stat['files']:05d}.parquet")
 
    blob = _bucket.blob(name)
    blob.upload_from_string(data, content_type="application/octet-stream")
    return name, len(data)
 
 
# ------------------------------------------------------------
# Flush
# ------------------------------------------------------------
def flush(consumer, buffer):
    """
    緩衝區 → GCS → 提交 offset。
 
    任何一個分組上傳失敗就整批不提交，下次重讀。
    重讀造成的重複由壓實階段去重，不會有資料遺失。
    """
    if not buffer:
        return True
 
    t0 = time.time()
    df = pd.DataFrame(buffer)
    df["data_date"] = derive_data_date(df["fetch_time"])
 
    written = []
    try:
        # 一次 flush 可能跨午夜、也一定含兩個城市 —— 必須分組
        for (city, data_date), grp in df.groupby(["city", "data_date"], sort=False):
            table = build_table(grp, city, data_date)
            name, nbytes = upload(table, city, data_date)
            written.append((name, table.num_rows, nbytes))
            _stat["files"] += 1
            _stat["bytes"] += nbytes
    except Exception as e:
        _stat["upload_err"] += 1
        log(f"上傳失敗，不提交 offset：{type(e).__name__}: {e}\n{traceback.format_exc()}")
        return False
 
    # 全部寫入成功之後才提交
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        # 已經寫進 GCS 但 offset 沒提交 → 下次會重讀 → 壓實時去重
        log(f"⚠️ commit 失敗（資料已落地，將重複讀取）：{e}")
 
    _stat["flushed"] += len(buffer)
    log(f"flush {len(buffer):,} 筆 → {len(written)} 檔 "
        f"/ {sum(w[2] for w in written)/1e6:.2f} MB / {time.time()-t0:.1f}s")
    for name, rows, nbytes in written:
        log(f"    {name}  {rows:,} 筆  {nbytes/1e6:.2f} MB")
    return True
 
 
# ------------------------------------------------------------
def main():
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
 
    consumer = make_consumer()
 
    def on_revoke(c, partitions):
        # 失去分區之前先落地，減少重複讀取的量
        log(f"rebalance: 收回 {len(partitions)} 個 partition，先 flush")
        flush(c, buffer)
        buffer.clear()

    def on_assign(c, partitions):
        log(f"已指派 {len(partitions)} 個 partition: "
        f"{sorted(p.partition for p in partitions)}")
    
    buffer = []
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
 
    log(f"archiver start | topic={TOPIC} group={GROUP_ID} bucket={GCS_BUCKET}")
    log(f"flush 門檻：{FLUSH_RECORDS:,} 筆 或 {FLUSH_SECONDS}s")
 
    last_flush = time.time()
 
    try:
        while not _stop_event.is_set():
            msgs = consumer.consume(num_messages=CONSUME_BATCH,
                                    timeout=CONSUME_TIMEOUT)
 
            for msg in msgs:
                err = msg.error()
                if err:
                    # _PARTITION_EOF 只是「讀到目前結尾」，不是錯誤
                    if err.code() != KafkaError._PARTITION_EOF:
                        log(f"消費錯誤: {err}")
                    continue
                try:
                    buffer.append(decode(msg))
                    _stat["consumed"] += 1
                except Exception as e:
                    # 單筆解析失敗不該拖垮整批，但要計數
                    _stat["decode_err"] += 1
                    if _stat["decode_err"] % 100 == 1:
                        log(f"解析失敗（累計 {_stat['decode_err']}）: {e}")
 
            due = (len(buffer) >= FLUSH_RECORDS
                   or (buffer and time.time() - last_flush >= FLUSH_SECONDS))
 
            if due:
                if flush(consumer, buffer):
                    buffer.clear()
                    last_flush = time.time()
                else:
                    # 上傳失敗：保留緩衝、稍候重試。
                    # 緩衝持續成長會吃光記憶體，所以要看得到這個訊息。
                    log(f"⚠️ 保留 {len(buffer):,} 筆待重試")
                    time.sleep(10)
    finally:
        log("關閉中，落地剩餘緩衝…")
        flush(consumer, buffer)
        consumer.close()
        log(f"archiver stopped | {_stat}")
 
 
if __name__ == "__main__":
    main()