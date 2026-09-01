"""
服務層匯出 —— BigQuery / MongoDB → MySQL

執行：
    .venv/bin/python -m trafficproject.export_serving baseline
    .venv/bin/python -m trafficproject.export_serving routes
    .venv/bin/python -m trafficproject.export_serving all

為什麼是「全量重載 + 原子交換」而不是增量更新
------------------------------------------------------------
  基準表只有 14.5 萬列、站序幾萬列，全量重載幾秒就好。
  增量更新要處理「哪些格子消失了」，而那正是最容易漏的一種情況：
  某個站牌對這次沒有資料，舊的那列會留在表裡，變成一個
  永遠不會更新、看起來卻很正常的過期數字。

  交換用 RENAME TABLE（MySQL 原子操作）。直接 TRUNCATE + INSERT 的話，
  中間那幾十秒服務層會查到空表或半份資料。

  這跟壓實「先寫檔、後提交 offset」、archiver「上傳成功才 commit」
  是同一個原則：切換只能發生在你已確認完成的東西上。
"""

import os
import sys
import time
from datetime import datetime, timezone

import pymysql
from pymongo import MongoClient
from google.cloud import bigquery

from trafficproject.paths import PROJECT_ROOT
from trafficproject.logging_util import make_logger

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")          # MONGO_URI
load_dotenv(PROJECT_ROOT / ".env.mysql")    # MYSQL_*

log = make_logger("export_serving")

BATCH = 2000

# ---------------------------------------------------------------------
# 欄位清單只寫一次，SELECT 與 INSERT 共用。
# 分開寫兩份的話，哪天加一欄而只改了一邊，資料會靜靜地錯位——
# 型別剛好相容時連錯誤都不會有。
# ---------------------------------------------------------------------
BASELINE_COLS = [
    "city", "from_station_id", "to_station_id", "day_type", "band",
    "level", "n", "n_route",
    "p25_sec", "p50_sec", "p75_sec", "p90_sec",
    "unreliability", "p90_reliable",
    "mean_sec", "sd_sec",
    "date_from", "date_to", "n_days",
    "rule_version", "baseline_version", "computed_at",
]

ROUTE_COLS = [
    "city", "sub_route_uid", "direction", "seq",
    "station_id", "stop_name", "boarding", "version_id", "loaded_at",
]

# sub_route_uid 的前綴 → 城市。未知前綴會讓程式停下來，不會猜。
CITY_BY_PREFIX = {"TPE": "Taipei", "NWT": "NewTaipei"}


# ---------------------------------------------------------------------
def mysql_conn():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
        charset="utf8mb4",          # 站名是中文，少了這個會變問號
        autocommit=False,
    )


def load_table(conn, table, cols, rows):
    """
    載進 {table}_new → 驗證 → RENAME 原子交換 → 丟掉舊表。

    {table}_new 用 CREATE TABLE ... LIKE 建，schema 只有 schema.sql 一份定義。
    在這裡重述欄位型別就等於製造第二份真相。
    """
    tmp, old = f"{table}_new", f"{table}_old"
    n_src = len(rows)
    if n_src == 0:
        raise SystemExit(f"{table}: 來源 0 列，中止（不要用空表覆蓋線上資料）")

    with conn.cursor() as cur:
        # 前一次失敗可能留下殘骸，先清掉才能重跑
        cur.execute(f"DROP TABLE IF EXISTS `{tmp}`")
        cur.execute(f"DROP TABLE IF EXISTS `{old}`")
        cur.execute(f"CREATE TABLE `{tmp}` LIKE `{table}`")

        sql = (f"INSERT INTO `{tmp}` ({', '.join(cols)}) "
               f"VALUES ({', '.join(['%s'] * len(cols))})")
        t0 = time.time()
        for i in range(0, n_src, BATCH):
            cur.executemany(sql, rows[i:i + BATCH])
        conn.commit()
        log(f"  {table}: 寫入 {n_src:,} 列 ({time.time() - t0:.1f}s)")

        # 驗證：斷言的第一個參數必須是「要判斷真假的運算式」。
        # 寫成 f-string 的話非空字串永遠為真，斷言永遠通過——
        # 而下一行就是不可逆的 RENAME。
        cur.execute(f"SELECT COUNT(*) FROM `{tmp}`")
        n_dst = cur.fetchone()[0]
        assert n_dst == n_src, f"{table}: 來源 {n_src:,} 寫入後 {n_dst:,}，不符"

        # RENAME TABLE 的多組交換是原子的：服務層不會看到空表或半份資料
        cur.execute(f"RENAME TABLE `{table}` TO `{old}`, `{tmp}` TO `{table}`")
        cur.execute(f"DROP TABLE `{old}`")
        conn.commit()
    log(f"  {table}: 交換完成")


# ---------------------------------------------------------------------
def fetch_baseline():
    client = bigquery.Client()
    sql = f"SELECT {', '.join(BASELINE_COLS)} FROM `bus.baseline_segment`"
    t0 = time.time()
    rows = []
    for r in client.query(sql).result():
        v = [r[c] for c in BASELINE_COLS]
        # BigQuery 的 TIMESTAMP 帶時區，MySQL 的 DATETIME 不存時區。
        # 明確轉成 UTC naive，不要讓驅動程式替你決定。
        ts = v[BASELINE_COLS.index("computed_at")]
        if ts is not None and ts.tzinfo is not None:
            v[BASELINE_COLS.index("computed_at")] = (
                ts.astimezone(timezone.utc).replace(tzinfo=None))
        rows.append(tuple(v))
    log(f"  BigQuery 讀出 {len(rows):,} 列 ({time.time() - t0:.1f}s)")
    return rows


def fetch_routes():
    db = MongoClient(os.environ["MONGO_URI"]).tdx
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows, seen_prefix = [], {}

    for doc in db.route_stops.find(
        {"valid_to": None},
        {"sub_route_uid": 1, "direction": 1, "version_id": 1, "stops": 1, "_id": 0},
    ):
        sru = doc["sub_route_uid"]
        prefix = sru[:3]
        seen_prefix[prefix] = seen_prefix.get(prefix, 0) + 1
        city = CITY_BY_PREFIX.get(prefix)
        if city is None:
            raise SystemExit(
                f"未知的 sub_route_uid 前綴 {prefix!r}（例：{sru}）。"
                f"請把它加進 CITY_BY_PREFIX，不要讓程式猜。")

        for s in doc["stops"]:
            rows.append((
                city, sru, int(doc["direction"]), int(s["seq"]),
                str(s["station_id"]), s["name"], s.get("boarding"),
                doc["version_id"], now,
            ))

    log(f"  MongoDB 讀出 {len(rows):,} 列 / 子路線前綴 {seen_prefix}")
    return rows


# ---------------------------------------------------------------------
def verify(conn):
    """交換後的合理性檢查。數字前面量過很多次，對不上就是這支程式有問題。"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT day_type, level, COUNT(*),
                   ROUND(AVG(p50_sec), 1), ROUND(AVG(unreliability), 2)
            FROM baseline_segment GROUP BY day_type, level ORDER BY 1, 2""")
        log("  baseline_segment  day_type / level / 格數 / p50平均 / 不可靠指數平均")
        for r in cur.fetchall():
            log(f"    {r[0]:8s} {r[1]:8s} {r[2]:7,d}  {r[3]:6}  {r[4]}")

        cur.execute("SELECT COUNT(*), COUNT(DISTINCT sub_route_uid) FROM route_stop")
        n, n_route = cur.fetchone()
        log(f"  route_stop  {n:,} 列 / {n_route:,} 條子路線")


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    if what not in ("baseline", "routes", "all"):
        raise SystemExit("用法：export_serving [baseline|routes|all]")

    t0 = time.time()
    conn = mysql_conn()
    try:
        if what in ("baseline", "all"):
            log("baseline_segment：BigQuery → MySQL")
            load_table(conn, "baseline_segment", BASELINE_COLS, fetch_baseline())
        if what in ("routes", "all"):
            log("route_stop：MongoDB → MySQL")
            load_table(conn, "route_stop", ROUTE_COLS, fetch_routes())
        verify(conn)
    finally:
        conn.close()
    log(f"完成，耗時 {time.time() - t0:.1f}s")