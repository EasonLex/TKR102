-- =====================================================================
-- route_stop 加上 route_name（使用者認得的路線號碼，例如 287）
--
-- 執行（在 GCE 上）：
--   docker exec -i mysql sh -c \
--     'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -u root --default-character-set=utf8mb4 bus' \
--     < sql/migrate_route_name.sql
--
-- ★ 順序：先跑這個，再跑 export_serving routes。★
--
--   反過來的話 INSERT 會因為 Unknown column 而失敗——不過那是安全的失敗：
--   load_table 是先寫進 route_stop_new、驗完筆數才 RENAME，
--   所以線上的 route_stop 不會被動到，重跑一次就好。
--
--   真正要小心的是「只跑 ALTER 沒跑匯出」：那時候 route_stop 有欄位但
--   全是空字串，查詢介面會列出一堆沒有名字的路線，看起來像資料就長那樣。
--   所以兩件事要連著做，中間不要停下來去做別的。
-- =====================================================================

-- 既有 85,001 列會被填成空字串（NOT NULL 沒有預設值時 MySQL 的行為）。
-- 這是暫時的，下一步的全量重載會把它們換掉。
ALTER TABLE route_stop
  ADD COLUMN route_name VARCHAR(64) NOT NULL
    COMMENT '使用者認得的路線號碼，例如 287。sub_route_uid（TPE11881）是 TDX 內部代碼，沒有人會拿它查公車'
    AFTER city,
  ADD KEY idx_route_name (route_name);


-- ---------------------------------------------------------------------
-- 驗證（跑完匯出之後再看一次這段）
-- ---------------------------------------------------------------------

-- 1. 欄位在不在
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_COMMENT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'route_stop'
ORDER BY ORDINAL_POSITION;

-- 2. 還有沒有沒名字的（匯出完之後這裡必須是 0）
SELECT COUNT(*) AS 沒有名字的列數
FROM route_stop WHERE route_name = '';

-- 3. 一個號碼對到幾條子路線——這是預期會 > 1 的，
--    287 底下有主線、區間車、副線，各自去返程。
--    查詢介面靠起訖站名讓使用者分辨。
SELECT route_name,
       COUNT(DISTINCT sub_route_uid) AS 子路線數,
       COUNT(DISTINCT CONCAT(sub_route_uid, '-', direction)) AS 方向數
FROM route_stop
GROUP BY route_name
HAVING 子路線數 > 1
ORDER BY 子路線數 DESC
LIMIT 10;
