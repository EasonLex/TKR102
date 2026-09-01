-- =====================================================================
-- 服務層 schema（MySQL 8.4）
--
-- 執行：
--   docker exec -i mysql mysql -u root -p bus < sql/schema.sql
--
-- 這份是可重跑的：每個 CREATE 都帶 IF NOT EXISTS。
-- 刻意不放進 /docker-entrypoint-initdb.d ——那裡只在資料目錄為空時
-- 執行一次，之後改了不會重跑也不會報錯。能重跑的步驟才敢自動化。
-- =====================================================================

-- ---------------------------------------------------------------------
-- 路段行車時間基準
--
-- 主鍵就是查詢鍵：InnoDB 的叢集索引讓「某站到某站、某日型、某時段」
-- 的點查是一次索引尋訪。這是這裡用 MySQL 而不是繼續查 BigQuery 的理由。
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS baseline_segment (
  city              VARCHAR(16)  NOT NULL,
  from_station_id   VARCHAR(32)  NOT NULL,
  to_station_id     VARCHAR(32)  NOT NULL,
  day_type          ENUM('weekday','weekend') NOT NULL,
  band              VARCHAR(16)  NOT NULL
                    COMMENT 'am_peak / midday / pm_peak / evening / night；刻意用 VARCHAR 不用 ENUM，時段定義之後可能改',

  level             ENUM('band','daytype','pair') NOT NULL
                    COMMENT '這一列的統計量實際來自哪一層。降級是常態不是例外，查詢端必須知道手上這筆有多粗',
  n                 INT UNSIGNED NOT NULL
                    COMMENT '支撐本列統計量的樣本數，對應 level 那一層。★不可跨層加總★——不同 level 的 n 意義不同',
  n_route           SMALLINT UNSIGNED NOT NULL
                    COMMENT '這段路被幾條子路線共用',

  p25_sec           SMALLINT UNSIGNED NOT NULL,
  p50_sec           SMALLINT UNSIGNED NOT NULL,
  p75_sec           SMALLINT UNSIGNED NOT NULL,
  p90_sec           SMALLINT UNSIGNED NOT NULL,
  unreliability     DECIMAL(5,2) NOT NULL
                    COMMENT 'p90 / p50。★只在 level=band 時乾淨★——降級的列把不同時段混在一桶，p90 會被時段差異撐大',
  p90_reliable      TINYINT(1)   NOT NULL
                    COMMENT 'n >= 100。估中位數 n>=30 就穩，估 p90 要讓尾巴有十來個點',

  mean_sec          DECIMAL(7,2) NOT NULL
                    COMMENT '多路段合成用：分位數不可加，平均與變異數可加',
  sd_sec            DECIMAL(7,2) NOT NULL
                    COMMENT '合成 A→B 時用 Σmean 與 √Σsd²，再以 mean + 1.28·sd 估 p90（CLT 在「和」上成立）',

  date_from         DATE NOT NULL,
  date_to           DATE NOT NULL,
  n_days            SMALLINT UNSIGNED NOT NULL,
  rule_version      VARCHAR(8)  NOT NULL COMMENT '產生來源事件的判定規則版本',
  baseline_version  VARCHAR(16) NOT NULL,
  computed_at       DATETIME    NOT NULL COMMENT 'UTC。伺服器 time_zone 固定 +00:00，轉換一律在應用層做',

  PRIMARY KEY (city, from_station_id, to_station_id, day_type, band)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  ROW_FORMAT=DYNAMIC
  COMMENT='路段行車時間基準。一列 = 一個站牌對 × 日型 × 時段';


-- ---------------------------------------------------------------------
-- 路線站序（從 MongoDB 匯出）
--
-- 沒有這張表，服務層要一邊查 MongoDB 拿站序、一邊查 MySQL 拿基準，
-- 然後在應用層 join——跨庫 join 在應用層做是最容易出錯的地方。
-- 有了它，「路線 X 從 A 站到 B 站、平日早尖峰」是一句 SQL。
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS route_stop (
  city           VARCHAR(16)  NOT NULL,
  route_name     VARCHAR(64)  NOT NULL
                 COMMENT '使用者認得的路線號碼，例如 287。sub_route_uid（TPE11881）是 TDX 內部代碼，沒有人會拿它查公車',
  sub_route_uid  VARCHAR(32)  NOT NULL,
  direction      TINYINT UNSIGNED NOT NULL,
  seq            SMALLINT UNSIGNED NOT NULL,
  station_id     VARCHAR(32)  NOT NULL,
  stop_name      VARCHAR(64)  NOT NULL,
  boarding       TINYINT      NULL COMMENT 'TDX 的上下車限制欄位',
  version_id     VARCHAR(64)  NOT NULL COMMENT '對應 MongoDB 該筆站序的版本，出問題時可追回來源',
  loaded_at      DATETIME     NOT NULL,

  PRIMARY KEY (sub_route_uid, direction, seq),
  KEY idx_station (station_id),
  KEY idx_city_route (city, sub_route_uid),
  KEY idx_route_name (route_name)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  ROW_FORMAT=DYNAMIC
  COMMENT='現行版本的路線站序。一列 = 某子路線某方向的第 n 站';


-- ---------------------------------------------------------------------
-- 驗證（建完跑一次）
-- ---------------------------------------------------------------------
SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, TABLE_COMMENT
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
ORDER BY TABLE_NAME;
