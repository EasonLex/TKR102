-- =====================================================================
-- 歷史基準建置 —— 步驟 1～3（BigQuery）
--
-- 1  bus.segment_steps      步階事實表（視窗運算只做一次）
-- 2  一次 ROLLUP 產生 L1/L2/L3 三層
-- 3  bus.baseline_segment   階梯展開成最終表
--
-- 輸入  bus.events_v2   22 天 × 2 城 = 44 分區，判定規則 v2
-- 輸出  bus.baseline_segment  約 14 萬列，之後匯出到 MySQL
-- =====================================================================


-- ---------------------------------------------------------------------
-- 步驟 1：步階事實表
--
-- 為什麼要物化而不是用 VIEW：
--   LAG() OVER (PARTITION BY trip_id) 是全表掃描 + 排序，
--   而下面三層聚合都要用它。做成 VIEW 等於算三次。
--
-- 為什麼用 service_date 而不是外部表的 dt：
--   dt 是 hive 分區欄，型別由 AUTO 推導（可能是 STRING），
--   不能直接拿來 PARTITION BY。從 arrival_from 導出的台北日期
--   語意也更正確——這一步屬於「起站到站的那一天」。
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE bus.segment_steps
PARTITION BY service_date
CLUSTER BY city, from_station_id, to_station_id
AS
WITH stepped AS (
  SELECT
    city,
    sub_route_uid,
    direction,
    LAG(station_id)   OVER w AS from_station_id,
    station_id               AS to_station_id,
    seq - LAG(seq)    OVER w AS dseq,
    LAG(arrival_time) OVER w AS arrival_from,
    LAG(min_speed)    OVER w AS from_min_speed,
    TIMESTAMP_DIFF(arrival_time, LAG(arrival_time) OVER w, SECOND) AS sec
  FROM `bus.events_v2`
  WINDOW w AS (PARTITION BY trip_id ORDER BY arrival_time)
),
tagged AS (
  SELECT
    *,
    DATE(arrival_from, 'Asia/Taipei')                        AS service_date,
    EXTRACT(HOUR FROM arrival_from AT TIME ZONE 'Asia/Taipei') AS hr,
    FORMAT_TIMESTAMP('%u', arrival_from, 'Asia/Taipei')       AS dow
  FROM stepped
  -- dseq = 1：路段的單位是「相鄰兩站」。跨站的步是漏抓造成的，
  --           混進來會把分佈整個拉長。
  -- sec 上下界：1 秒以下不可能，30 分鐘以上是趟次判定的殘留。
  WHERE dseq = 1 AND sec BETWEEN 1 AND 1800
)
SELECT
  city,
  service_date,
  sub_route_uid,
  direction,
  from_station_id,
  to_station_id,
  arrival_from,
  sec,
  from_min_speed,
  hr,
  IF(dow IN ('6', '7'), 'weekend', 'weekday') AS day_type,
  CASE
    WHEN hr BETWEEN  6 AND  9 THEN 'am_peak'
    WHEN hr BETWEEN 10 AND 15 THEN 'midday'
    WHEN hr BETWEEN 16 AND 19 THEN 'pm_peak'
    WHEN hr BETWEEN 20 AND 22 THEN 'evening'
    ELSE                           'night'
  END AS band
FROM tagged;


-- ---------------------------------------------------------------------
-- 步驟 2 + 3：一次 ROLLUP 出三層，再展開成最終表
--
-- ROLLUP(city, from, to, day_type, band) 會產生六種分組粒度，
-- 前三種正好就是 L1 / L2 / L3：
--     (city, from, to, day_type, band)   L1  最細
--     (city, from, to, day_type)         L2  併時段
--     (city, from, to)                   L3  併平日假日
--     (city, from) / (city) / ()         用不到，下面濾掉
--
-- 用 ROLLUP 而不是寫三段 GROUP BY：統計量清單只出現一次。
-- 抄三遍的話，哪天加一個分位數就有三個地方要改，
-- 而改漏一個不會報錯，只會讓某一層的欄位悄悄變成 NULL。
--
-- 被 ROLLUP 捲掉的欄位會是 NULL，這裡不會跟真實 NULL 混淆：
-- dseq = 1 保證 from_station_id 非空，day_type / band 由
-- arrival_from 導出也必定非空。
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE bus.baseline_segment AS
WITH agg AS (
  SELECT
    city, from_station_id, to_station_id, day_type, band,
    COUNT(*)                              AS n,
    COUNT(DISTINCT sub_route_uid)         AS n_route,
    APPROX_QUANTILES(sec, 100)[OFFSET(25)] AS p25_sec,
    APPROX_QUANTILES(sec, 100)[OFFSET(50)] AS p50_sec,
    APPROX_QUANTILES(sec, 100)[OFFSET(75)] AS p75_sec,
    APPROX_QUANTILES(sec, 100)[OFFSET(90)] AS p90_sec,
    AVG(sec)                              AS mean_sec,
    STDDEV_SAMP(sec)                      AS sd_sec,
    MIN(service_date)                     AS date_from,
    MAX(service_date)                     AS date_to,
    COUNT(DISTINCT service_date)          AS n_days
  FROM bus.segment_steps
  GROUP BY ROLLUP(city, from_station_id, to_station_id, day_type, band)
),
l1 AS (SELECT * FROM agg WHERE band IS NOT NULL),
l2 AS (SELECT * FROM agg WHERE band IS NULL AND day_type IS NOT NULL),
l3 AS (SELECT * FROM agg WHERE day_type IS NULL AND to_station_id IS NOT NULL),

-- 階梯：取「樣本數撐得住的最細一層」。
-- 門檻 30 是給中位數用的（中位數對樣本數不敏感）。
-- p90 需要的樣本量高得多，因此不另設一層，改成把 n 一起輸出，
-- 由 p90_reliable 標記，讓服務層決定要不要呈現 p90。
picked AS (
  SELECT
    l1.city, l1.from_station_id, l1.to_station_id, l1.day_type, l1.band,
    CASE WHEN l1.n >= 30 THEN 'band'
         WHEN l2.n >= 30 THEN 'daytype'
         ELSE                  'pair'
    END AS level,
    CASE WHEN l1.n >= 30 THEN l1.n       WHEN l2.n >= 30 THEN l2.n       ELSE l3.n       END AS n,
    CASE WHEN l1.n >= 30 THEN l1.n_route WHEN l2.n >= 30 THEN l2.n_route ELSE l3.n_route END AS n_route,
    CASE WHEN l1.n >= 30 THEN l1.p25_sec WHEN l2.n >= 30 THEN l2.p25_sec ELSE l3.p25_sec END AS p25_sec,
    CASE WHEN l1.n >= 30 THEN l1.p50_sec WHEN l2.n >= 30 THEN l2.p50_sec ELSE l3.p50_sec END AS p50_sec,
    CASE WHEN l1.n >= 30 THEN l1.p75_sec WHEN l2.n >= 30 THEN l2.p75_sec ELSE l3.p75_sec END AS p75_sec,
    CASE WHEN l1.n >= 30 THEN l1.p90_sec WHEN l2.n >= 30 THEN l2.p90_sec ELSE l3.p90_sec END AS p90_sec,
    CASE WHEN l1.n >= 30 THEN l1.mean_sec WHEN l2.n >= 30 THEN l2.mean_sec ELSE l3.mean_sec END AS mean_sec,
    CASE WHEN l1.n >= 30 THEN l1.sd_sec  WHEN l2.n >= 30 THEN l2.sd_sec  ELSE l3.sd_sec  END AS sd_sec,
    CASE WHEN l1.n >= 30 THEN l1.date_from WHEN l2.n >= 30 THEN l2.date_from ELSE l3.date_from END AS date_from,
    CASE WHEN l1.n >= 30 THEN l1.date_to   WHEN l2.n >= 30 THEN l2.date_to   ELSE l3.date_to   END AS date_to,
    CASE WHEN l1.n >= 30 THEN l1.n_days    WHEN l2.n >= 30 THEN l2.n_days    ELSE l3.n_days    END AS n_days
  FROM l1
  LEFT JOIN l2
    ON  l1.city            = l2.city
    AND l1.from_station_id = l2.from_station_id
    AND l1.to_station_id   = l2.to_station_id
    AND l1.day_type        = l2.day_type
  LEFT JOIN l3
    ON  l1.city            = l3.city
    AND l1.from_station_id = l3.from_station_id
    AND l1.to_station_id   = l3.to_station_id
)
SELECT
  city, from_station_id, to_station_id, day_type, band,
  level, n, n_route,
  p25_sec, p50_sec, p75_sec, p90_sec,
  ROUND(SAFE_DIVIDE(p90_sec, p50_sec), 2) AS unreliability,
  n >= 100                                AS p90_reliable,
  ROUND(mean_sec, 2)                      AS mean_sec,
  ROUND(IFNULL(sd_sec, 0), 2)             AS sd_sec,   -- n=1 時 STDDEV_SAMP 為 NULL
  date_from, date_to, n_days,
  'v2'                                    AS rule_version,
  'b1'                                    AS baseline_version,
  CURRENT_TIMESTAMP()                     AS computed_at
FROM picked;