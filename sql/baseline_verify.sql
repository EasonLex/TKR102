-- =====================================================================
-- 驗證 —— 匯出到 MySQL 之前先跑這三個
-- =====================================================================

-- 1) 階梯分佈：band 應該佔絕大多數，pair 應該很少
SELECT day_type, level, COUNT(*) AS `格子數`,
       ROUND(100 * SUM(n) / SUM(SUM(n)) OVER (PARTITION BY day_type), 1) AS `走行佔比`
FROM bus.baseline_segment
GROUP BY day_type, level
ORDER BY day_type, level;

-- 2) 主鍵唯一性。應該回 0 列——不回 0 就代表 ROLLUP 的 NULL
--    跟真實 NULL 撞在一起了，那會讓某些格子被靜默覆蓋
SELECT city, from_station_id, to_station_id, day_type, band, COUNT(*) AS c
FROM bus.baseline_segment
GROUP BY 1,2,3,4,5 HAVING c > 1;

-- 3) 合理性：中位數該落在 60–80 秒，不可靠指數該在 2 上下。
--    這兩個數字先前量過，若對不上就是這段 SQL 有問題，不是資料變了
SELECT day_type, level,
       APPROX_QUANTILES(p50_sec, 2)[OFFSET(1)]       AS `p50中位`,
       APPROX_QUANTILES(unreliability, 2)[OFFSET(1)] AS `不可靠指數中位`,
       ROUND(100 * COUNTIF(p90_reliable) / COUNT(*), 1) AS `p90可信佔比`
FROM bus.baseline_segment
GROUP BY day_type, level ORDER BY day_type, level;