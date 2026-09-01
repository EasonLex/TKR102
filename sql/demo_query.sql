-- 路線 TPE11881 方向 0，第 4 站到第 12 站，平日早尖峰
SELECT
  a.seq,
  a.stop_name  AS `起站`,
  b.stop_name  AS `迄站`,
  s.p50_sec    AS `中位秒`,
  s.p90_sec    AS `p90秒`,
  s.unreliability AS `不可靠指數`,
  s.n, s.level, s.p90_reliable
FROM route_stop a
JOIN route_stop b
  ON  b.sub_route_uid = a.sub_route_uid
  AND b.direction     = a.direction
  AND b.seq           = a.seq + 1
LEFT JOIN baseline_segment s
  ON  s.city            = a.city
  AND s.from_station_id = a.station_id
  AND s.to_station_id   = b.station_id
  AND s.day_type        = 'weekday'
  AND s.band            = 'am_peak'
WHERE a.sub_route_uid = 'TPE11881' AND a.direction = 0
  AND a.seq BETWEEN 4 AND 11
ORDER BY a.seq;