SELECT
  COUNT(s.p50_sec)                                   AS `有基準路段數`,
  SUM(s.p50_sec)                                     AS `中位合計秒`,
  ROUND(SUM(s.mean_sec))                             AS `平均合計秒`,
  ROUND(SUM(s.mean_sec) + 1.28 * SQRT(SUM(POW(s.sd_sec, 2)))) AS `估p90秒`,
  SUM(s.level <> 'band')                             AS `降級路段數`,
  SUM(NOT s.p90_reliable)                            AS `p90不可信路段數`
FROM route_stop a
JOIN route_stop b
  ON b.sub_route_uid = a.sub_route_uid AND b.direction = a.direction
 AND b.seq = a.seq + 1
LEFT JOIN baseline_segment s
  ON  s.city = a.city
  AND s.from_station_id = a.station_id AND s.to_station_id = b.station_id
  AND s.day_type = 'weekday' AND s.band = 'am_peak'
WHERE a.sub_route_uid = 'TPE11881' AND a.direction = 0
  AND a.seq BETWEEN 4 AND 11;