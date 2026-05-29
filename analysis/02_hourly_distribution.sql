-- ============================================================
-- 쿼리 ②-a: 시간대별 이벤트 분포 (0~23시)
-- 목적: 서비스 피크 타임 파악 → 인프라 스케일링 기준 수립
-- Grafana: Bar chart
-- ============================================================
SELECT
    toHour(event_timestamp)     AS hour_of_day,
    count()                     AS event_count
FROM events FINAL
GROUP BY hour_of_day
ORDER BY hour_of_day;
