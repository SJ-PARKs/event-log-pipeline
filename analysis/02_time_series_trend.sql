-- ============================================================
-- 쿼리 ②-b: 실시간 시계열 트렌드
-- 목적: 시간 흐름에 따른 이벤트 변화 모니터링, 이상 징후 감지
-- Grafana: Time series (실시간 자동 갱신)
-- ============================================================
SELECT
    toStartOfHour(event_timestamp)  AS hour,
    event_type,
    count()                         AS event_count
FROM events FINAL
GROUP BY
    hour,
    event_type
ORDER BY hour;
