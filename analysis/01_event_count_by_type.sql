-- ============================================================
-- 쿼리 ①: 이벤트 타입별 발생 건수
-- 목적: 전체 파이프라인 정상 동작 확인 및 이벤트 분포 파악
-- Grafana: Bar chart / Pie chart
-- ============================================================
SELECT
    event_type,
    count()                                                    AS event_count,
    round(count() * 100.0 / sum(count()) OVER (), 2)          AS percentage
FROM events FINAL
GROUP BY event_type
ORDER BY event_count DESC;
