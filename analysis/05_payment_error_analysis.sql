-- ============================================================
-- 쿼리 ⑤: 결제 실패 에러 코드별 집계
-- 목적: 결제 장애 원인 분석 → 우선 해결 에러 코드 파악
--       실시간 처리 가치 증명: 에러 코드별 건수로 즉각 대응 우선순위 결정
-- Grafana: Pie chart / Bar chart
-- ============================================================
SELECT
    error_code,
    count()                                                   AS fail_count,
    round(count() * 100.0 / sum(count()) OVER (), 2)          AS percentage
FROM events FINAL
WHERE event_type = 'payment_failed'
GROUP BY error_code
ORDER BY fail_count DESC;
