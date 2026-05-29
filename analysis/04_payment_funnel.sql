-- ============================================================
-- 쿼리 ④: 결제 퍼널 전환율 + 실패율
-- 목적: checkout_start 대비 purchase_complete 비율로 결제 UX 개선 근거 도출
--       전환율과 실패율을 함께 보면 "왜 전환이 안 되는지" 원인 파악 가능
-- Grafana: Stat panel (숫자 강조) + Bar chart
-- ============================================================
SELECT
    countIf(event_type = 'checkout_start')    AS checkout_start,
    countIf(event_type = 'purchase_complete') AS purchase_complete,
    countIf(event_type = 'payment_failed')    AS payment_failed,
    round(
        countIf(event_type = 'purchase_complete') * 100.0
        / NULLIF(countIf(event_type = 'checkout_start'), 0),
        2
    )                                         AS conversion_rate,
    round(
        countIf(event_type = 'payment_failed') * 100.0
        / NULLIF(countIf(event_type = 'checkout_start'), 0),
        2
    )                                         AS failure_rate
FROM events FINAL;
