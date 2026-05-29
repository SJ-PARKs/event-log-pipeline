-- ============================================================
-- 쿼리 ③: 강의별 완강률 vs 이탈률
-- 목적: 콘텐츠 품질 핵심 KPI. 완강률 낮은 강의 식별 → 강사 피드백
-- Grafana: Bar chart / Table
-- ============================================================
SELECT
    course_id,
    countIf(event_type = 'lecture_start')    AS start_count,
    countIf(event_type = 'lecture_complete') AS complete_count,
    countIf(event_type = 'lecture_drop')     AS drop_count,
    round(
        countIf(event_type = 'lecture_complete') * 100.0
        / NULLIF(countIf(event_type = 'lecture_start'), 0),
        1
    )                                        AS completion_rate,
    round(
        avg(CASE
            WHEN event_type IN ('lecture_complete', 'lecture_drop')
            THEN watch_percentage
        END),
        1
    )                                        AS avg_watch_percentage
FROM events FINAL
WHERE event_type IN ('lecture_start', 'lecture_complete', 'lecture_drop')
GROUP BY course_id
ORDER BY completion_rate ASC;  -- 완강률 낮은 강의 우선 노출
