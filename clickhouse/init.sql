-- clickhouse/init.sql
-- ClickHouse 컨테이너 첫 기동 시 자동 실행

CREATE TABLE IF NOT EXISTS default.events
(
    -- 공통 베이스 필드
    event_id             UUID,
    event_type           LowCardinality(String),
    event_timestamp      DateTime64(3, 'UTC'),
    received_at          DateTime64(3, 'UTC'),
    session_id           String,
    user_id              Nullable(String),
    user_type            LowCardinality(String),
    device_type          LowCardinality(String),
    platform             LowCardinality(String),
    os                   LowCardinality(String),
    browser              Nullable(String),
    page_url             String,
    referrer_url         String,

    -- 강의/콘텐츠 관련
    course_id            Nullable(String),
    course_title         Nullable(String),
    instructor_id        Nullable(String),
    lecture_id           Nullable(String),
    lecture_duration_sec Nullable(Int32),
    watch_duration_sec   Nullable(Int32),
    watch_percentage     Nullable(Float32),
    total_watch_time_sec Nullable(Int32),
    exit_trigger         Nullable(String),

    -- 결제 관련
    amount               Nullable(Int32),
    currency             Nullable(String),
    payment_method       Nullable(String),
    coupon_code          Nullable(String),
    discount_amount      Nullable(Int32),

    -- 검색 관련
    query                Nullable(String),
    result_count         Nullable(Int32),
    category_filter      Nullable(String),

    -- 에러 관련
    error_code           Nullable(String),
    error_message        Nullable(String),
    endpoint             Nullable(String),
    status_code          Nullable(Int32)
)
ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_type, toDate(event_timestamp), event_id);
