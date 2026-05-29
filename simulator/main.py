"""LiveClass 사용자 행동 이벤트를 생성해 Kafka로 전송한다."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
import json
import logging
import os
import random
import time
import uuid

from confluent_kafka import KafkaException, Producer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

Event = dict[str, Any]

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "10"))

# 실제 서비스처럼 보이도록 제한된 사용자/강의 풀에서 이벤트 대상을 샘플링한다.
USER_POOL = [f"user_{i:04d}" for i in range(1, 201)]
COURSE_POOL = [f"course_{i:02d}" for i in range(1, 21)]
LECTURE_POOL = {
    course: [f"{course}_lec_{j:02d}" for j in range(1, 6)]
    for course in COURSE_POOL
}

# page_view처럼 빈도가 높은 탐색 이벤트와 결제/에러처럼 낮은 이벤트를 함께 섞는다.
EVENT_TYPES = [
    "page_view",
    "lecture_start",
    "lecture_complete",
    "course_view",
    "lecture_drop",
    "search",
    "user_login",
    "checkout_start",
    "purchase_complete",
    "user_signup",
    "payment_failed",
    "video_load_error",
    "api_error",
]
EVENT_WEIGHTS = [35, 18, 12, 10, 8, 6, 4, 3, 2, 1, 0.5, 0.3, 0.2]

# 이벤트 성격별로 토픽을 나누면 향후 에러 알림 Consumer 등을 독립적으로 붙일 수 있다.
TOPIC_MAP = {
    "user_signup": "events.user",
    "user_login": "events.user",
    "page_view": "events.user",
    "course_view": "events.user",
    "search": "events.user",
    "lecture_start": "events.learning",
    "lecture_complete": "events.learning",
    "lecture_drop": "events.learning",
    "checkout_start": "events.commerce",
    "purchase_complete": "events.commerce",
    "payment_failed": "events.commerce",
    "video_load_error": "events.error",
    "api_error": "events.error",
}

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
MOBILE_PLATFORMS = ["ios", "android"]
OS_LIST = ["Windows", "macOS", "iOS", "Android", "Linux"]
BROWSER_LIST = ["Chrome", "Firefox", "Safari", "Edge"]
PAYMENT_METHODS = ["card", "kakao_pay", "naver_pay"]
EXIT_TRIGGERS = ["tab_close", "navigation", "idle_timeout"]
PAYMENT_ERROR_CODES = [
    "INSUFFICIENT_FUNDS",
    "CARD_EXPIRED",
    "NETWORK_ERROR",
    "INVALID_CARD",
    "LIMIT_EXCEEDED",
]
SEARCH_QUERIES = ["파이썬", "데이터분석", "엑셀", "재테크", "영어회화", "포토샵"]
PRICE_OPTIONS = [19900, 29900, 49900, 99900]
SESSION_MIN_EVENTS = 5
SESSION_MAX_EVENTS = 15


@dataclass
class SessionState:
    """로그인 유저의 여러 행동을 하나의 방문 흐름으로 묶기 위한 상태."""

    session_id: str
    event_count: int
    max_events: int


_user_sessions: dict[str, SessionState] = {}


def new_session() -> SessionState:
    return SessionState(
        session_id=str(uuid.uuid4()),
        event_count=0,
        max_events=random.randint(SESSION_MIN_EVENTS, SESSION_MAX_EVENTS),
    )


def get_session_id(user_id: str | None) -> str:
    """로그인 유저에게 일정 기간 유지되는 세션 ID를 반환한다."""
    if user_id is None:
        return str(uuid.uuid4())

    if user_id not in _user_sessions:
        _user_sessions[user_id] = new_session()

    session = _user_sessions[user_id]
    if session.event_count >= session.max_events:
        session = new_session()
        _user_sessions[user_id] = session

    session.event_count += 1
    return session.session_id


def get_simulated_hour() -> int:
    """저녁 피크가 가장 높도록 가중치를 둔 이벤트 발생 시각을 반환한다."""
    r = random.random()
    if r < 0.15:
        return random.randint(7, 9)
    if r < 0.40:
        return random.randint(10, 17)
    if r < 0.90:
        return random.randint(18, 23)
    return random.randint(0, 6)


def make_base_event(event_type: str) -> Event:
    """모든 이벤트에 공통으로 들어가는 베이스 필드를 만든다."""
    # 가입 이벤트는 익명 유저에게 발생할 수 없으므로 항상 로그인 유저로 생성한다.
    is_anonymous = False if event_type == "user_signup" else random.random() < 0.1
    user_id = None if is_anonymous else random.choice(USER_POOL)
    user_type = "anonymous" if is_anonymous else random.choice(["student", "instructor"])
    device = random.choice(DEVICE_TYPES)
    platform = "web" if device == "desktop" else random.choice(MOBILE_PLATFORMS)

    now = datetime.now(timezone.utc)
    event_time = now.replace(
        hour=get_simulated_hour(),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=0,
    )

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": event_time.isoformat(),
        "received_at": now.isoformat(),
        "session_id": get_session_id(user_id),
        "user_id": user_id,
        "user_type": user_type,
        "device_type": device,
        "platform": platform,
        "os": random.choice(OS_LIST),
        "browser": random.choice(BROWSER_LIST) if platform == "web" else None,
        "page_url": f"https://liveclass.io/{random.choice(['home', 'courses', 'lecture', 'dashboard'])}",
        "referrer_url": f"https://liveclass.io/{random.choice(['home', 'search', ''])}",
        # Wide Event Table 구조에 맞춰 이벤트별 전용 컬럼은 기본값을 None으로 둔다.
        "course_id": None,
        "course_title": None,
        "instructor_id": None,
        "lecture_id": None,
        "lecture_duration_sec": None,
        "watch_duration_sec": None,
        "watch_percentage": None,
        "total_watch_time_sec": None,
        "exit_trigger": None,
        "amount": None,
        "currency": None,
        "payment_method": None,
        "coupon_code": None,
        "discount_amount": None,
        "query": None,
        "result_count": None,
        "category_filter": None,
        "error_code": None,
        "error_message": None,
        "endpoint": None,
        "status_code": None,
    }


def enrich_event(event: Event) -> Event:
    """이벤트 타입별 고유 필드를 채운다."""
    event_type = event["event_type"]
    course = random.choice(COURSE_POOL)
    lecture = random.choice(LECTURE_POOL[course])

    if event_type in ("user_signup", "user_login", "page_view"):
        return event

    if event_type == "course_view":
        event["course_id"] = course
        event["course_title"] = f"강의 제목 {course}"
        event["instructor_id"] = f"instructor_{random.randint(1, 20):03d}"

    elif event_type == "search":
        event["query"] = random.choice(SEARCH_QUERIES)
        event["result_count"] = random.randint(5, 50)
        event["category_filter"] = random.choice(["IT", "비즈니스", "언어", None])

    elif event_type == "lecture_start":
        event["course_id"] = course
        event["lecture_id"] = lecture
        event["instructor_id"] = f"instructor_{random.randint(1, 20):03d}"
        event["lecture_duration_sec"] = random.randint(600, 5400)

    elif event_type == "lecture_complete":
        duration = random.randint(600, 5400)
        event["course_id"] = course
        event["lecture_id"] = lecture
        event["instructor_id"] = f"instructor_{random.randint(1, 20):03d}"
        event["lecture_duration_sec"] = duration
        event["watch_percentage"] = round(random.uniform(85.0, 100.0), 1)
        event["watch_duration_sec"] = int(duration * event["watch_percentage"] / 100)
        event["total_watch_time_sec"] = event["watch_duration_sec"]

    elif event_type == "lecture_drop":
        duration = random.randint(600, 5400)
        watched = random.randint(30, int(duration * 0.8))
        event["course_id"] = course
        event["lecture_id"] = lecture
        event["instructor_id"] = f"instructor_{random.randint(1, 20):03d}"
        event["lecture_duration_sec"] = duration
        event["watch_duration_sec"] = watched
        event["watch_percentage"] = round(watched / duration * 100, 1)
        event["exit_trigger"] = random.choice(EXIT_TRIGGERS)

    elif event_type == "checkout_start":
        event["course_id"] = course
        event["course_title"] = f"강의 제목 {course}"
        event["instructor_id"] = f"instructor_{random.randint(1, 20):03d}"
        event["amount"] = random.choice(PRICE_OPTIONS)
        event["currency"] = "KRW"
        event["payment_method"] = random.choice(PAYMENT_METHODS)

    elif event_type == "purchase_complete":
        original = random.choice(PRICE_OPTIONS)
        discount = random.choice([0, 0, 0, 5000, 10000])
        event["course_id"] = course
        event["course_title"] = f"강의 제목 {course}"
        event["instructor_id"] = f"instructor_{random.randint(1, 20):03d}"
        event["amount"] = original - discount
        event["currency"] = "KRW"
        event["payment_method"] = random.choice(PAYMENT_METHODS)
        event["coupon_code"] = f"COUP{random.randint(1000, 9999)}" if discount > 0 else None
        event["discount_amount"] = discount

    elif event_type == "payment_failed":
        event["course_id"] = course
        event["amount"] = random.choice(PRICE_OPTIONS)
        event["payment_method"] = random.choice(PAYMENT_METHODS)
        event["error_code"] = random.choice(PAYMENT_ERROR_CODES)
        event["error_message"] = f"결제 오류: {event['error_code']}"

    elif event_type == "video_load_error":
        error_code = random.choice(["VIDEO_404", "CDN_TIMEOUT", "FORMAT_UNSUPPORTED"])
        event["course_id"] = course
        event["lecture_id"] = lecture
        event["error_code"] = error_code
        event["error_message"] = f"영상 로드 실패: {error_code}"

    elif event_type == "api_error":
        error_code = random.choice(["INTERNAL_ERROR", "TIMEOUT", "SERVICE_UNAVAILABLE"])
        event["endpoint"] = random.choice([
            "/api/v1/courses",
            "/api/v1/lectures",
            "/api/v1/payments",
            "/api/v1/users",
        ])
        event["error_code"] = error_code
        event["error_message"] = f"API 오류: {error_code}"
        event["status_code"] = random.choice([500, 502, 503, 504])

    return event


def delivery_report(err, msg) -> None:
    """Kafka 전송 실패를 로그로 남긴다."""
    if err is not None:
        logger.error(f"Delivery failed for {msg.topic()}: {err}")


def create_producer(broker: str, max_retries: int = 10) -> Producer:
    """Kafka 브로커가 준비될 때까지 재시도하며 Producer를 만든다."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = Producer({"bootstrap.servers": broker})
            producer.list_topics(timeout=5)
            logger.info(f"Connected to Kafka: {broker}")
            return producer
        except KafkaException as exc:
            logger.warning(f"Kafka not ready ({attempt}/{max_retries}): {exc}")
            time.sleep(3)

    raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts")


def main() -> None:
    producer = create_producer(KAFKA_BROKER)
    logger.info(f"Starting: {EVENTS_PER_SECOND} events/sec")

    try:
        while True:
            tick_start = time.time()

            for _ in range(EVENTS_PER_SECOND):
                event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
                event = enrich_event(make_base_event(event_type))
                producer.produce(
                    TOPIC_MAP[event_type],
                    # 같은 유저의 이벤트가 같은 Kafka 파티션에 들어가도록 key를 지정한다.
                    key=(event.get("user_id") or event["session_id"]).encode(),
                    value=json.dumps(event, default=str).encode(),
                    on_delivery=delivery_report,
                )

            producer.flush(timeout=5)
            sleep_sec = max(0.0, 1.0 - (time.time() - tick_start))
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    except KeyboardInterrupt:
        logger.info("Stopped.")
    finally:
        producer.flush(timeout=10)


if __name__ == "__main__":
    main()
