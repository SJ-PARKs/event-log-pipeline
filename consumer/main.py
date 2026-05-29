"""Kafka 이벤트를 소비해 ClickHouse에 배치로 적재한다."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import json
import logging
import os
import time
import uuid

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError, Producer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

Event = dict[str, Any]

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

TOPICS = ["events.user", "events.learning", "events.commerce", "events.error"]
DLQ_TOPIC = "events.dead_letter"

# ClickHouse는 작은 insert가 너무 자주 발생하면 part가 많아지므로 배치 단위로 적재한다.
BATCH_SIZE = 500
FLUSH_INTERVAL_SEC = 5.0
MAX_INSERT_RETRIES = 3

# clickhouse/init.sql의 컬럼 순서와 반드시 같아야 한다.
COLUMNS = [
    "event_id",
    "event_type",
    "event_timestamp",
    "received_at",
    "session_id",
    "user_id",
    "user_type",
    "device_type",
    "platform",
    "os",
    "browser",
    "page_url",
    "referrer_url",
    "course_id",
    "course_title",
    "instructor_id",
    "lecture_id",
    "lecture_duration_sec",
    "watch_duration_sec",
    "watch_percentage",
    "total_watch_time_sec",
    "exit_trigger",
    "amount",
    "currency",
    "payment_method",
    "coupon_code",
    "discount_amount",
    "query",
    "result_count",
    "category_filter",
    "error_code",
    "error_message",
    "endpoint",
    "status_code",
]


def parse_datetime(value: Any) -> datetime | None:
    """ISO 타임스탬프를 ClickHouse insert용 UTC naive datetime으로 변환한다."""
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    if dt.tzinfo is None:
        return dt

    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def parse_uuid(value: Any) -> uuid.UUID | None:
    """event_id를 UUID로 변환하고, 잘못된 값이면 None을 반환한다."""
    if value is None:
        return None

    try:
        return uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return None


def event_to_row(event: Event) -> tuple[Any, ...]:
    """이벤트 dict를 ClickHouse insert row tuple로 변환한다."""
    row = []

    for column in COLUMNS:
        value = event.get(column)

        if column in ("event_timestamp", "received_at"):
            value = parse_datetime(value)
        elif column == "event_id":
            value = parse_uuid(value)

        row.append(value)

    return tuple(row)


def insert_batch(client: clickhouse_connect.driver.Client, batch: list[Event]) -> bool:
    """이벤트 배치를 ClickHouse에 적재하고 제한된 횟수만 재시도한다."""
    rows = [event_to_row(event) for event in batch]

    for attempt in range(1, MAX_INSERT_RETRIES + 1):
        try:
            client.insert("events", rows, column_names=COLUMNS)
            return True
        except Exception as exc:
            logger.warning(
                f"Insert attempt {attempt}/{MAX_INSERT_RETRIES} failed: {exc}"
            )
            if attempt < MAX_INSERT_RETRIES:
                time.sleep(1)

    return False


def send_to_dlq(producer: Producer, events: list[Event]) -> None:
    """적재 실패 이벤트를 추후 재처리할 수 있도록 DLQ 토픽에 보낸다."""
    for event in events:
        producer.produce(
            DLQ_TOPIC,
            value=json.dumps(event, default=str).encode(),
        )

    producer.flush()
    logger.warning(f"Sent {len(events)} events to DLQ ({DLQ_TOPIC})")


def wait_for_clickhouse(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    max_retries: int = 20,
) -> clickhouse_connect.driver.Client:
    """ClickHouse가 쿼리를 받을 수 있을 때까지 대기하고 client를 반환한다."""
    for attempt in range(1, max_retries + 1):
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
            )
            client.query("SELECT 1")
            logger.info("Connected to ClickHouse")
            return client
        except Exception as exc:
            logger.warning(f"ClickHouse not ready ({attempt}/{max_retries}): {exc}")
            time.sleep(3)

    raise RuntimeError("Could not connect to ClickHouse")


def create_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "liveclass-consumer-group",
        "auto.offset.reset": "earliest",
        # ClickHouse 적재가 끝난 뒤 offset을 commit해야 이벤트 유실을 줄일 수 있다.
        "enable.auto.commit": False,
    })
    consumer.subscribe(TOPICS)
    logger.info(f"Subscribed to: {TOPICS}")
    return consumer


def should_flush(buffer: list[Event], last_flush: float) -> bool:
    if len(buffer) >= BATCH_SIZE:
        return True

    return bool(buffer) and time.time() - last_flush >= FLUSH_INTERVAL_SEC


def main() -> None:
    clickhouse_client = wait_for_clickhouse(
        CLICKHOUSE_HOST,
        CLICKHOUSE_PORT,
        CLICKHOUSE_USER,
        CLICKHOUSE_PASSWORD,
        CLICKHOUSE_DB,
    )
    consumer = create_consumer()
    dlq_producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    buffer: list[Event] = []
    last_flush = time.time()
    total_inserted = 0

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                pass
            elif message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {message.error()}")
            else:
                try:
                    buffer.append(json.loads(message.value().decode("utf-8")))
                except Exception as exc:
                    logger.error(f"Failed to parse message: {exc}")

            if not should_flush(buffer, last_flush):
                continue

            count = len(buffer)
            if insert_batch(clickhouse_client, buffer):
                total_inserted += count
                # 저장 성공 후에만 offset을 확정한다.
                consumer.commit(asynchronous=False)
                logger.info(f"Inserted {count} events (total: {total_inserted})")
                buffer = []
                last_flush = time.time()
                continue

            try:
                # 저장에 계속 실패한 배치는 DLQ에 남겨 재처리 가능성을 확보한다.
                send_to_dlq(dlq_producer, buffer)
                consumer.commit(asynchronous=False)
                buffer = []
                last_flush = time.time()
            except Exception as exc:
                logger.error(f"DLQ send failed: {exc}, retaining buffer")

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        if buffer:
            logger.info(f"Flushing remaining {len(buffer)} events before exit...")
            if insert_batch(clickhouse_client, buffer):
                consumer.commit(asynchronous=False)
                logger.info("Final flush successful.")
            else:
                try:
                    send_to_dlq(dlq_producer, buffer)
                    consumer.commit(asynchronous=False)
                except Exception as exc:
                    logger.error(f"Final DLQ send failed: {exc}")
    finally:
        dlq_producer.flush(timeout=10)
        consumer.close()


if __name__ == "__main__":
    main()
