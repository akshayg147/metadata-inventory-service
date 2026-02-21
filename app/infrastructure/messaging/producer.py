"""
Kafka message producer — publish URLs for background collection.

Uses confluent-kafka's Producer with async-friendly delivery callbacks.
The Producer is initialized once during application startup and reused.
"""

import json
from typing import Optional

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Module-level producer reference
_producer: Optional[Producer] = None


def ensure_topics() -> None:
    """
    Create Kafka topics if they don't already exist.

    Uses the AdminClient to explicitly create the main task topic
    and the dead letter queue topic. Idempotent — silently skips
    topics that already exist.
    """
    admin = AdminClient({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
    })

    topics_to_create = [
        NewTopic(settings.kafka_topic, num_partitions=3, replication_factor=1),
        NewTopic(settings.kafka_dlq_topic, num_partitions=1, replication_factor=1),
    ]

    futures = admin.create_topics(topics_to_create, operation_timeout=10)

    for topic, future in futures.items():
        try:
            future.result()  # Block until topic is created
            logger.info("Created Kafka topic: %s", topic)
        except Exception as exc:
            # TOPIC_ALREADY_EXISTS is fine — anything else is a real error
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                logger.debug("Kafka topic already exists: %s", topic)
            else:
                logger.error("Failed to create Kafka topic %s: %s", topic, exc)
                raise


def init_producer() -> None:
    """
    Initialize the Kafka producer.

    Called once during application startup (lifespan).
    """
    global _producer
    _producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "metadata-api-producer",
        "acks": "all",                    # Wait for all replicas
        "retries": 3,                     # Retry transient failures
        "retry.backoff.ms": 200,
        "linger.ms": 10,                  # Micro-batch for throughput
        "compression.type": "snappy",     # Compress messages
    })
    logger.info(
        "Kafka producer initialized (bootstrap=%s)",
        settings.kafka_bootstrap_servers,
    )


def close_producer() -> None:
    """
    Flush and close the Kafka producer.

    Called during application shutdown. Flushes any buffered
    messages with a timeout to avoid hanging on shutdown.
    """
    global _producer
    if _producer is not None:
        remaining = _producer.flush(timeout=10)
        if remaining > 0:
            logger.warning(
                "Kafka producer shutdown with %d unflushed messages", remaining
            )
        _producer = None
        logger.info("Kafka producer closed")


def _delivery_callback(err, msg) -> None:
    """
    Callback invoked once per message to indicate delivery result.

    Args:
        err: KafkaError if delivery failed, else None.
        msg: The Message object that was delivered (or failed).
    """
    if err is not None:
        logger.error(
            "Kafka delivery failed for topic=%s: %s",
            msg.topic(),
            err,
        )
    else:
        logger.debug(
            "Kafka message delivered to topic=%s partition=%d offset=%d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


async def enqueue(url: str) -> None:
    """
    Publish a URL to the Kafka topic for background collection.

    The confluent-kafka Producer.produce() is non-blocking — it
    buffers the message internally and sends it asynchronously.
    poll(0) triggers delivery callbacks without blocking.

    Args:
        url: The normalised URL to collect metadata for.

    Raises:
        RuntimeError: If the producer is not initialized.
        BufferError: If the producer's internal buffer is full.
    """
    if _producer is None:
        raise RuntimeError(
            "Kafka producer is not initialized. "
            "Ensure init_producer() was called during startup."
        )

    message = json.dumps({"url": url})

    try:
        _producer.produce(
            topic=settings.kafka_topic,
            value=message.encode("utf-8"),
            callback=_delivery_callback,
        )
        # Trigger any pending delivery callbacks (non-blocking)
        _producer.poll(0)
        logger.info("Enqueued url=%s to Kafka topic=%s", url, settings.kafka_topic)

    except BufferError:
        logger.warning(
            "Kafka producer buffer full. Dropping url=%s — "
            "consider scaling or increasing buffer size.",
            url,
        )
        raise


async def publish_with_retry(url: str, retry_count: int) -> None:
    """
    Re-publish a URL back to the main topic with an incremented retry count.

    Used by the consumer when process_url fails but retries remain.

    Args:
        url: The URL that failed processing.
        retry_count: Current retry attempt number.
    """
    if _producer is None:
        raise RuntimeError("Kafka producer is not initialized.")

    message = json.dumps({"url": url, "retry_count": retry_count})

    _producer.produce(
        topic=settings.kafka_topic,
        value=message.encode("utf-8"),
        callback=_delivery_callback,
    )
    _producer.poll(0)
    logger.info(
        "Re-enqueued url=%s to topic=%s (retry_count=%d)",
        url, settings.kafka_topic, retry_count,
    )


async def publish_to_dlq(url: str, retry_count: int, error: str) -> None:
    """
    Publish a permanently failed message to the dead letter topic.

    Args:
        url: The URL that exhausted all retries.
        retry_count: Final retry count.
        error: The error message from the last failure.
    """
    if _producer is None:
        raise RuntimeError("Kafka producer is not initialized.")

    message = json.dumps({
        "url": url,
        "retry_count": retry_count,
        "error": error,
    })

    _producer.produce(
        topic=settings.kafka_dlq_topic,
        value=message.encode("utf-8"),
        callback=_delivery_callback,
    )
    _producer.poll(0)
    logger.warning(
        "Sent url=%s to DLQ topic=%s after %d retries (error=%s)",
        url, settings.kafka_dlq_topic, retry_count, error,
    )
