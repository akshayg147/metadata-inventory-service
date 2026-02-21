"""
Kafka message consumer — subscribe to URL collection tasks.

Runs as a long-lived asyncio task started during application lifespan.
Uses confluent-kafka's Consumer with run_in_executor to avoid blocking
the asyncio event loop.
"""

import asyncio
import json
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

from app.core.config import settings
from app.core.exceptions import PermanentCollectionError, TransientCollectionError
from app.core.logging import get_logger

logger = get_logger(__name__)

# Reference to the consumer task for graceful shutdown
_consumer_task: Optional[asyncio.Task] = None
_shutdown_event: Optional[asyncio.Event] = None


async def _consume_loop() -> None:
    """
    Main consumer loop — polls Kafka for messages and dispatches them.

    Uses run_in_executor for the blocking consumer.poll() call so the
    asyncio event loop stays responsive. The poll timeout (1.0s) ensures
    we check the shutdown event regularly.
    """
    from app.worker.worker import process_url
    from app.infrastructure.messaging.producer import publish_with_retry, publish_to_dlq
    from app.infrastructure.db.repository import MetadataRepository

    repo = MetadataRepository()

    async def mark_failed_in_db(url: str, reason: str) -> None:
        """Mark a URL as permanently failed in MongoDB."""
        try:
            await repo.mark_failed(url, reason)
        except Exception as exc:
            logger.error("Failed to mark url=%s as failed in DB: %s", url, exc)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,       # Manual commit for at-least-once
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 300000,
    })

    consumer.subscribe([settings.kafka_topic])
    logger.info(
        "Kafka consumer started (topic=%s, group=%s)",
        settings.kafka_topic,
        settings.kafka_consumer_group,
    )

    loop = asyncio.get_event_loop()

    try:
        while not _shutdown_event.is_set():
            try:
                # Run blocking poll() in a thread to avoid blocking event loop
                msg = await loop.run_in_executor(None, consumer.poll, 1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            "Reached end of partition %s [%d] at offset %d",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                        )
                        continue
                    else:
                        logger.error("Kafka consumer error: %s", msg.error())
                        continue

                # Deserialize the message
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    url = data.get("url")
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    logger.error(
                        "Failed to decode Kafka message at offset %d: %s",
                        msg.offset(),
                        exc,
                    )
                    continue

                if not url:
                    logger.warning(
                        "Received Kafka message without 'url' field at offset %d",
                        msg.offset(),
                    )
                    continue

                logger.info(
                    "Consumer received url=%s (topic=%s, partition=%d, offset=%d)",
                    url,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )

                # Process the URL with error classification
                retry_count = data.get("retry_count", 0)

                try:
                    await process_url(url)
                    # Success — commit offset
                    consumer.commit(message=msg)
                    logger.debug(
                        "Committed offset %d for partition %d",
                        msg.offset(),
                        msg.partition(),
                    )

                except PermanentCollectionError as exc:
                    # Non-retryable (404, bad DNS, etc) → DLQ immediately
                    error_msg = str(exc)
                    logger.error(
                        "Permanent failure for url=%s: %s → sending to DLQ",
                        url, error_msg,
                    )
                    try:
                        await publish_to_dlq(url, retry_count, error_msg)
                        await mark_failed_in_db(url, error_msg)
                    except Exception as dlq_exc:
                        logger.error(
                            "Failed to publish url=%s to DLQ: %s", url, dlq_exc,
                        )
                    consumer.commit(message=msg)

                except (TransientCollectionError, Exception) as exc:
                    # Retryable (5xx, timeout, etc) → retry up to max
                    error_msg = str(exc)
                    retry_count += 1

                    if retry_count >= settings.kafka_max_retries:
                        logger.error(
                            "Transient failure exhausted retries for url=%s "
                            "after %d attempts: %s → sending to DLQ",
                            url, retry_count, error_msg,
                        )
                        try:
                            await publish_to_dlq(url, retry_count, error_msg)
                            await mark_failed_in_db(url, error_msg)
                        except Exception as dlq_exc:
                            logger.error(
                                "Failed to publish url=%s to DLQ: %s", url, dlq_exc,
                            )
                    else:
                        logger.warning(
                            "Transient failure for url=%s (attempt %d/%d): %s → retrying",
                            url, retry_count, settings.kafka_max_retries, error_msg,
                        )
                        try:
                            await publish_with_retry(url, retry_count)
                        except Exception as retry_exc:
                            logger.error(
                                "Failed to re-enqueue url=%s: %s", url, retry_exc,
                            )

                    consumer.commit(message=msg)

            except asyncio.CancelledError:
                logger.info("Consumer loop cancelled")
                raise

    except asyncio.CancelledError:
        pass
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")


async def start_consumer() -> None:
    """Start the background consumer as an asyncio task."""
    global _consumer_task, _shutdown_event

    if _consumer_task is not None and not _consumer_task.done():
        logger.warning("Kafka consumer is already running")
        return

    _shutdown_event = asyncio.Event()
    _consumer_task = asyncio.create_task(
        _consume_loop(), name="kafka-consumer"
    )
    logger.info("Kafka consumer task created")


async def stop_consumer() -> None:
    """Gracefully stop the Kafka consumer."""
    global _consumer_task, _shutdown_event

    if _consumer_task is None or _consumer_task.done():
        return

    # Signal shutdown
    _shutdown_event.set()

    # Cancel and wait
    _consumer_task.cancel()
    try:
        await _consumer_task
    except asyncio.CancelledError:
        pass

    _consumer_task = None
    _shutdown_event = None
    logger.info("Kafka consumer stopped")
