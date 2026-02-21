"""
FastAPI application lifespan management.

Handles startup and shutdown of all long-lived resources:
  - MongoDB connection
  - Database indexes
  - Kafka producer and consumer
  - Logging setup
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app.core.logging import setup_logging, get_logger
from app.infrastructure.db.mongo import (
    connect_to_mongo,
    close_mongo,
    ensure_indexes,
)
from app.infrastructure.messaging.producer import (
    init_producer,
    close_producer,
    ensure_topics,
)
from app.infrastructure.messaging.consumer import start_consumer, stop_consumer

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Manage application lifecycle.

    Startup:
      1. Configure structured logging
      2. Connect to MongoDB with retry-backoff
      3. Create database indexes
      4. Initialize Kafka producer
      5. Start Kafka consumer task

    Shutdown:
      1. Stop Kafka consumer
      2. Close Kafka producer (flush pending messages)
      3. Close MongoDB connection
    """
    # ── Startup ──────────────────────────────────────────────
    setup_logging()
    logger.info("Starting metadata service...")

    await connect_to_mongo()
    await ensure_indexes()
    logger.info("MongoDB connected and indexes ensured")

    ensure_topics()
    logger.info("Kafka topics ensured")

    init_producer()
    logger.info("Kafka producer initialized")

    await start_consumer()
    logger.info("Kafka consumer started")

    yield

    # ── Shutdown ─────────────────────────────────────────────
    logger.info("Shutting down metadata service...")
    await stop_consumer()
    close_producer()
    await close_mongo()
    logger.info("Shutdown complete")
