"""
MongoDB client lifecycle management.

Provides async connection with retry-backoff, graceful shutdown,
and index management for the metadata collection.
"""

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Module-level client reference
_client: AsyncIOMotorClient | None = None
_database: AsyncIOMotorDatabase | None = None


async def connect_to_mongo(
    max_retries: int = 5, base_delay: float = 1.0
) -> None:
    """
    Establish MongoDB connection with exponential backoff retry.

    Args:
        max_retries: Maximum number of connection attempts.
        base_delay: Initial delay in seconds, doubles each retry.

    Raises:
        ConnectionFailure: If all retries are exhausted.
    """
    global _client, _database

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Connecting to MongoDB (attempt %d/%d)...",
                attempt,
                max_retries,
            )
            _client = AsyncIOMotorClient(
                settings.mongo_uri,
                serverSelectionTimeoutMS=5000,
                maxPoolSize=50,
                minPoolSize=5,
            )
            # Verify the connection is alive
            await _client.admin.command("ping")
            _database = _client[settings.mongo_db_name]
            logger.info("MongoDB connection established successfully")
            return

        except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
            if attempt == max_retries:
                logger.error("Failed to connect to MongoDB after %d attempts", max_retries)
                raise ConnectionFailure(
                    f"Could not connect to MongoDB after {max_retries} attempts"
                ) from exc

            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "MongoDB connection attempt %d failed: %s. Retrying in %.1fs...",
                attempt,
                str(exc),
                delay,
            )
            await asyncio.sleep(delay)


async def close_mongo() -> None:
    """Gracefully close the MongoDB connection."""
    global _client, _database

    if _client is not None:
        _client.close()
        _client = None
        _database = None
        logger.info("MongoDB connection closed")


def get_database() -> AsyncIOMotorDatabase:
    """
    Return the active database handle.

    Raises:
        RuntimeError: If called before connect_to_mongo().
    """
    if _database is None:
        raise RuntimeError(
            "MongoDB is not connected. Ensure connect_to_mongo() "
            "has been called during application startup."
        )
    return _database


async def ensure_indexes() -> None:
    """
    Create required database indexes.

    - Unique index on `url` for O(1) lookups and deduplication.
    - Index on `status` for efficient worker polling of pending tasks.
    - TTL index on `created_at` is intentionally omitted (metadata is
      long-lived), but can be added for cache expiry if needed.
    """
    db = get_database()
    collection = db.metadata

    await collection.create_index("url", unique=True, name="idx_url_unique")
    await collection.create_index("status", name="idx_status")
    logger.info("Database indexes ensured on 'metadata' collection")
