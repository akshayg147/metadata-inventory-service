"""
Metadata repository — MongoDB CRUD operations.

All database interactions for metadata records go through this module.
Uses Motor async driver for non-blocking I/O.
"""

from datetime import datetime, timezone
from typing import Any

from pymongo.errors import DuplicateKeyError, PyMongoError

from app.core.exceptions import DatabaseError
from app.core.logging import get_logger
from app.infrastructure.db.mongo import get_database

logger = get_logger(__name__)


class MetadataRepository:
    """Async repository for metadata documents in MongoDB."""

    COLLECTION_NAME = "metadata"

    def _get_collection(self):
        """Return the metadata collection handle."""
        return get_database()[self.COLLECTION_NAME]

    async def find_by_url(self, url: str) -> dict[str, Any] | None:
        """
        Look up a metadata record by its normalised URL.

        Args:
            url: The normalised URL to search for.

        Returns:
            The document dict if found, otherwise None.
        """
        try:
            document = await self._get_collection().find_one({"url": url})
            return document
        except PyMongoError as exc:
            logger.error("Database lookup failed for url=%s: %s", url, exc)
            raise DatabaseError("find_by_url", str(exc)) from exc

    async def upsert(self, url: str, data: dict[str, Any]) -> str:
        """
        Insert or update a metadata record for the given URL.

        Uses MongoDB's upsert to guarantee idempotency — repeated
        POSTs for the same URL simply overwrite the existing record.

        Args:
            url: The normalised URL (used as the unique key).
            data: The metadata payload to store.

        Returns:
            The string representation of the document's _id.
        """
        try:
            now = datetime.now(timezone.utc)
            result = await self._get_collection().update_one(
                {"url": url},
                {
                    "$set": {
                        **data,
                        "url": url,
                        "status": "completed",
                        "updated_at": now,
                    },
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
            doc_id = result.upserted_id or (
                await self._get_collection().find_one(
                    {"url": url}, {"_id": 1}
                )
            )["_id"]
            logger.info("Upserted metadata for url=%s (id=%s)", url, doc_id)
            return str(doc_id)

        except DuplicateKeyError:
            # Race condition on concurrent upserts — safe to retry once
            existing = await self.find_by_url(url)
            if existing:
                return str(existing["_id"])
            raise DatabaseError("upsert", f"Duplicate key conflict for {url}")

        except PyMongoError as exc:
            logger.error("Database upsert failed for url=%s: %s", url, exc)
            raise DatabaseError("upsert", str(exc)) from exc

    async def mark_pending(self, url: str) -> bool:
        """
        Atomically mark a URL as pending collection.

        Uses findAndModify semantics to prevent duplicate background
        fetches — only succeeds if no document exists or the existing
        document has failed status.

        Args:
            url: The normalised URL to mark as pending.

        Returns:
            True if the URL was newly marked pending (worker should fetch),
            False if a record already exists (skip).
        """
        try:
            now = datetime.now(timezone.utc)
            result = await self._get_collection().update_one(
                {
                    "url": url,
                    "status": {"$nin": ["completed", "pending"]},
                },
                {
                    "$set": {"url": url, "status": "pending", "updated_at": now},
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
            was_created = result.upserted_id is not None
            was_modified = result.modified_count > 0

            if was_created or was_modified:
                logger.info("Marked url=%s as pending", url)
                return True
            else:
                logger.debug("url=%s already pending or completed, skipping", url)
                return False

        except DuplicateKeyError:
            # Another request already created a record — safe to skip
            logger.debug("url=%s race condition on mark_pending, skipping", url)
            return False

        except PyMongoError as exc:
            logger.error("mark_pending failed for url=%s: %s", url, exc)
            raise DatabaseError("mark_pending", str(exc)) from exc

    async def mark_failed(self, url: str, reason: str) -> None:
        """
        Mark a URL's collection as failed so it can be retried later.

        Args:
            url: The normalised URL.
            reason: Human-readable failure reason.
        """
        try:
            await self._get_collection().update_one(
                {"url": url},
                {
                    "$set": {
                        "status": "failed",
                        "error": reason,
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
            logger.warning("Marked url=%s as failed: %s", url, reason)
        except PyMongoError as exc:
            logger.error("mark_failed failed for url=%s: %s", url, exc)
