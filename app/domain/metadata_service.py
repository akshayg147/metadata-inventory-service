"""
Metadata service — core business logic.

Orchestrates the domain operations for creating and retrieving
URL metadata. This layer is framework-agnostic and depends only
on the repository and infrastructure abstractions.
"""

from typing import Optional

from app.core.exceptions import CollectionError
from app.core.logging import get_logger
from app.domain.models import MetadataRecord
from app.infrastructure.crawler.http_client import fetch_url
from app.infrastructure.db.repository import MetadataRepository
from app.infrastructure.messaging.producer import enqueue
from app.utils.url_normalizer import normalize_url

logger = get_logger(__name__)


class MetadataService:
    """Business logic for URL metadata operations."""

    def __init__(self, repository: MetadataRepository) -> None:
        self._repo = repository

    async def create_metadata(self, raw_url: str) -> MetadataRecord:
        """
        Collect and store metadata for a given URL (synchronous flow).

        This is the POST endpoint logic:
          1. Normalise the URL
          2. Fetch headers, cookies, and page source via HTTP
          3. Upsert the record into MongoDB
          4. Return the complete metadata record

        Args:
            raw_url: The original URL from the user.

        Returns:
            The complete MetadataRecord.

        Raises:
            CollectionError: If the URL cannot be fetched.
        """
        url = normalize_url(raw_url)
        logger.info("Creating metadata for url=%s (normalised from %s)", url, raw_url)

        # Fetch the URL metadata
        collected = await fetch_url(url)

        # Prepare the document payload
        data = {
            "headers": collected.headers,
            "cookies": collected.cookies,
            "page_source": collected.page_source,
            "status_code": collected.status_code,
        }

        # Upsert into the database
        doc_id = await self._repo.upsert(url, data)

        # Retrieve and return the full record
        doc = await self._repo.find_by_url(url)
        return MetadataRecord.from_mongo(doc)

    async def get_metadata(self, raw_url: str) -> tuple[Optional[MetadataRecord], bool]:
        """
        Retrieve metadata for a given URL (with background fill).

        This is the GET endpoint logic:
          1. Normalise the URL
          2. Look up in the database
          3a. If found and completed → return the record
          3b. If not found or not completed → enqueue for background
              collection and return None

        Args:
            raw_url: The original URL from the user.

        Returns:
            A tuple of (MetadataRecord or None, was_found: bool).
            If was_found is False, a background collection has been scheduled.
        """
        url = normalize_url(raw_url)
        logger.info("Getting metadata for url=%s (normalised from %s)", url, raw_url)

        # Check the database
        doc = await self._repo.find_by_url(url)

        if doc and doc.get("status") == "completed":
            logger.info("Cache HIT for url=%s", url)
            return MetadataRecord.from_mongo(doc), True

        # Cache miss — schedule background collection
        logger.info("Cache MISS for url=%s, scheduling background collection", url)
        was_marked = await self._repo.mark_pending(url)

        if was_marked:
            try:
                await enqueue(url)
            except Exception as exc:
                logger.error(
                    "Failed to enqueue url=%s: %s. "
                    "The pending record will be retried by the worker.",
                    url,
                    exc,
                )

        return None, False
