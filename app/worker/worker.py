"""
Background worker — processes URLs from the message queue.

Receives URLs from the consumer, fetches metadata via the HTTP client,
and stores the result in MongoDB through the repository.

Raises classified exceptions so the consumer can decide:
  - PermanentCollectionError → DLQ immediately
  - TransientCollectionError → retry
  - Other exceptions → treated as transient
"""

from app.core.exceptions import (
    CollectionError,
    PermanentCollectionError,
    TransientCollectionError,
)
from app.core.logging import get_logger
from app.infrastructure.crawler.http_client import fetch_url
from app.infrastructure.db.repository import MetadataRepository

logger = get_logger(__name__)

# Shared repository instance for the worker
_repo = MetadataRepository()


async def process_url(url: str) -> None:
    """
    Process a single URL: fetch metadata and store in the database.

    This function is called by the consumer for each dequeued URL.
    It lets classified exceptions propagate so the consumer can
    decide whether to retry or send to DLQ.

    Args:
        url: The normalised URL to process.

    Raises:
        PermanentCollectionError: URL will never succeed (4xx, bad DNS).
        TransientCollectionError: URL might succeed on retry (5xx, timeout).
    """
    logger.info("Worker: processing url=%s", url)

    # Fetch URL metadata — raises classified exceptions
    collected = await fetch_url(url)

    # Prepare the document payload
    data = {
        "headers": collected.headers,
        "cookies": collected.cookies,
        "page_source": collected.page_source,
        "status_code": collected.status_code,
    }

    # Store in database
    doc_id = await _repo.upsert(url, data)
    logger.info(
        "Worker: successfully collected and stored metadata for "
        "url=%s (id=%s, size=%d bytes)",
        url,
        doc_id,
        len(collected.page_source),
    )
