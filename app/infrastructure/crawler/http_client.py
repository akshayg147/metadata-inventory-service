"""
HTTP client for crawling URL metadata.

Uses httpx.AsyncClient for non-blocking HTTP requests with
connection pooling, configurable timeouts, and classified error handling.

Errors are classified as:
  - PermanentCollectionError: 4xx, DNS not found, TLS errors (no retry)
  - TransientCollectionError: 5xx, timeouts, connection resets (retry)
"""

import httpx
from dataclasses import dataclass, field

from app.core.config import settings
from app.core.exceptions import (
    CollectionError,
    PermanentCollectionError,
    TransientCollectionError,
)
from app.core.logging import get_logger

logger = get_logger(__name__)

# HTTP status codes that indicate permanent failure (never retry)
PERMANENT_STATUS_CODES = {400, 401, 403, 404, 405, 406, 410, 414, 451}

# HTTP status codes that indicate transient failure (worth retrying)
TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}


@dataclass
class CollectedData:
    """Structured result of a URL metadata collection."""

    headers: dict[str, str] = field(default_factory=dict)
    cookies: dict[str, str] = field(default_factory=dict)
    page_source: str = ""
    status_code: int = 0


async def fetch_url(url: str) -> CollectedData:
    """
    Fetch metadata (headers, cookies, page source) from a URL.

    Args:
        url: The URL to fetch.

    Returns:
        CollectedData with headers, cookies, and page source.

    Raises:
        PermanentCollectionError: For non-retryable failures (4xx, DNS, TLS).
        TransientCollectionError: For retryable failures (5xx, timeouts).
        CollectionError: For unclassified failures.
    """
    timeout = httpx.Timeout(
        timeout=settings.http_timeout,
        connect=10.0,
    )

    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            max_redirects=10,
        ) as client:
            logger.info("Fetching url=%s", url)
            response = await client.get(url)

            # Classify HTTP status codes
            if response.status_code in PERMANENT_STATUS_CODES:
                raise PermanentCollectionError(
                    url,
                    f"HTTP {response.status_code} — permanent failure",
                )

            if response.status_code in TRANSIENT_STATUS_CODES:
                raise TransientCollectionError(
                    url,
                    f"HTTP {response.status_code} — server error, retryable",
                )

            # Extract headers as a flat dict
            headers = dict(response.headers)

            # Extract cookies as a flat dict
            cookies = {
                name: value
                for name, value in response.cookies.items()
            }

            # Page source (body text)
            page_source = response.text

            logger.info(
                "Successfully fetched url=%s (status=%d, size=%d bytes)",
                url,
                response.status_code,
                len(page_source),
            )

            return CollectedData(
                headers=headers,
                cookies=cookies,
                page_source=page_source,
                status_code=response.status_code,
            )

    except (PermanentCollectionError, TransientCollectionError):
        # Re-raise classified errors without wrapping
        raise

    except httpx.TimeoutException as exc:
        logger.warning("Timeout fetching url=%s: %s", url, exc)
        raise TransientCollectionError(
            url, f"Request timed out after {settings.http_timeout}s"
        ) from exc

    except httpx.TooManyRedirects as exc:
        logger.warning("Too many redirects for url=%s: %s", url, exc)
        raise PermanentCollectionError(url, "Too many redirects") from exc

    except httpx.ConnectError as exc:
        error_str = str(exc).lower()
        # DNS resolution failure for non-existent domains → permanent
        if "name or service not known" in error_str or \
           "nodename nor servname" in error_str or \
           "no address associated" in error_str or \
           "getaddrinfo failed" in error_str:
            logger.warning("DNS resolution failed for url=%s: %s", url, exc)
            raise PermanentCollectionError(
                url, f"DNS resolution failed — domain does not exist: {exc}"
            ) from exc

        # Other connection errors (refused, reset) → transient
        logger.warning("Connection failed for url=%s: %s", url, exc)
        raise TransientCollectionError(
            url, f"Connection failed: {exc}"
        ) from exc

    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status in PERMANENT_STATUS_CODES:
            raise PermanentCollectionError(url, f"HTTP {status}") from exc
        elif status in TRANSIENT_STATUS_CODES:
            raise TransientCollectionError(url, f"HTTP {status}") from exc
        else:
            raise CollectionError(url, f"HTTP {status}") from exc

    except httpx.HTTPError as exc:
        logger.error("Unexpected HTTP error for url=%s: %s", url, exc)
        raise TransientCollectionError(url, str(exc)) from exc
