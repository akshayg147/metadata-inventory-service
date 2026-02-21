"""
Custom application exceptions.

Centralised exception definitions for clean error handling
across all layers of the application.
"""


class MetadataServiceError(Exception):
    """Base exception for the metadata service."""

    def __init__(self, message: str = "An unexpected error occurred"):
        self.message = message
        super().__init__(self.message)


class CollectionError(MetadataServiceError):
    """Raised when URL metadata collection fails."""

    def __init__(self, url: str, reason: str = "Unknown error"):
        self.url = url
        self.reason = reason
        super().__init__(f"Failed to collect metadata for '{url}': {reason}")


class PermanentCollectionError(CollectionError):
    """
    Non-retryable failure — the URL will never succeed.

    Examples: 404 Not Found, 403 Forbidden, DNS for non-existent domain,
    TLS certificate error, invalid URL scheme.
    """
    pass


class TransientCollectionError(CollectionError):
    """
    Retryable failure — the URL might succeed on a subsequent attempt.

    Examples: 503 Service Unavailable, 429 Too Many Requests,
    connection timeout, temporary DNS resolution failure.
    """
    pass


class UrlValidationError(MetadataServiceError):
    """Raised when a URL is invalid or unreachable."""

    def __init__(self, url: str, reason: str = "Invalid URL"):
        self.url = url
        self.reason = reason
        super().__init__(f"URL validation failed for '{url}': {reason}")


class DatabaseError(MetadataServiceError):
    """Raised when a database operation fails."""

    def __init__(self, operation: str, reason: str = "Unknown error"):
        self.operation = operation
        self.reason = reason
        super().__init__(f"Database error during '{operation}': {reason}")
