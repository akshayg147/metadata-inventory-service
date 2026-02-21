"""
FastAPI dependency injection.

Provides shared instances for use across API endpoints,
ensuring consistent lifecycle management and testability.
"""

from app.domain.metadata_service import MetadataService
from app.infrastructure.db.repository import MetadataRepository


def get_metadata_service() -> MetadataService:
    """
    Provide a MetadataService instance with its dependencies.

    This is registered as a FastAPI dependency so endpoints
    receive a fully-wired service without coupling to
    infrastructure details.
    """
    repository = MetadataRepository()
    return MetadataService(repository=repository)
