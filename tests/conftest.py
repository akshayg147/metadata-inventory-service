"""
Shared test fixtures for the metadata service test suite.

Provides:
  - Async test client for FastAPI integration tests
  - Mock MongoDB fixtures
  - Mock HTTP response fixtures
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncIterator

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from app.main import app


@pytest.fixture(scope="session")
def event_loop():
    """Create a single event loop for the entire test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def async_client() -> AsyncIterator[AsyncClient]:
    """
    Provide an async HTTP test client for integration tests.

    Patches MongoDB connection and consumer to avoid requiring
    a live database during testing.
    """
    with patch("app.core.lifespan.connect_to_mongo", new_callable=AsyncMock), \
         patch("app.core.lifespan.close_mongo", new_callable=AsyncMock), \
         patch("app.core.lifespan.ensure_indexes", new_callable=AsyncMock), \
         patch("app.core.lifespan.ensure_topics"), \
         patch("app.core.lifespan.init_producer"), \
         patch("app.core.lifespan.close_producer"), \
         patch("app.core.lifespan.start_consumer", new_callable=AsyncMock), \
         patch("app.core.lifespan.stop_consumer", new_callable=AsyncMock):

        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            yield client


@pytest.fixture
def mock_repository():
    """
    Provide a mock MetadataRepository.

    Pre-configured with async methods for use in unit tests.
    """
    repo = MagicMock()
    repo.find_by_url = AsyncMock(return_value=None)
    repo.upsert = AsyncMock(return_value="mock_id_123")
    repo.mark_pending = AsyncMock(return_value=True)
    repo.mark_failed = AsyncMock()
    return repo


@pytest.fixture
def sample_metadata_doc():
    """Provide a sample MongoDB metadata document for testing."""
    from datetime import datetime, timezone

    return {
        "_id": "64f1a2b3c4d5e6f7a8b9c0d1",
        "url": "https://example.com/",
        "headers": {
            "content-type": "text/html; charset=UTF-8",
            "server": "ECS (dcb/7EEB)",
        },
        "cookies": {},
        "page_source": "<html><body><h1>Example</h1></body></html>",
        "status_code": 200,
        "status": "completed",
        "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }


@pytest.fixture
def sample_pending_doc():
    """Provide a sample pending metadata document."""
    from datetime import datetime, timezone

    return {
        "_id": "64f1a2b3c4d5e6f7a8b9c0d2",
        "url": "https://pending.example.com/",
        "status": "pending",
        "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }
