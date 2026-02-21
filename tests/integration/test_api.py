"""
Integration tests for the metadata API endpoints.

These tests use a test HTTP client against the FastAPI app
with mocked MongoDB and HTTP clients, validating the full
request → response cycle.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from httpx import AsyncClient

from app.infrastructure.crawler.http_client import CollectedData


@pytest.mark.asyncio
class TestPostMetadata:
    """Integration tests for POST /api/v1/metadata."""

    async def test_post_valid_url_returns_201(self, async_client: AsyncClient):
        """Should return 201 with full metadata on successful collection."""
        mock_collected = CollectedData(
            headers={"content-type": "text/html"},
            cookies={"session": "abc"},
            page_source="<html>Test</html>",
            status_code=200,
        )

        mock_doc = {
            "_id": "test_id_123",
            "url": "https://example.com/",
            "headers": {"content-type": "text/html"},
            "cookies": {"session": "abc"},
            "page_source": "<html>Test</html>",
            "status_code": 200,
            "status": "completed",
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        with patch(
            "app.domain.metadata_service.fetch_url",
            new_callable=AsyncMock,
            return_value=mock_collected,
        ), patch(
            "app.infrastructure.db.repository.get_database"
        ) as mock_db:
            mock_collection = MagicMock()
            mock_collection.update_one = AsyncMock(
                return_value=MagicMock(upserted_id="test_id_123")
            )
            mock_collection.find_one = AsyncMock(return_value=mock_doc)
            mock_db.return_value.__getitem__ = MagicMock(
                return_value=mock_collection
            )

            response = await async_client.post(
                "/api/v1/metadata",
                json={"url": "https://example.com"},
            )

        assert response.status_code == 201
        data = response.json()
        assert data["url"] == "https://example.com/"
        assert data["status"] == "completed"
        assert "headers" in data
        assert "page_source" in data

    async def test_post_invalid_url_returns_422(self, async_client: AsyncClient):
        """Should return 422 for invalid URL format (Pydantic validation)."""
        response = await async_client.post(
            "/api/v1/metadata",
            json={"url": "not-a-valid-url"},
        )

        assert response.status_code == 422

    async def test_post_missing_url_returns_422(self, async_client: AsyncClient):
        """Should return 422 when URL field is missing."""
        response = await async_client.post(
            "/api/v1/metadata",
            json={},
        )

        assert response.status_code == 422


@pytest.mark.asyncio
class TestGetMetadata:
    """Integration tests for GET /api/v1/metadata."""

    async def test_get_existing_url_returns_200(self, async_client: AsyncClient):
        """Should return 200 with full metadata when record exists."""
        mock_doc = {
            "_id": "test_id_123",
            "url": "https://example.com/",
            "headers": {"content-type": "text/html"},
            "cookies": {},
            "page_source": "<html>Cached</html>",
            "status_code": 200,
            "status": "completed",
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        with patch(
            "app.infrastructure.db.repository.get_database"
        ) as mock_db:
            mock_collection = MagicMock()
            mock_collection.find_one = AsyncMock(return_value=mock_doc)
            mock_db.return_value.__getitem__ = MagicMock(
                return_value=mock_collection
            )

            response = await async_client.get(
                "/api/v1/metadata",
                params={"url": "https://example.com"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["url"] == "https://example.com/"
        assert data["status"] == "completed"

    async def test_get_missing_url_returns_202(self, async_client: AsyncClient):
        """Should return 202 Accepted and schedule background collection."""
        with patch(
            "app.infrastructure.db.repository.get_database"
        ) as mock_db, patch(
            "app.domain.metadata_service.enqueue",
            new_callable=AsyncMock,
        ) as mock_enqueue:
            mock_collection = MagicMock()
            mock_collection.find_one = AsyncMock(return_value=None)
            mock_collection.update_one = AsyncMock(
                return_value=MagicMock(upserted_id="new_id", modified_count=0)
            )
            mock_db.return_value.__getitem__ = MagicMock(
                return_value=mock_collection
            )

            response = await async_client.get(
                "/api/v1/metadata",
                params={"url": "https://unknown-site.com"},
            )

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "pending"
        assert "scheduled" in data["message"].lower() or "retry" in data["message"].lower()

    async def test_get_without_url_param_returns_422(
        self, async_client: AsyncClient
    ):
        """Should return 422 when url query param is missing."""
        response = await async_client.get("/api/v1/metadata")

        assert response.status_code == 422


@pytest.mark.asyncio
class TestHealthCheck:
    """Integration tests for the health check endpoint."""

    async def test_health_returns_200(self, async_client: AsyncClient):
        """Should return 200 with healthy status."""
        response = await async_client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
