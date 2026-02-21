"""
Unit tests for the MetadataService domain logic.

Tests cover:
  - create_metadata: successful collection and storage
  - get_metadata: cache hit behaviour
  - get_metadata: cache miss with background scheduling
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone

from app.domain.metadata_service import MetadataService
from app.infrastructure.crawler.http_client import CollectedData


@pytest.mark.asyncio
class TestCreateMetadata:
    """Tests for MetadataService.create_metadata()."""

    async def test_create_metadata_success(self, mock_repository, sample_metadata_doc):
        """Should fetch URL, upsert to DB, and return the complete record."""
        # Arrange
        service = MetadataService(repository=mock_repository)
        mock_repository.upsert = AsyncMock(return_value="mock_id_123")
        mock_repository.find_by_url = AsyncMock(return_value=sample_metadata_doc)

        mock_collected = CollectedData(
            headers={"content-type": "text/html"},
            cookies={},
            page_source="<html>Test</html>",
            status_code=200,
        )

        with patch(
            "app.domain.metadata_service.fetch_url",
            new_callable=AsyncMock,
            return_value=mock_collected,
        ):
            # Act
            result = await service.create_metadata("https://example.com")

        # Assert
        assert result.url == "https://example.com/"
        assert result.status == "completed"
        mock_repository.upsert.assert_called_once()

    async def test_create_metadata_fetch_failure(self, mock_repository):
        """Should propagate CollectionError when fetch fails."""
        from app.core.exceptions import CollectionError

        service = MetadataService(repository=mock_repository)

        with patch(
            "app.domain.metadata_service.fetch_url",
            new_callable=AsyncMock,
            side_effect=CollectionError("https://bad.com", "Connection refused"),
        ):
            with pytest.raises(CollectionError):
                await service.create_metadata("https://bad.com")


@pytest.mark.asyncio
class TestGetMetadata:
    """Tests for MetadataService.get_metadata()."""

    async def test_get_metadata_cache_hit(
        self, mock_repository, sample_metadata_doc
    ):
        """Should return the existing record on cache hit."""
        service = MetadataService(repository=mock_repository)
        mock_repository.find_by_url = AsyncMock(return_value=sample_metadata_doc)

        record, found = await service.get_metadata("https://example.com")

        assert found is True
        assert record is not None
        assert record.url == "https://example.com/"
        assert record.status == "completed"

    async def test_get_metadata_cache_miss(self, mock_repository):
        """Should return None and schedule background collection on miss."""
        service = MetadataService(repository=mock_repository)
        mock_repository.find_by_url = AsyncMock(return_value=None)
        mock_repository.mark_pending = AsyncMock(return_value=True)

        with patch(
            "app.domain.metadata_service.enqueue", new_callable=AsyncMock
        ) as mock_enqueue:
            record, found = await service.get_metadata("https://unknown.com")

        assert found is False
        assert record is None
        mock_repository.mark_pending.assert_called_once()
        mock_enqueue.assert_called_once()

    async def test_get_metadata_pending_not_re_enqueued(self, mock_repository):
        """Should not re-enqueue a URL that is already pending."""
        service = MetadataService(repository=mock_repository)

        pending_doc = {
            "_id": "some_id",
            "url": "https://pending.com/",
            "status": "pending",
        }
        mock_repository.find_by_url = AsyncMock(return_value=pending_doc)
        mock_repository.mark_pending = AsyncMock(return_value=False)

        with patch(
            "app.domain.metadata_service.enqueue", new_callable=AsyncMock
        ) as mock_enqueue:
            record, found = await service.get_metadata("https://pending.com")

        assert found is False
        # Should not enqueue since mark_pending returned False
        mock_enqueue.assert_not_called()
