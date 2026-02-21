"""
Unit tests for the HTTP client (crawler).

Tests cover:
  - Successful URL fetch
  - Timeout handling
  - Connection error handling
  - Redirect handling
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from app.infrastructure.crawler.http_client import fetch_url, CollectedData
from app.core.exceptions import CollectionError


@pytest.mark.asyncio
class TestFetchUrl:
    """Tests for the fetch_url function."""

    @patch("app.infrastructure.crawler.http_client.httpx.AsyncClient")
    async def test_successful_fetch(self, mock_client_cls):
        """Should return CollectedData with headers, cookies, and page source."""
        # Arrange
        mock_response = MagicMock()
        mock_response.headers = {"content-type": "text/html", "server": "nginx"}
        mock_response.cookies = MagicMock()
        mock_response.cookies.items.return_value = [("session", "abc123")]
        mock_response.text = "<html><body>Hello</body></html>"
        mock_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        # Act
        result = await fetch_url("https://example.com")

        # Assert
        assert isinstance(result, CollectedData)
        assert result.headers["content-type"] == "text/html"
        assert result.cookies["session"] == "abc123"
        assert "Hello" in result.page_source
        assert result.status_code == 200

    @patch("app.infrastructure.crawler.http_client.httpx.AsyncClient")
    async def test_timeout_raises_collection_error(self, mock_client_cls):
        """Should raise CollectionError on request timeout."""
        import httpx

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=httpx.TimeoutException("Connection timed out")
        )
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(CollectionError) as exc_info:
            await fetch_url("https://slow-site.com")

        assert "timed out" in exc_info.value.message.lower()

    @patch("app.infrastructure.crawler.http_client.httpx.AsyncClient")
    async def test_connection_error_raises_collection_error(self, mock_client_cls):
        """Should raise CollectionError on connection failure."""
        import httpx

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=httpx.ConnectError("Connection refused")
        )
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(CollectionError) as exc_info:
            await fetch_url("https://down-site.com")

        assert "connection failed" in exc_info.value.message.lower()

    @patch("app.infrastructure.crawler.http_client.httpx.AsyncClient")
    async def test_too_many_redirects(self, mock_client_cls):
        """Should raise CollectionError on redirect loops."""
        import httpx

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=httpx.TooManyRedirects("Too many redirects")
        )
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(CollectionError) as exc_info:
            await fetch_url("https://loop-site.com")

        assert "redirect" in exc_info.value.message.lower()
