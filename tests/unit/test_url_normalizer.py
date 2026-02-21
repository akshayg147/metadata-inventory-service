"""
Unit tests for URL normalisation.

Tests cover:
  - Scheme handling
  - Hostname lowering
  - Default port removal
  - Fragment removal
  - Query parameter sorting
  - Trailing slash handling
"""

import pytest

from app.utils.url_normalizer import normalize_url


class TestNormalizeUrl:
    """Tests for the normalize_url function."""

    def test_adds_https_scheme_when_missing(self):
        """Should add https:// if no scheme is provided."""
        result = normalize_url("google.com")
        assert result == "https://google.com/"

    def test_lowercase_scheme_and_host(self):
        """Should lowercase the scheme and hostname."""
        result = normalize_url("HTTP://GOOGLE.COM/Path")
        assert result == "http://google.com/Path"

    def test_removes_default_http_port(self):
        """Should strip port 80 for http."""
        result = normalize_url("http://google.com:80/path")
        assert result == "http://google.com/path"

    def test_removes_default_https_port(self):
        """Should strip port 443 for https."""
        result = normalize_url("https://google.com:443/path")
        assert result == "https://google.com/path"

    def test_preserves_non_default_port(self):
        """Should keep non-default ports."""
        result = normalize_url("http://google.com:8080/path")
        assert result == "http://google.com:8080/path"

    def test_removes_fragment(self):
        """Should strip the URL fragment (hash)."""
        result = normalize_url("https://google.com/page#section")
        assert result == "https://google.com/page"

    def test_sorts_query_parameters(self):
        """Should sort query parameters alphabetically by key."""
        result = normalize_url("https://google.com/search?z=1&a=2&m=3")
        assert result == "https://google.com/search?a=2&m=3&z=1"

    def test_removes_trailing_slash_on_path(self):
        """Should remove trailing slash on non-root paths."""
        result = normalize_url("https://google.com/path/")
        assert result == "https://google.com/path"

    def test_keeps_root_slash(self):
        """Should keep the root path as /."""
        result = normalize_url("https://google.com")
        assert result == "https://google.com/"

    def test_preserves_full_path(self):
        """Should preserve full paths without modification."""
        result = normalize_url("https://google.com/a/b/c")
        assert result == "https://google.com/a/b/c"
