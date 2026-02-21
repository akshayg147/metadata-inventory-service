"""
URL normalisation utilities.

Ensures the same logical URL always maps to the same database key,
preventing duplicate entries for variants like:
  - http://Example.COM  →  http://example.com
  - https://example.com/path?b=2&a=1  →  https://example.com/path?a=1&b=2
  - https://example.com/path#section  →  https://example.com/path
"""

from urllib.parse import urlparse, urlunparse, urlencode, parse_qs


def normalize_url(url: str) -> str:
    """
    Normalise a URL to its canonical form.

    Transformations applied:
      1. Lowercase scheme and hostname
      2. Remove default ports (80 for http, 443 for https)
      3. Remove fragment (hash)
      4. Sort query parameters alphabetically
      5. Remove trailing slash on path (except root)
      6. Ensure scheme is present (default to https)

    Args:
        url: The raw URL string.

    Returns:
        The normalised URL string.
    """
    # Ensure scheme is present (case-insensitive check)
    if not url.lower().startswith(("http://", "https://")):
        url = f"https://{url}"

    parsed = urlparse(url)

    # Lowercase scheme and hostname
    scheme = parsed.scheme.lower()
    hostname = parsed.hostname or ""
    hostname = hostname.lower()

    # Remove default ports
    port = parsed.port
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None

    # Reconstruct netloc
    netloc = hostname
    if port:
        netloc = f"{hostname}:{port}"

    # Clean path — remove trailing slash (except for root "/")
    path = parsed.path
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    if not path:
        path = "/"

    # Sort query parameters for consistent ordering
    query_params = parse_qs(parsed.query, keep_blank_values=True)
    sorted_query = urlencode(
        sorted(
            [(k, v[0]) for k, v in query_params.items()],
            key=lambda x: x[0],
        )
    ) if query_params else ""

    # Drop fragment entirely
    normalised = urlunparse((scheme, netloc, path, "", sorted_query, ""))

    return normalised
