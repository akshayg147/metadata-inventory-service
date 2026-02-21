"""
Structured logging configuration.

Sets up JSON-style structured logging with consistent formatting
across all application modules.
"""

import logging
import sys
from app.core.config import settings


def setup_logging() -> None:
    """Configure application-wide structured logging."""
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # Root logger config
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )

    # Quieten noisy third-party loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("motor").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with the application's configuration."""
    return logging.getLogger(name)
