"""
Application configuration using Pydantic Settings.

All configuration is read from environment variables (12-factor app),
with sensible defaults for local Docker Compose development.
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Central application configuration."""

    # MongoDB
    mongo_uri: str = Field(
        default="mongodb://localhost:27017",
        description="MongoDB connection URI",
    )
    mongo_db_name: str = Field(
        default="metadata_service",
        description="MongoDB database name",
    )

    # HTTP Client (for crawling URLs)
    http_timeout: int = Field(
        default=30,
        description="Timeout in seconds for outbound HTTP requests",
    )

    # Logging
    log_level: str = Field(
        default="INFO",
        description="Application log level (DEBUG, INFO, WARNING, ERROR)",
    )

    # API Server
    api_host: str = Field(default="0.0.0.0", description="API bind host")
    api_port: int = Field(default=8000, description="API bind port")

    # Kafka
    kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Kafka broker address(es), comma-separated",
    )
    kafka_topic: str = Field(
        default="metadata-tasks",
        description="Kafka topic for URL collection tasks",
    )
    kafka_consumer_group: str = Field(
        default="metadata-workers",
        description="Kafka consumer group for worker instances",
    )
    kafka_dlq_topic: str = Field(
        default="metadata-tasks-dlq",
        description="Kafka dead letter topic for permanently failed messages",
    )
    kafka_max_retries: int = Field(
        default=3,
        description="Max retry attempts before sending message to DLQ",
    )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


# Singleton — import this throughout the app
settings = Settings()
