"""
API request/response schemas.

These Pydantic models define the contract between the API layer
and external clients. They are separate from domain models to
allow the API surface to evolve independently.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class MetadataRequest(BaseModel):
    """Request body for POST /metadata."""

    url: HttpUrl = Field(
        ...,
        description="The URL to collect metadata for",
        json_schema_extra={"example": "https://example.com"},
    )


class MetadataResponse(BaseModel):
    """Full metadata response returned on cache hit."""

    id: str = Field(..., description="Unique record identifier")
    url: str = Field(..., description="The normalised URL")
    headers: dict[str, str] = Field(..., description="HTTP response headers")
    cookies: dict[str, str] = Field(..., description="HTTP response cookies")
    page_source: str = Field(..., description="HTML page source")
    status_code: int = Field(..., description="HTTP response status code")
    status: str = Field(..., description="Record status: completed")
    collected_at: Optional[datetime] = Field(
        None, description="When the metadata was last collected"
    )


class MetadataAccepted(BaseModel):
    """
    Response returned when metadata is not yet available (202 Accepted).

    Indicates that a background collection has been scheduled.
    """

    url: str = Field(..., description="The requested URL")
    status: str = Field(
        default="pending", description="Record status: pending"
    )
    message: str = Field(
        default="Metadata collection has been scheduled. Please retry shortly.",
        description="Human-readable status message",
    )


class ErrorResponse(BaseModel):
    """Standard error response."""

    detail: str = Field(..., description="Error description")
