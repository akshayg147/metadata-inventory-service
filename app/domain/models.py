"""
Domain models — pure data structures for the metadata service.

These models have no framework dependencies and represent the core
business entities. They are used across all layers.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class MetadataRecord(BaseModel):
    """
    Complete metadata record as stored in the database.

    This is the canonical internal representation used between
    the domain, repository, and API layers.
    """

    id: Optional[str] = Field(None, description="MongoDB document ID")
    url: str = Field(..., description="Normalised URL")
    headers: dict[str, str] = Field(
        default_factory=dict, description="HTTP response headers"
    )
    cookies: dict[str, str] = Field(
        default_factory=dict, description="HTTP response cookies"
    )
    page_source: str = Field(default="", description="HTML page source")
    status_code: int = Field(default=0, description="HTTP status code")
    status: str = Field(
        default="pending",
        description="Record status: pending | completed | failed",
    )
    error: Optional[str] = Field(
        None, description="Error message if collection failed"
    )
    created_at: Optional[datetime] = Field(
        None, description="Timestamp of initial creation"
    )
    updated_at: Optional[datetime] = Field(
        None, description="Timestamp of last update"
    )

    model_config = {"from_attributes": True}

    @classmethod
    def from_mongo(cls, doc: dict) -> "MetadataRecord":
        """
        Construct a MetadataRecord from a raw MongoDB document.

        Handles _id → id conversion and missing fields gracefully.
        """
        if doc is None:
            raise ValueError("Cannot create MetadataRecord from None")

        return cls(
            id=str(doc.get("_id", "")),
            url=doc.get("url", ""),
            headers=doc.get("headers", {}),
            cookies=doc.get("cookies", {}),
            page_source=doc.get("page_source", ""),
            status_code=doc.get("status_code", 0),
            status=doc.get("status", "pending"),
            error=doc.get("error"),
            created_at=doc.get("created_at"),
            updated_at=doc.get("updated_at"),
        )
