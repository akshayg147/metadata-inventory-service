"""
API routes for the metadata service.

Defines the POST and GET endpoints for URL metadata collection
and retrieval. Uses FastAPI dependency injection for clean
separation from business logic.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.dependencies import get_metadata_service
from app.api.schemas import (
    MetadataAccepted,
    MetadataRequest,
    MetadataResponse,
    ErrorResponse,
)
from app.core.exceptions import CollectionError, UrlValidationError
from app.core.logging import get_logger
from app.domain.metadata_service import MetadataService

logger = get_logger(__name__)

router = APIRouter(prefix="/metadata", tags=["Metadata"])


@router.post(
    "",
    response_model=MetadataResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Collect and store URL metadata",
    description=(
        "Fetches the headers, cookies, and page source for the given URL "
        "and stores them in the database. Returns the complete metadata record."
    ),
    responses={
        400: {"model": ErrorResponse, "description": "Invalid URL"},
        500: {"model": ErrorResponse, "description": "Collection failed"},
    },
)
async def create_metadata(
    request: MetadataRequest,
    service: MetadataService = Depends(get_metadata_service),
) -> MetadataResponse:
    """
    POST /metadata — Create a metadata record for a given URL.

    Synchronously fetches the URL, collects headers/cookies/page source,
    stores in MongoDB, and returns the full dataset.
    """
    try:
        record = await service.create_metadata(str(request.url))
        return MetadataResponse(
            id=record.id or "",
            url=record.url,
            headers=record.headers,
            cookies=record.cookies,
            page_source=record.page_source,
            status_code=record.status_code,
            status=record.status,
            collected_at=record.updated_at,
        )

    except CollectionError as exc:
        logger.warning("Collection failed: %s", exc.message)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=exc.message,
        ) from exc

    except UrlValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=exc.message,
        ) from exc

    except Exception as exc:
        logger.error("Unexpected error in create_metadata: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while collecting metadata.",
        ) from exc


@router.get(
    "",
    summary="Retrieve URL metadata",
    description=(
        "Returns cached metadata for the given URL if available. "
        "If not cached, returns 202 Accepted and schedules background collection."
    ),
    responses={
        200: {"model": MetadataResponse, "description": "Metadata found"},
        202: {"model": MetadataAccepted, "description": "Collection scheduled"},
        400: {"model": ErrorResponse, "description": "Invalid URL"},
    },
)
async def get_metadata(
    url: str = Query(
        ...,
        description="The URL to retrieve metadata for",
        example="https://example.com",
    ),
    service: MetadataService = Depends(get_metadata_service),
):
    """
    GET /metadata?url=... — Retrieve metadata for a given URL.

    - If the record exists and is completed → 200 with full metadata.
    - If the record is missing/pending → 202 Accepted with a background
      collection scheduled.
    """
    if not url or not url.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL query parameter is required.",
        )

    try:
        record, found = await service.get_metadata(url)

        if found and record:
            return MetadataResponse(
                id=record.id or "",
                url=record.url,
                headers=record.headers,
                cookies=record.cookies,
                page_source=record.page_source,
                status_code=record.status_code,
                status=record.status,
                collected_at=record.updated_at,
            )

        # Cache miss — return 202 Accepted
        from fastapi.responses import JSONResponse

        accepted = MetadataAccepted(url=url, status="pending")
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=accepted.model_dump(),
        )

    except Exception as exc:
        logger.error("Unexpected error in get_metadata: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while retrieving metadata.",
        ) from exc
