"""
FastAPI application entrypoint.

Creates and configures the FastAPI application with:
  - Lifespan management (MongoDB, indexes, consumer)
  - API router registration
  - CORS middleware
  - Custom exception handlers
  - Health check endpoint
"""

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import router as metadata_router
from app.core.lifespan import lifespan
from app.core.exceptions import MetadataServiceError


def create_app() -> FastAPI:
    """Application factory — creates and configures the FastAPI instance."""

    application = FastAPI(
        title="Metadata Collection Service",
        description=(
            "A scalable service for collecting and caching URL metadata "
            "(headers, cookies, and page source). Supports synchronous "
            "collection via POST and asynchronous background collection "
            "via GET with automatic cache population."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ── Middleware ────────────────────────────────────────────
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Routes ───────────────────────────────────────────────
    application.include_router(metadata_router, prefix="/api/v1")

    # ── Health Check ─────────────────────────────────────────
    @application.get(
        "/health",
        tags=["Health"],
        summary="Service health check",
        status_code=status.HTTP_200_OK,
    )
    async def health_check():
        """Return service health status."""
        return {"status": "healthy", "service": "metadata-service"}

    # ── Exception Handlers ───────────────────────────────────
    @application.exception_handler(MetadataServiceError)
    async def metadata_service_error_handler(
        request: Request, exc: MetadataServiceError
    ):
        """Handle all custom MetadataService exceptions."""
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": exc.message},
        )

    @application.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """Catch-all handler to prevent stack traces from leaking to clients."""
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "An internal server error occurred."},
        )

    return application


# Create the app instance — referenced by uvicorn as app.main:app
app = create_app()
