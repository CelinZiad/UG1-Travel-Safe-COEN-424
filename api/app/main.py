from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import get_settings
from .routers import areas, scores

settings = get_settings()

app = FastAPI(
    title="Travel Safe API",
    description=(
        "Thin, stateless REST API exposing area safety scores for the "
        "Travel Safe web map and AI helpers."
    ),
    version="0.1.0",
    contact={
        "name": "Travel Safe Team",
        "email": "example@example.com",
    },
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(areas.router)
app.include_router(scores.router)


@app.get("/health", tags=["health"])
def health():
    """
    Simple liveness / readiness check.
    Stateless: returns current UTC time and environment.
    """
    now_utc = datetime.now(timezone.utc).isoformat()
    return {
        "status": "ok",
        "service": settings.service_name,
        "environment": settings.environment,
        "time_utc": now_utc,
        "_links": {
            "self": {"href": "/health"},
            "docs": {"href": "/docs"},
            "openapi": {"href": "/openapi.json"},
        },
    }


@app.get("/", include_in_schema=False)
def root():
    return {
        "message": "Travel Safe API",
        "_links": {
            "health": {"href": "/health"},
            "areas": {"href": "/areas"},
            "scores_latest": {"href": "/scores/latest"},
            "docs": {"href": "/docs"},
        },
    }
