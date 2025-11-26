from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import get_settings


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


# ---- CORS (will matter later for the frontend, but safe to enable now) ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- Health endpoint (TASK 6.1) ----
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


# Placeholder root, optional
@app.get("/", include_in_schema=False)
def root():
    return {
        "message": "Travel Safe API",
        "_links": {
            "health": {"href": "/health"},
            "docs": {"href": "/docs"},
        },
    }
