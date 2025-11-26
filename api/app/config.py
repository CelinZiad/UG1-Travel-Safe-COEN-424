import os
from functools import lru_cache

class Settings:
    service_name: str = "travel-safe-api"
    environment: str = os.getenv("APP_ENV", "local")
    # Will use this later for CORS, DB, etc.
    frontend_origin: str = os.getenv("FRONTEND_ORIGIN", "http://localhost:5173")

@lru_cache
def get_settings() -> Settings:
    return Settings()
