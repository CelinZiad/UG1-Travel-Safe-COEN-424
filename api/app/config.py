import os
from functools import lru_cache


class Settings:
    # General
    service_name: str = "travel-safe-api"
    environment: str = os.getenv("APP_ENV", "local")

    # Frontend (for CORS)
    frontend_origin: str = os.getenv("FRONTEND_ORIGIN", "http://localhost:5173")

    # AWS / DynamoDB
    aws_region: str = os.getenv("AWS_REGION", "ca-central-1")
    scores_table_name: str = os.getenv("SCORES_TABLE_NAME", "TravelSafeScores")

    # (For later) AI model, etc.
    hf_model: str = os.getenv("HF_MODEL", "distilbert-base-uncased")


@lru_cache
def get_settings() -> Settings:
    return Settings()
