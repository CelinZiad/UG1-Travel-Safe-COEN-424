from functools import lru_cache

import boto3
from boto3.dynamodb.conditions import Key, Attr

from .config import get_settings


@lru_cache
def get_dynamodb_table():
    """
    Returns the DynamoDB table handle for TravelSafeScores.
    """
    settings = get_settings()
    dynamodb = boto3.resource("dynamodb", region_name=settings.aws_region)
    return dynamodb.Table(settings.scores_table_name)


# Re-export Key/Attr so routers can just import from deps
__all__ = ["get_dynamodb_table", "Key", "Attr"]
