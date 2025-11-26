from typing import List

from fastapi import APIRouter, Depends, Query

from ..deps import get_dynamodb_table, Attr
from ..models import AreaSummary, AreaListResponse, Link
from .areas import _item_to_area_score

router = APIRouter(prefix="/scores", tags=["scores"])


@router.get("/latest", response_model=AreaListResponse)
def get_latest_scores(
    quart: str = Query(
        "jour",
        pattern="^(jour|soir|nuit)$",
        description="Time-of-day bucket: jour, soir, or nuit.",
    ),
    table=Depends(get_dynamodb_table),
):
    """
    GET /scores/latest

    Latest scores for all areas for a specific quart.
    """
    resp = table.scan(
        FilterExpression=Attr("isLatest").eq(True) & Attr("quart").eq(quart)
    )
    items: List[dict] = resp.get("Items", [])

    items_sorted = sorted(items, key=lambda it: it.get("areaName", ""))
    summaries: List[AreaSummary] = []
    for it in items_sorted:
        score = _item_to_area_score(it)
        summaries.append(
            AreaSummary(
                id=score.id,
                areaName=score.areaName,
                borough_code=score.borough_code,
                latestPeriod=score.period,
                quart=score.quart,
                safetyScore=score.safetyScore,
                colour=score.colour,
                _links=score._links,
            )
        )

    return AreaListResponse(
        items=summaries,
        total=len(summaries),
        limit=len(summaries),
        offset=0,
        _links={
            "self": Link(href=f"/scores/latest?quart={quart}"),
        },
    )
