from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query

from ..deps import get_dynamodb_table, Key, Attr
from ..models import (
    AreaListResponse,
    AreaSummary,
    AreaScore,
    AreaHistoryResponse,
    Link,
    Links,
)

router = APIRouter(prefix="/areas", tags=["areas"])


def _item_to_area_score(item: dict) -> AreaScore:
    """
    Convert a DynamoDB item from TravelSafeScores into an AreaScore model.
    Expects keys like pk, areaId, areaName, borough_code, period, quart, etc.
    """
    # Prefer explicit areaId; fall back to pk
    area_id = item.get("areaId") or item["pk"]
    period = item["period"]
    quart = item.get("quart", "all")

    base_path = f"/areas/{area_id}"
    links = Links(
        self=Link(href=base_path),
        scores=Link(href=f"{base_path}/scores"),
        latest=Link(href=base_path),
        collection=Link(href="/areas"),
    )

    # NOTE: we pass links=links (no underscore)
    return AreaScore(
        id=area_id,
        areaName=item["areaName"],
        borough_code=item["borough_code"],
        period=period,
        quart=quart,
        risk_crime=float(item.get("risk_crime", 0.0)),
        risk_acc=float(item.get("risk_acc", 0.0)),
        risk_total=float(item.get("risk_total", 0.0)),
        numIncidentsCrime=int(item.get("numIncidentsCrime", 0)),
        numIncidentsAccidents=int(item.get("numIncidentsAccidents", 0)),
        safetyScore=float(item.get("safetyScore", 0.0)),
        colour=item.get("colour", "GREY"),
        links=links,
    )


@router.get("", response_model=AreaListResponse)
def list_areas(
    quart: str = Query(
        "jour",
        pattern="^(jour|soir|nuit)$",
        description="Time-of-day bucket: jour, soir, or nuit.",
    ),
    limit: int = Query(18, ge=1, le=100),
    offset: int = Query(0, ge=0),
    table=Depends(get_dynamodb_table),
):
    """
    GET /areas

    List all areas (18 boroughs) with their latest safety score
    for a given time-of-day bucket (quart).
    """
    resp = table.scan(
        FilterExpression=Attr("isLatest").eq(True) & Attr("quart").eq(quart)
    )
    items: List[dict] = resp.get("Items", [])

    items_sorted = sorted(items, key=lambda it: it.get("areaName", ""))

    paged = items_sorted[offset : offset + limit]
    summaries: List[AreaSummary] = []
    for it in paged:
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
                # HERE: links=score.links instead of _links=score._links
                links=score.links,
            )
        )

    base_query = f"/areas?quart={quart}"
    self_href = f"{base_query}&limit={limit}&offset={offset}"

    list_links = {
        "self": Link(href=self_href),
        "first": Link(href=base_query),
    }
    if offset + limit < len(items_sorted):
        list_links["next"] = Link(
            href=f"{base_query}&limit={limit}&offset={offset + limit}"
        )
    if offset > 0:
        prev_offset = max(0, offset - limit)
        list_links["prev"] = Link(
            href=f"{base_query}&limit={limit}&offset={prev_offset}"
        )

    # HERE: links=list_links (no underscore)
    return AreaListResponse(
        items=summaries,
        total=len(items_sorted),
        limit=limit,
        offset=offset,
        links=list_links,
    )


@router.get("/{area_id}", response_model=AreaScore)
def get_area(
    area_id: str,
    quart: str = Query(
        "jour",
        pattern="^(jour|soir|nuit)$",
        description="Time-of-day bucket: jour, soir, or nuit.",
    ),
    table=Depends(get_dynamodb_table),
):
    """
    GET /areas/{id}

    Metadata + latest safety score for a given area and quart.
    """
    resp = table.scan(
        FilterExpression=(
            Attr("areaId").eq(area_id)
            & Attr("isLatest").eq(True)
            & Attr("quart").eq(quart)
        )
    )
    items = resp.get("Items", [])
    if not items:
        raise HTTPException(
            status_code=404,
            detail="Area not found for this quart",
        )

    item = items[0]
    return _item_to_area_score(item)


@router.get("/{area_id}/scores", response_model=AreaHistoryResponse)
def get_area_scores(
    area_id: str,
    from_: Optional[str] = Query(
        None, alias="from", description="Start period (YYYY-MM)"
    ),
    to: Optional[str] = Query(
        None, description="End period (YYYY-MM)"
    ),
    quart: Optional[str] = Query(
        None,
        description="jour | soir | nuit. If omitted, returns all quarts.",
    ),
    table=Depends(get_dynamodb_table),
):
    """
    GET /areas/{id}/scores

    Historical scores for an area, optionally filtered by period range and quart.
    """

    def to_period(val: Optional[str]) -> Optional[str]:
        return val.replace("-", "") if val else None

    from_period = to_period(from_)
    to_period = to_period(to)

    resp = table.query(
        KeyConditionExpression=Key("pk").eq(area_id)
        & Key("sk").begins_with("PERIOD#"),
        ScanIndexForward=True,
    )
    items = resp.get("Items", [])

    def within_period(it: dict) -> bool:
        p = it.get("period")
        if not p:
            return False
        if from_period and p < from_period:
            return False
        if to_period and p > to_period:
            return False
        return True

    filtered = [it for it in items if within_period(it)]
    if quart is not None:
        filtered = [it for it in filtered if it.get("quart") == quart]

    if not filtered:
        raise HTTPException(status_code=404, detail="No scores for this area")

    scores = [_item_to_area_score(it) for it in filtered]
    first = scores[0]

    # NOTE: links=..., not _links=...
    return AreaHistoryResponse(
        id=area_id,
        areaName=first.areaName,
        borough_code=first.borough_code,
        scores=scores,
        links={
            "self": Link(href=f"/areas/{area_id}/scores"),
            "area": Link(href=f"/areas/{area_id}"),
        },
    )
