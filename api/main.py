import os
from decimal import Decimal
from typing import List, Optional

import boto3
from boto3.dynamodb.conditions import Key, Attr
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

try:
    from .ai_safety import SafetyAdvisor
except ImportError:
    from ai_safety import SafetyAdvisor


# Configuration
AWS_REGION = os.getenv("AWS_REGION", "ca-central-1")
SCORES_TABLE = os.getenv("SCORES_TABLE", "TravelSafe_AreaScores")
AREAS_TABLE = os.getenv("AREAS_TABLE", "TravelSafe_Areas")

app = FastAPI(
    title="Travel Safe API",
    description="Safety scoring API for Montreal areas",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize AWS resources
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
scores_table = dynamodb.Table(SCORES_TABLE)
areas_table = dynamodb.Table(AREAS_TABLE)

# Initialize AI advisor
safety_advisor = SafetyAdvisor()


# Response models
class AreaSummary(BaseModel):
    area_id: str
    name: str
    source: Optional[str] = None


class AreaScore(BaseModel):
    area_id: str
    period: str
    borough_name: Optional[str] = None
    safety_score: float
    color: str
    crime_count: int
    accident_count: int


class AreaDetail(BaseModel):
    area_id: str
    name: str
    latest_score: Optional[AreaScore] = None


class SafetyAdvice(BaseModel):
    area_id: str
    area_name: str
    safety_score: float
    advice: str


class RouteAnalysis(BaseModel):
    areas: List[str]
    overall_safety: float
    recommendation: str
    details: List[dict]


def decimal_to_float(obj):
    """Convert Decimal values to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    return obj


@app.get("/")
def root():
    return {"service": "Travel Safe API", "version": "1.0.0", "status": "running"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/areas", response_model=List[AreaSummary])
def list_areas(
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0)
):
    """List all areas with pagination."""
    try:
        response = areas_table.scan(Limit=limit + offset)
        items = response.get("Items", [])[offset:offset + limit]

        return [
            AreaSummary(
                area_id=item.get("area_id", ""),
                name=item.get("name", ""),
                source=item.get("source")
            )
            for item in items
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/areas/{area_id}", response_model=AreaDetail)
def get_area(area_id: str):
    """Get area metadata and latest safety score."""
    try:
        # Get area metadata
        area_response = areas_table.get_item(Key={"area_id": area_id})
        area = area_response.get("Item")

        if not area:
            raise HTTPException(status_code=404, detail=f"Area {area_id} not found")

        # Get latest score (query by area_id, sort by period descending)
        scores_response = scores_table.query(
            KeyConditionExpression=Key("area_id").eq(area_id),
            ScanIndexForward=False,
            Limit=1
        )
        scores = scores_response.get("Items", [])

        latest_score = None
        if scores:
            s = decimal_to_float(scores[0])
            latest_score = AreaScore(
                area_id=s.get("area_id", ""),
                period=s.get("period", ""),
                borough_name=s.get("borough_name"),
                safety_score=s.get("safety_score", 0),
                color=s.get("color", "yellow"),
                crime_count=int(s.get("crime_count", 0)),
                accident_count=int(s.get("accident_count", 0))
            )

        return AreaDetail(
            area_id=area.get("area_id", ""),
            name=area.get("name", ""),
            latest_score=latest_score
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/areas/{area_id}/scores", response_model=List[AreaScore])
def get_area_scores(
    area_id: str,
    from_period: Optional[str] = Query(default=None, alias="from"),
    to_period: Optional[str] = Query(default=None, alias="to")
):
    """Get historical safety scores for an area."""
    try:
        # Build query
        key_condition = Key("area_id").eq(area_id)

        if from_period and to_period:
            key_condition = key_condition & Key("period").between(from_period, to_period)
        elif from_period:
            key_condition = key_condition & Key("period").gte(from_period)
        elif to_period:
            key_condition = key_condition & Key("period").lte(to_period)

        response = scores_table.query(
            KeyConditionExpression=key_condition,
            ScanIndexForward=True
        )

        items = decimal_to_float(response.get("Items", []))

        return [
            AreaScore(
                area_id=item.get("area_id", ""),
                period=item.get("period", ""),
                borough_name=item.get("borough_name"),
                safety_score=item.get("safety_score", 0),
                color=item.get("color", "yellow"),
                crime_count=int(item.get("crime_count", 0)),
                accident_count=int(item.get("accident_count", 0))
            )
            for item in items
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scores/latest", response_model=List[AreaScore])
def get_latest_scores(limit: int = Query(default=100, le=500)):
    """Get latest safety scores for all areas."""
    try:
        response = scores_table.scan(Limit=limit)
        items = decimal_to_float(response.get("Items", []))

        # Group by area and get latest
        latest_by_area = {}
        for item in items:
            area_id = item.get("area_id")
            period = item.get("period", "")
            if area_id not in latest_by_area or period > latest_by_area[area_id].get("period", ""):
                latest_by_area[area_id] = item

        return [
            AreaScore(
                area_id=item.get("area_id", ""),
                period=item.get("period", ""),
                borough_name=item.get("borough_name"),
                safety_score=item.get("safety_score", 0),
                color=item.get("color", "yellow"),
                crime_count=int(item.get("crime_count", 0)),
                accident_count=int(item.get("accident_count", 0))
            )
            for item in latest_by_area.values()
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/advice/{area_id}", response_model=SafetyAdvice)
def get_safety_advice(area_id: str):
    """Get AI-generated safety advice for an area."""
    try:
        # Get area info
        area_response = areas_table.get_item(Key={"area_id": area_id})
        area = area_response.get("Item")

        if not area:
            raise HTTPException(status_code=404, detail=f"Area {area_id} not found")

        # Get latest score
        scores_response = scores_table.query(
            KeyConditionExpression=Key("area_id").eq(area_id),
            ScanIndexForward=False,
            Limit=1
        )
        scores = scores_response.get("Items", [])

        safety_score = 50.0
        crime_count = 0
        accident_count = 0

        if scores:
            s = decimal_to_float(scores[0])
            safety_score = s.get("safety_score", 50.0)
            crime_count = int(s.get("crime_count", 0))
            accident_count = int(s.get("accident_count", 0))

        area_name = area.get("name", area_id)
        advice = safety_advisor.get_advice(area_name, safety_score, crime_count, accident_count)

        return SafetyAdvice(
            area_id=area_id,
            area_name=area_name,
            safety_score=safety_score,
            advice=advice
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analyze-route", response_model=RouteAnalysis)
def analyze_route(area_ids: List[str]):
    """Analyze safety of a route through multiple areas."""
    try:
        if not area_ids:
            raise HTTPException(status_code=400, detail="No areas provided")

        details = []
        total_score = 0

        for area_id in area_ids:
            # Get latest score for each area
            scores_response = scores_table.query(
                KeyConditionExpression=Key("area_id").eq(area_id),
                ScanIndexForward=False,
                Limit=1
            )
            scores = scores_response.get("Items", [])

            score = 50.0
            if scores:
                s = decimal_to_float(scores[0])
                score = s.get("safety_score", 50.0)

            details.append({
                "area_id": area_id,
                "safety_score": score
            })
            total_score += score

        overall = total_score / len(area_ids) if area_ids else 0
        recommendation = safety_advisor.get_route_recommendation(overall, len(area_ids))

        return RouteAnalysis(
            areas=area_ids,
            overall_safety=overall,
            recommendation=recommendation,
            details=details
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ask")
def ask_safety_question(question: str = Query(..., min_length=3)):
    """Ask a natural language question about safety."""
    try:
        answer = safety_advisor.answer_question(question)
        return {"question": question, "answer": answer}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
