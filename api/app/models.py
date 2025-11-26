from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Link(BaseModel):
    href: str


class Links(BaseModel):
    self: Link
    scores: Optional[Link] = None
    latest: Optional[Link] = None
    collection: Optional[Link] = None


class AreaScore(BaseModel):
    id: str = Field(description="Area identifier, e.g. BOROUGH#Westmount")
    areaName: str
    borough_code: str
    period: str = Field(description="YYYYMM, e.g. 201201")
    quart: str = Field(description="Time-of-day bucket: jour, soir, or nuit")
    risk_crime: float
    risk_acc: float
    risk_total: float
    numIncidentsCrime: int
    numIncidentsAccidents: int
    safetyScore: float
    colour: str
    _links: Links


class AreaSummary(BaseModel):
    id: str
    areaName: str
    borough_code: str
    latestPeriod: str
    quart: str
    safetyScore: float
    colour: str
    _links: Links


class AreaListResponse(BaseModel):
    items: List[AreaSummary]
    total: int
    limit: int
    offset: int
    _links: Dict[str, Link]


class AreaHistoryResponse(BaseModel):
    id: str
    areaName: str
    borough_code: str
    scores: List[AreaScore]
    _links: Dict[str, Link]
