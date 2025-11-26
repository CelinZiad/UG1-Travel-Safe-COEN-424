from typing import Dict, List, Optional

from pydantic import BaseModel, Field, ConfigDict


class Link(BaseModel):
    href: str


class Links(BaseModel):
    self: Link
    scores: Optional[Link] = None
    latest: Optional[Link] = None
    collection: Optional[Link] = None


class AreaScore(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

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
    # Internal name `links`, JSON name `_links`
    links: Links = Field(alias="_links")


class AreaSummary(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    areaName: str
    borough_code: str
    latestPeriod: str
    quart: str
    safetyScore: float
    colour: str
    links: Links = Field(alias="_links")


class AreaListResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    items: List[AreaSummary]
    total: int
    limit: int
    offset: int
    links: Dict[str, Link] = Field(alias="_links")


class AreaHistoryResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    areaName: str
    borough_code: str
    scores: List[AreaScore]
    links: Dict[str, Link] = Field(alias="_links")
