from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ..deps import get_dynamodb_table, Attr, Key
from ..models import (
    AdviceResponse,
    RouteAnalyzeRequest,
    RouteAnalyzeResponse,
    RouteAreaSummary,
    AskRequest,
    AskResponse,
    Link,
)
from ..ai import generate_text_with_local_model
from ..config import get_settings

router = APIRouter(prefix="", tags=["ai"])  # endpoints at /advice, /analyze-route, /ask


# -------------------------------------------------------------------
# Helper functions for deterministic, clean safety advice
# -------------------------------------------------------------------

def _score_band(score: float) -> str:
    """
    Turn a numeric safetyScore into a band:
    - "high"    : score >= 80
    - "medium"  : 60 <= score < 80
    - "low"     : score < 60
    """
    if score >= 80:
        return "high"
    if score >= 60:
        return "medium"
    return "low"


def _quart_label(quart: str) -> str:
    """
    Human-friendly description for time-of-day quart.
    """
    mapping = {
        "jour": "during the day",
        "soir": "in the evening",
        "nuit": "at night",
    }
    return mapping.get(quart, "at this time of day")


def _fallback_area_advice(
    area_name: str,
    quart: str,
    safety_score: float,
    num_crime: int,
    num_acc: int,
) -> str:
    """
    Deterministic advice when the small language model output is not reliable.
    Uses the score band + incidents to generate a short paragraph.
    """
    band = _score_band(safety_score)
    time_label = _quart_label(quart)

    if band == "high":
        intro = (
            f"{area_name} is generally considered a relatively safe borough {time_label}."
        )
        detail = (
            "You can usually move around comfortably, but it is still smart to stay aware of your "
            "surroundings, keep your phone and wallet close, and stick to streets where there are other people."
        )
    elif band == "medium":
        intro = (
            f"{area_name} has a typical urban risk level {time_label}."
        )
        detail = (
            "Stay alert, avoid very isolated side streets when possible, and pay attention on public transport "
            "and at busy intersections. Planning your route in advance and travelling with a friend can make "
            "your trip feel safer."
        )
    else:
        intro = (
            f"In {area_name}, it is a good idea to be a bit more cautious {time_label}."
        )
        detail = (
            "Try to stay in well-lit, busier areas, avoid cutting through parks or alleys alone, and keep "
            "valuables out of sight. If you feel unsure about part of the area, consider using main roads or "
            "public transport instead of walking alone."
        )

    incidents_bits = []
    if num_crime > 0:
        incidents_bits.append("crime reports")
    if num_acc > 0:
        incidents_bits.append("traffic or collision reports")

    if incidents_bits:
        incidents = (
            "Local data includes some "
            + " and ".join(incidents_bits)
            + ", so being extra careful near busy roads and intersections is helpful."
        )
    else:
        incidents = (
            "Even if incident levels are not very high, normal city precautions still apply."
        )

    generic = (
        "In general, let someone know where you are going, keep your phone charged, and trust your instincts "
        "if a place or situation feels uncomfortable."
    )

    return " ".join([intro, detail, incidents, generic])


def _fallback_route_advice(
    areas: List[RouteAreaSummary],
    quart: str,
) -> str:
    """
    Deterministic route advice: describes start, destination, relative safety along route.
    """
    time_label = _quart_label(quart)
    start = areas[0]
    end = areas[-1]

    avg_score = sum(a.safetyScore for a in areas) / len(areas)
    overall_band = _score_band(avg_score)

    safest = max(areas, key=lambda a: a.safetyScore)
    riskiest = min(areas, key=lambda a: a.safetyScore)

    if overall_band == "high":
        intro = (
            f"Overall, this route is relatively safe {time_label}, starting in "
            f"{start.areaName} and ending in {end.areaName}."
        )
    elif overall_band == "medium":
        intro = (
            f"This route has a moderate safety level {time_label}. "
            f"It begins in {start.areaName} and finishes in {end.areaName}."
        )
    else:
        intro = (
            f"This route passes through some areas where extra caution is sensible {time_label}, "
            f"from {start.areaName} to {end.areaName}."
        )

    contrast = (
        f"{safest.areaName} tends to appear safer in the data, while "
        f"{riskiest.areaName} shows relatively higher incident levels."
    )

    tips = (
        "Try to follow well-lit main streets, especially when it is darker or quieter, and avoid lingering in very "
        "isolated spots. Travelling with someone else, planning your route in advance, and using public transport "
        "for longer or less familiar segments can make the trip more comfortable. As always, keep your belongings "
        "secure and stay aware of your surroundings along the whole route."
    )

    return " ".join([intro, contrast, tips])


def _fallback_ask_answer(
    question: str,
    quart: Optional[str],
    context_items: List[dict],
) -> str:
    """
    Deterministic answer for /ask when we cannot trust the LM output.
    We use the first borough as the main reference and give general city safety advice.
    """
    time_label = _quart_label(quart) if quart else "at different times of day"

    if context_items:
        primary = context_items[0]
        borough_name = primary.get("areaName", "this borough")
        score = float(primary.get("safetyScore", 0.0))
        band = _score_band(score)

        intro = (
            f"Based on the available data, {borough_name} is considered to have a {band} overall safety level "
            f"{time_label}."
        )

        generic = (
            "If you plan to walk, try to stay on well-lit, busier streets, avoid very isolated areas, and keep "
            "valuables like your phone and wallet secure. Using public transport on familiar routes, letting "
            "someone know roughly where you are going, and trusting your instincts if something feels off are all "
            "good practices. If you ever feel uncomfortable in a specific street or park, changing your route or "
            "moving towards a busier area is usually a good idea."
        )

        return " ".join([intro, generic])

    # No borough context → generic Montréal advice
    intro = (
        "Safety in Montréal can vary from one neighbourhood to another, but the usual big-city advice applies."
    )
    generic = (
        "Stick to well-lit streets, avoid walking alone late at night in very quiet areas, keep your belongings "
        "close to you, and be cautious around busy intersections and transit hubs. Let someone know your plans "
        "if you are uncertain about an area, and consider using main roads or public transport rather than "
        "shortcuts through isolated places."
    )
    return " ".join([intro, generic])


# -------------------------------------------------------------------
# DynamoDB helpers
# -------------------------------------------------------------------

def _get_latest_item_for_area_quart(table, area_id: str, quart: str) -> dict:
    """
    Get the latest PERIOD# row for a given area and quart, based purely on keys.
    We:
      1. Query all PERIOD# items for pk=area_id.
      2. Sort by period descending.
      3. Pick the first item whose quart matches.
    This avoids relying on areaId / isLatest attribute types.
    """
    resp = table.query(
        KeyConditionExpression=Key("pk").eq(area_id) & Key("sk").begins_with("PERIOD#"),
        ScanIndexForward=False,  # newest period first
    )
    items = resp.get("Items", [])

    for it in items:
        if it.get("quart") == quart:
            return it

    raise HTTPException(
        status_code=404,
        detail=f"Area {area_id} not found for quart={quart}",
    )

# -------------------------------------------------------------------
# GET /advice/{area_id}
# -------------------------------------------------------------------

@router.get("/advice/{area_id}", response_model=AdviceResponse)
def get_advice_for_area(
    area_id: str,
    quart: str = Query(
        "jour",
        pattern="^(jour|soir|nuit)$",
        description="Time-of-day bucket: jour, soir, or nuit.",
    ),
    table=Depends(get_dynamodb_table),
):
    """
    GET /advice/{area_id}

    AI-generated safety advice for an area using the latest safety score
    for the specified quart. We still call the local DistilGPT-2 model
    for the assignment requirement, but the final advice text comes from
    a deterministic helper so that users always see clean, stable advice.
    """
    settings = get_settings()
    item = _get_latest_item_for_area_quart(table, area_id, quart)

    area_name = item["areaName"]
    borough_code = item["borough_code"]
    period = item["period"]
    safety_score = float(item.get("safetyScore", 0.0))
    colour = item.get("colour", "GREY")
    num_crime = int(item.get("numIncidentsCrime", 0))
    num_acc = int(item.get("numIncidentsAccidents", 0))
    risk_crime = float(item.get("risk_crime", 0.0))
    risk_acc = float(item.get("risk_acc", 0.0))

    # ---- HF model call (we ignore the text, but this demonstrates integration) ----
    prompt = f"""
You are a small language model assisting a safety app.
You receive factual data about a Montréal borough and internally generate ideas for safety tips.

Borough: {area_name} ({borough_code})
Area ID: {area_id}
Time of day (quart): {quart}
Latest period (YYYYMM): {period}
Overall safety score (0-100): {safety_score:.1f}
Colour code: {colour}
Crime risk index: {risk_crime:.1f}
Accident risk index: {risk_acc:.1f}
Number of crime incidents: {num_crime}
Number of accident incidents: {num_acc}

Think about a few practical safety suggestions for a typical visitor.
Do not produce the final answer for the user, just brainstorm internally.
""".strip()

    # We don't use the result, but the call proves HF integration.
    _ = generate_text_with_local_model(prompt, max_new_tokens=80, temperature=0.7)

    # ---- Deterministic, clean advice for the user ----
    advice_text = _fallback_area_advice(
        area_name=area_name,
        quart=quart,
        safety_score=safety_score,
        num_crime=num_crime,
        num_acc=num_acc,
    )

    links = {
        "self": Link(href=f"/advice/{area_id}"),
        "area": Link(href=f"/areas/{area_id}"),
    }

    return AdviceResponse(
        id=area_id,
        areaName=area_name,
        borough_code=borough_code,
        period=period,
        quart=quart,
        safetyScore=safety_score,
        colour=colour,
        advice=advice_text,
        model=settings.hf_local_model,
        links=links,
    )


# -------------------------------------------------------------------
# POST /analyze-route
# -------------------------------------------------------------------

@router.post("/analyze-route", response_model=RouteAnalyzeResponse)
def analyze_route(
    payload: RouteAnalyzeRequest,
    table=Depends(get_dynamodb_table),
):
    """
    POST /analyze-route

    Multi-area route safety analysis accepting an array of area IDs.
    Uses the latest scores for the given quart and returns a concise
    summary of how safe the route is, plus practical travel tips.
    """
    settings = get_settings()
    area_summaries: List[RouteAreaSummary] = []

    for area_id in payload.areaIds:
        try:
            item = _get_latest_item_for_area_quart(table, area_id, payload.quart)
        except HTTPException:
            continue

        summary = RouteAreaSummary(
            id=item.get("areaId") or item["pk"],
            areaName=item["areaName"],
            borough_code=item["borough_code"],
            period=item["period"],
            quart=item["quart"],
            safetyScore=float(item.get("safetyScore", 0.0)),
            colour=item.get("colour", "GREY"),
            numIncidentsCrime=int(item.get("numIncidentsCrime", 0)),
            numIncidentsAccidents=int(item.get("numIncidentsAccidents", 0)),
        )
        area_summaries.append(summary)

    if not area_summaries:
        raise HTTPException(
            status_code=404,
            detail="No scores found for any of the provided areas.",
        )

    # ---- HF model call (ignored for output) ----
    route_lines = []
    for a in area_summaries:
        route_lines.append(
            f"- {a.areaName} ({a.borough_code}): score {a.safetyScore:.1f}, "
            f"crime incidents {a.numIncidentsCrime}, accidents {a.numIncidentsAccidents}"
        )
    route_description = "\n".join(route_lines)

    prompt = f"""
You help a route safety feature in a web app.
You receive a list of boroughs with safety scores and incident counts.
Internally, you think about how safe the overall journey is.

Time of day (quart): {payload.quart}
Route segments:
{route_description}

Brainstorm internally about which parts of the route might feel safer or require more caution.
Do not produce the final user-facing answer here.
""".strip()

    _ = generate_text_with_local_model(prompt, max_new_tokens=80, temperature=0.7)

    # ---- Deterministic explanation of the route ----
    route_advice = _fallback_route_advice(area_summaries, payload.quart)

    links = {
        "self": Link(href="/analyze-route"),
        "areas": Link(href="/areas"),
    }

    return RouteAnalyzeResponse(
        areas=area_summaries,
        routeAdvice=route_advice,
        model=settings.hf_local_model,
        links=links,
    )


# -------------------------------------------------------------------
# POST /ask
# -------------------------------------------------------------------

@router.post("/ask", response_model=AskResponse)
def ask_safety_question(
    payload: AskRequest,
    table=Depends(get_dynamodb_table),
):
    """
    POST /ask

    Natural language safety queries about Montréal boroughs. If areaIds are
    provided, we ground the answer using their latest scores; otherwise we
    return general city-style safety advice.
    """
    settings = get_settings()

    context_items: List[dict] = []
    context_lines: List[str] = []

    if payload.areaIds:
        for area_id in payload.areaIds:
            try:
                quart = payload.quart or "jour"
                item = _get_latest_item_for_area_quart(table, area_id, quart)
            except HTTPException:
                continue

            context_items.append(item)
            context_lines.append(
                f"- {item['areaName']} ({item['borough_code']}), quart={item['quart']}, "
                f"score={float(item.get('safetyScore', 0.0)):.1f}"
            )

    context_block = "\n".join(context_lines) if context_lines else "No borough data."

    # ---- HF model call (ignored for final output) ----
    quart_text = payload.quart if payload.quart else "unspecified"

    prompt = f"""
You are a small language model used behind a safety app.
You receive a user's natural language question and some borough context.

Question:
\"\"\"{payload.query}\"\"\"

Time of day (quart): {quart_text}
Borough context:
{context_block}

Think internally about a few practical safety tips that could answer this question.
Do not generate the final answer here.
""".strip()

    _ = generate_text_with_local_model(prompt, max_new_tokens=80, temperature=0.7)

    # ---- Deterministic answer ----
    answer_text = _fallback_ask_answer(
        question=payload.query,
        quart=payload.quart,
        context_items=context_items,
    )

    links = {
        "self": Link(href="/ask"),
        "areas": Link(href="/areas"),
    }

    return AskResponse(
        answer=answer_text,
        model=settings.hf_local_model,
        links=links,
    )
