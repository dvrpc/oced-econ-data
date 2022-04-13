from datetime import date
from typing import List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
import psycopg
from pydantic import BaseModel

from config import PG_CREDS


class UnemploymentRateResponse(BaseModel):
    period: date
    rate: float
    area: str


class Error(BaseModel):
    message: str


areas = ["United States", "DVRPC Region", "Philadelphia MSA", "Trenton MSA"]


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="DVRPC Economic Data API",
        version="1.0",
        description=("TK"),
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app = FastAPI(openapi_url="/api/econ-data/v1/openapi.json", docs_url="/api/econ-data/v1/docs")
app.openapi = custom_openapi
responses = {
    400: {"model": Error, "description": "Bad Request"},
    404: {"model": Error, "description": "Not Found"},
    500: {"model": Error, "description": "Internal Server Error"},
}
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get(
    "/api/econ-data/v1/unemployment",
    response_model=List[UnemploymentRateResponse],
    responses=responses,
)
def unemployment_rate(
    area: Optional[str] = None, start_year: Optional[int] = None, end_year: Optional[int] = None
):
    """Get the unemployment rate for the United States, Philadelphia MSA, and Trenton MSA."""

    # build query, starting with base (all items), and then limit by query params
    query = "SELECT * FROM unemployment_rate"
    q_modifiers = []

    if area:
        if area not in areas:
            message = "Please enter a valid area. Must be one of: " + ", ".join(areas)
            return JSONResponse(status_code=400, content={"message": message})
        q_modifiers.append("area = '" + area + "'")

    if start_year and end_year:
        if end_year < start_year:
            message = "end_year must be after start_year"
            return JSONResponse(status_code=400, content={"message": message})

    # we don't need to validate that year is an int b/c of coercion by pydantic into int
    # (FastAPI will handle this error), but we do need to convert it back to a string
    if start_year:
        q_modifiers.append("date_part('year', period) >= '" + str(start_year) + "'")

    if end_year:
        q_modifiers.append("date_part('year', period) <= '" + str(end_year) + "'")

    if q_modifiers:
        query += " WHERE " + " AND ".join(q_modifiers)

    query += " ORDER BY period, area ASC"

    try:
        with psycopg.connect(PG_CREDS) as conn:
            result = conn.execute(query).fetchall()
    except psycopg.OperationalError:
        return JSONResponse(
            status_code=500,
            content={"message": "Database error."},
        )

    if not result:
        return JSONResponse(
            status_code=404,
            content={"message": "No data available."},
        )

    ur = []
    for row in result:
        ur.append({"period": row[0], "rate": row[1], "area": row[2]})
    return ur
