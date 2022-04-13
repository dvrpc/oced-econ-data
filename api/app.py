from datetime import date
from typing import List

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
def unemployment_rate():
    """Get the unemployment rate for the United States, Philadelphia MSA, and Trenton MSA."""
    try:
        with psycopg.connect(PG_CREDS) as conn:
            result = conn.execute("SELECT * FROM unemployment_rate").fetchall()
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
