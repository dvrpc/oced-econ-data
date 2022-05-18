import calendar
from datetime import date
from itertools import groupby
from operator import itemgetter
from typing import Dict, List, Optional, Union

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
import psycopg
from pydantic import BaseModel

from config import PG_CREDS


class RateResponse(BaseModel):
    period: date
    area: str
    rate: float


class IndexRateResponse(BaseModel):
    period: date
    area: str
    index: float
    rate: Union[float, None]


class UnitsResponse(BaseModel):
    period: date
    units: int


class Error(BaseModel):
    message: str


class EconDataError(BaseException):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message


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


areas = ["United States", "DVRPC Region", "Philadelphia MSA", "Trenton MSA"]


def get_data(
    table: str, area: str = None, start_year: int = None, end_year: int = None
) -> List[Union[RateResponse, IndexRateResponse, UnitsResponse]]:
    """Get data from *table*, with optional query parameters."""
    # build query, starting with base (all items), and then limit by query params
    query = "SELECT * FROM " + table
    q_modifiers = []

    if area:
        if area not in areas:
            message = "Please enter a valid area. Must be one of: " + ", ".join(areas)
            raise EconDataError(400, message)
        q_modifiers.append("area = '" + area + "'")

    if start_year and end_year:
        if end_year < start_year:
            message = "end_year must be after start_year"
            raise EconDataError(400, message)

    # we don't need to validate that year is an int b/c of coercion by pydantic into int
    # (FastAPI will handle this error), but we do need to convert it back to a string
    if start_year:
        q_modifiers.append("date_part('year', period) >= '" + str(start_year) + "'")

    if end_year:
        q_modifiers.append("date_part('year', period) <= '" + str(end_year) + "'")

    if q_modifiers:
        query += " WHERE " + " AND ".join(q_modifiers)

    if table in ["cpi", "unemployment_rate"]:
        query += " ORDER BY period, area ASC"

    try:
        with psycopg.connect(PG_CREDS) as conn:
            result = conn.execute(query).fetchall()
    except psycopg.OperationalError:
        raise EconDataError(500, "Database error")

    if not result:
        raise EconDataError(404, "No data available for given criteria.")

    data = []
    for row in result:
        if table == "cpi":
            item = {"period": row[0], "area": row[1], "index": row[2], "rate": row[3]}
            data.append(IndexRateResponse(**item))
        elif table == "unemployment_rate":
            item = {"period": row[0], "area": row[1], "rate": row[2]}
            data.append(RateResponse(**item))
        elif table == "housing":
            item = {"period": row[0], "units": row[1]}
            data.append(UnitsResponse(**item))
    return data


def get_recent_matching_data(
    table: str, years: int = None
) -> List[Union[RateResponse, IndexRateResponse]]:
    """
    Get data only when it exists for all series per period.

    For instance, for the CPI, there is data every month for the U.S., but only every
    two months for Philadelphia. get_data() would return two months of U.S. data per month
    of Philadelphipa data, whereas this function only returns the months were data is
    available for both the U.S. and Philadelphia.

    A similar situation occurs because local data lags national data, so get_data() will often
    return an additional leading month of national data compared to local data.

    If *years* isn't provided, default to returning 1 year of data.
    """

    if not years:
        years = 1

    # set vars per table
    # count: number of series in table (for subquery that gets only data that has *count*
    #   items per period (i.e., if less than this, data not available for all series))
    # periods:
    #   CPI = bimonthly for 2 series, so 12 = 1 year of data
    #   unemployment = monthly for 3 series, so 36 = 1 year of data
    if table == "cpi":
        count = 2
        periods = years * 12

    if table == "unemployment_rate":
        count = 3
        periods = years * 36

    query = f"""
        SELECT * FROM {table}
        WHERE period IN
            (SELECT period FROM {table}
                GROUP BY period
                HAVING COUNT(area) = {count}
            )
        ORDER BY period DESC, area ASC
        LIMIT {periods}
    """

    try:
        with psycopg.connect(PG_CREDS) as conn:
            result = conn.execute(query).fetchall()
    except psycopg.OperationalError:
        raise EconDataError(500, "Database error")

    if not result:
        raise EconDataError(404, "No data available for given criteria.")

    data = []
    for row in result:
        if table == "cpi":
            item = {"period": row[0], "area": row[1], "index": row[2], "rate": row[3]}
            data.append(IndexRateResponse(**item))
        elif table == "unemployment_rate":
            item = {"period": row[0], "area": row[1], "rate": row[2]}
            data.append(RateResponse(**item))
    return data


@app.get(
    "/api/econ-data/v1/unemployment",
    response_model=List[RateResponse],
    responses=responses,
)
def unemployment_rate(
    area: Optional[str] = None, start_year: Optional[int] = None, end_year: Optional[int] = None
):
    """Get the unemployment rate for the United States, Philadelphia MSA, and Trenton MSA."""
    try:
        data = get_data("unemployment_rate", area, start_year, end_year)
    except EconDataError as e:
        return JSONResponse(
            status_code=e.status_code,
            content={"message": e.message},
        )
    return data


@app.get(
    "/api/econ-data/v1/cpi",
    response_model=List[IndexRateResponse],
    responses=responses,
)
def cpi(
    area: Optional[str] = None, start_year: Optional[int] = None, end_year: Optional[int] = None
):
    """
    Get the CPI for All Urban Consumers index (1982-84=100) and year-over-year percentage change
    for the United States and Philadelphia MSA. (Trenton MSA is not included in the BLS survey
    from which this data comes.)
    """
    try:
        data = get_data("cpi", area, start_year, end_year)
    except EconDataError as e:
        return JSONResponse(
            status_code=e.status_code,
            content={"message": e.message},
        )
    return data


@app.get(
    "/api/econ-data/v1/unemployment-recent",
    response_model=List[RateResponse],
    responses=responses,
)
def recent_unemployment_rates(years: Optional[int] = None):
    """
    Get the most recent unemployment rate for the United States, Philadelphia MSA, and Trenton MSA
    where data is available for all areas.
    """
    try:
        data = get_recent_matching_data("unemployment_rate", years)
    except EconDataError as e:
        return JSONResponse(
            status_code=e.status_code,
            content={"message": e.message},
        )
    return data


@app.get(
    "/api/econ-data/v1/cpi-recent",
    response_model=List[IndexRateResponse],
    responses=responses,
)
def recent_cpi(years: Optional[int] = None):
    """
    Get the most recent CPI for All Urban Consumers index (1982-84=100) and year-over-year
    percentage change for the United States and Philadelphia MSA, where data is available for both
    areas. (Trenton MSA is not included in the BLS survey from which this data comes.)
    """
    try:
        data = get_recent_matching_data("cpi", years)
    except EconDataError as e:
        return JSONResponse(
            status_code=e.status_code,
            content={"message": e.message},
        )

    return data


@app.get(
    "/api/econ-data/v1/employment-by-industry",
    response_model=Dict,
    responses=responses,
)
def employment_by_industry():
    """
    Get the most recent employment, and 1- and 2-year change/percentage change, by industry
    for the Philaladelphia and Trenton MSAs.
    """
    query = "SELECT * FROM employment_by_industry ORDER BY period DESC, industry ASC"

    try:
        with psycopg.connect(PG_CREDS) as conn:
            result = conn.execute(query).fetchall()
    except psycopg.OperationalError:
        raise EconDataError(500, "Database error")

    if not result:
        raise EconDataError(404, "No data available for given criteria.")

    data = []
    for row in result:
        data.append(
            {
                "period": row[0],
                "area": row[1],
                "industry": row[2],
                "number": row[3],
                "one-year change": row[4],
                "one-year percent change": row[5],
                "two-year change": row[6],
                "two-year percent change": row[7],
            }
        )

    # get most recent period and one year before that; data for just those periods
    most_recent = data[0]["period"]
    year_ago = date(most_recent.year - 1, most_recent.month, most_recent.day)
    most_recent_data = [record for record in data if record["period"] == most_recent]
    year_ago_data = [record for record in data if record["period"] == year_ago]

    # use groupby with itemgetter to reshape the data
    # <https://www.geeksforgeeks.org/group-list-of-dictionary-data-by-particular-key-in-python/>

    most_recent_data_by_industry = {}
    for key, value in groupby(most_recent_data, key=itemgetter("industry")):
        values = list(value)
        area = {}
        area[values[0]["area"]] = {
            "employment": values[0]["number"],
            "one-year change (number)": values[0]["one-year change"],
            "one-year change (percent)": values[0]["one-year percent change"],
            "two-year change (number)": values[0]["two-year change"],
            "two-year change (percent)": values[0]["two-year percent change"],
        }
        area[values[1]["area"]] = {
            "employment": values[1]["number"],
            "one-year change (number)": values[1]["one-year change"],
            "one-year change (percent)": values[1]["one-year percent change"],
            "two-year change (number)": values[1]["two-year change"],
            "two-year change (percent)": values[1]["two-year percent change"],
        }
        most_recent_data_by_industry[key] = area

    year_ago_data_by_industry = {}
    for key, value in groupby(year_ago_data, key=itemgetter("industry")):
        values = list(value)
        area = {}
        area[values[0]["area"]] = {
            "employment": values[0]["number"],
            "one-year change (number)": values[0]["one-year change"],
            "one-year change (percent)": values[0]["one-year percent change"],
            "two-year change (number)": values[0]["two-year change"],
            "two-year change (percent)": values[0]["two-year percent change"],
        }
        area[values[1]["area"]] = {
            "employment": values[1]["number"],
            "one-year change (number)": values[1]["one-year change"],
            "one-year change (percent)": values[1]["one-year percent change"],
            "two-year change (number)": values[1]["two-year change"],
            "two-year change (percent)": values[1]["two-year percent change"],
        }
        year_ago_data_by_industry[key] = area

    most_recent_friendly_date = calendar.month_abbr[most_recent.month] + " " + str(most_recent.year)
    year_ago_friendly_date = calendar.month_abbr[year_ago.month] + " " + str(year_ago.year)

    summary_data = {
        most_recent_friendly_date: most_recent_data_by_industry,
        year_ago_friendly_date: year_ago_data_by_industry,
    }

    return summary_data


@app.get(
    "/api/econ-data/v1/housing",
    response_model=List[UnitsResponse],
    responses=responses,
)
def housing(start_year: Optional[int] = None, end_year: Optional[int] = None):
    """Get the total number of authorized housing units for the DVRPC Region by month."""
    try:
        data = get_data("housing", None, start_year, end_year)
    except EconDataError as e:
        return JSONResponse(
            status_code=e.status_code,
            content={"message": e.message},
        )
    return data
