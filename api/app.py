import calendar
from datetime import date
from typing import Dict, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
import pandas as pd
import psycopg
from pydantic import BaseModel

from config import PG_CREDS


class RateResponse(BaseModel):
    period: date
    rate: float
    area: str


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
) -> List[RateResponse]:
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
        item = {"period": row[0], "rate": row[1], "area": row[2]}
        data.append(RateResponse(**item))
    return data


def get_recent_matching_data(table: str, years: int = None) -> List[RateResponse]:
    """
    Get data only when it exists for all series per period.

    For instance, for inflation, there is data every month for the U.S., but only every
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
    #   inflation = bimonthly for 2 series, so 12 = 1 year of data
    #   unemployment = monthly for 3 series, so 36 = 1 year of data
    if table == "inflation_rate":
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
        item = {"period": row[0], "rate": row[1], "area": row[2]}
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
    "/api/econ-data/v1/inflation",
    response_model=List[RateResponse],
    responses=responses,
)
def inflation_rate(
    area: Optional[str] = None, start_year: Optional[int] = None, end_year: Optional[int] = None
):
    """
    Get the inflation rate (CPI, all urban consumers) for the United States and Philadelphia
    MSA. (Trenton MSA is not included in the BLS survey from which this data comes.)
    """
    try:
        data = get_data("inflation_rate", area, start_year, end_year)
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
    Get the most recent unemployment rate for the United States, Philadelphia MSA, and Trenton MSA where data is available for all areas.
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
    "/api/econ-data/v1/inflation-recent",
    response_model=List[RateResponse],
    responses=responses,
)
def recent_inflation_rates(years: Optional[int] = None):
    """
    Get the most recent inflation rate (CPI, all urban consumers) for the United States and
    Philadelphia MSA where data is available for both areas. (Trenton MSA is not included in the
    BLS survey from which this data comes.)
    """
    try:
        data = get_recent_matching_data("inflation_rate", years)
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
    Get the most recent employment, and year-over-year percentage change, by industry for the
    Philaladelphia MSA.
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
        data.append({"period": row[0], "number": row[1], "industry": row[2], "area": row[3]})

    # get most recent period and one year before that
    most_recent_period = data[0]["period"]
    one_year_ago = date(
        most_recent_period.year - 1, most_recent_period.month, most_recent_period.day
    )

    # create pandas dataframe to make it easier to do calculations
    df = pd.DataFrame(data)

    # create a dataframe for the data for each of these periods
    most_recent_df = df[df["period"] == most_recent_period]
    year_ago_df = df[df["period"] == one_year_ago]

    # combine them so we have each period/data in same row, matched on industry/area,
    # and then set industry as index and add columns for absolute and percentage change
    combined = most_recent_df.merge(year_ago_df, on=["industry", "area"], how="left")
    combined.set_index("industry", inplace=True)
    combined["percent change"] = (combined["number_x"] - combined["number_y"]) / combined[
        "number_y"
    ]
    combined["abs_change"] = combined["number_x"] - combined["number_y"]

    human_most_recent = (
        calendar.month_abbr[most_recent_period.month] + " " + str(most_recent_period.year)
    )
    summary_data = {
        human_most_recent: {
            "Mining, Logging, and Construction": {
                "Philadelphia MSA": {
                    "number": combined.loc["Mining, Logging, and Construction"]["number_x"],
                    "absolute_change": combined.loc["Mining, Logging, and Construction"][
                        "abs_change"
                    ],
                    "percent_change": combined.loc["Mining, Logging, and Construction"][
                        "percent change"
                    ],
                }
            },
            "Manufacturing": {
                "Philadelphia MSA": {
                    "number": combined.loc["Manufacturing"]["number_x"],
                    "absolute_change": combined.loc["Manufacturing"]["abs_change"],
                    "percent_change": combined.loc["Manufacturing"]["percent change"],
                }
            },
            "Trade, Transportation, and Utilities": {
                "Philadelphia MSA": {
                    "number": combined.loc["Trade, Transportation, and Utilities"]["number_x"],
                    "absolute_change": combined.loc["Trade, Transportation, and Utilities"][
                        "abs_change"
                    ],
                    "percent_change": combined.loc["Trade, Transportation, and Utilities"][
                        "percent change"
                    ],
                }
            },
            "Information": {
                "Philadelphia MSA": {
                    "number": combined.loc["Information"]["number_x"],
                    "absolute_change": combined.loc["Information"]["abs_change"],
                    "percent_change": combined.loc["Information"]["percent change"],
                }
            },
            "Financial Activities": {
                "Philadelphia MSA": {
                    "number": combined.loc["Financial Activities"]["number_x"],
                    "absolute_change": combined.loc["Financial Activities"]["abs_change"],
                    "percent_change": combined.loc["Financial Activities"]["percent change"],
                }
            },
            "Professional and Business Services": {
                "Philadelphia MSA": {
                    "number": combined.loc["Professional and Business Services"]["number_x"],
                    "absolute_change": combined.loc["Professional and Business Services"][
                        "abs_change"
                    ],
                    "percent_change": combined.loc["Professional and Business Services"][
                        "percent change"
                    ],
                }
            },
            "Educational and Health Services": {
                "Philadelphia MSA": {
                    "number": combined.loc["Educational and Health Services"]["number_x"],
                    "absolute_change": combined.loc["Educational and Health Services"][
                        "abs_change"
                    ],
                    "percent_change": combined.loc["Educational and Health Services"][
                        "percent change"
                    ],
                }
            },
            "Leisure and Hospitality": {
                "Philadelphia MSA": {
                    "number": combined.loc["Leisure and Hospitality"]["number_x"],
                    "absolute_change": combined.loc["Leisure and Hospitality"]["abs_change"],
                    "percent_change": combined.loc["Leisure and Hospitality"]["percent change"],
                }
            },
            "Other Services": {
                "Philadelphia MSA": {
                    "number": combined.loc["Other Services"]["number_x"],
                    "absolute_change": combined.loc["Other Services"]["abs_change"],
                    "percent_change": combined.loc["Other Services"]["percent change"],
                }
            },
            "Government": {
                "Philadelphia MSA": {
                    "number": combined.loc["Government"]["number_x"],
                    "absolute_change": combined.loc["Government"]["abs_change"],
                    "percent_change": combined.loc["Government"]["percent change"],
                }
            },
            "Total Nonfarm": {
                "Philadelphia MSA": {
                    "number": combined.loc["Total Nonfarm"]["number_x"],
                    "absolute_change": combined.loc["Total Nonfarm"]["abs_change"],
                    "percent_change": combined.loc["Total Nonfarm"]["percent change"],
                }
            },
        }
    }
    return summary_data
