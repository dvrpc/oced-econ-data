"""
Fetch employment by industry data (CES) from BLS's API.

If --csv is passed to the program (python3 industry_employment.py --csv), it will create a CSV of
the fetched data. Otherwise, it will insert it into the database specified in the PG_CREDS
variable in config.py.
"""

import argparse
import csv
from datetime import date
import json
from pathlib import Path
import sys
from typing import List

import psycopg
import requests

from config import BLS_API_KEY


parser = argparse.ArgumentParser()
parser.add_argument("--csv", action="store_true")
args = parser.parse_args()

# we don't need a database connection if just trying to create a CSV
if args.csv:
    PG_CREDS = ""
else:
    from config import PG_CREDS


trenton = "SMU3445940"
philadelphia = "SMU4237980"

industries = {
    "0000000001": "Total Nonfarm",
    "1500000001": "Mining, Logging, and Construction",
    "3000000001": "Manufacturing",
    "4000000001": "Trade, Transportation, and Utilities",
    "5000000001": "Information",
    "5500000001": "Financial Activities",
    "6000000001": "Professional and Business Services",
    "6500000001": "Education and Health Services",
    "7000000001": "Leisure and Hospitality",
    "8000000001": "Other Services",
    "9000000001": "Government",
}

series: List = []
for industry in industries.keys():
    series.append(trenton + industry)
    series.append(philadelphia + industry)

# Get data from API
headers = {"Content-type": "application/json"}
data = json.dumps(
    {
        "seriesid": series,
        "startyear": "2000",
        "endyear": "2022",
        "registrationkey": BLS_API_KEY,
    }
)

p = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers)
if p.status_code != 200:
    sys.exit(f"Unable to fetch data from BLS API.")

json_data = json.loads(p.text)

cleaned_data = []

for series in json_data["Results"]["series"]:
    if series["seriesID"][:10] == trenton:
        area = "Trenton MSA"
    if series["seriesID"][:10] == philadelphia:
        area = "Philadelphia MSA"
    industry = industries[series["seriesID"][10:]]

    for record in series["data"]:
        period = date.fromisoformat(record["year"] + "-" + (str(record["period"][1:]) + "-01"))

        # determine if preliminary data
        for each in record["footnotes"]:
            preliminary = True if each.get("code") == "P" else False

        cleaned_data.append(
            {
                "period": period,
                "area": area,
                "industry": industry,
                "jobs": record["value"],
                "preliminary": preliminary,
            }
        )

for record in cleaned_data:
    one_year_ago = date(record["period"].year - 1, record["period"].month, record["period"].day)
    two_years_ago = date(record["period"].year - 2, record["period"].month, record["period"].day)
    # get previous years' jobs, or None if not available (before start of data)
    one_year_ago_jobs = next(
        (
            item["jobs"]
            for item in cleaned_data
            if item["period"] == one_year_ago
            and item["area"] == record["area"]
            and item["industry"] == record["industry"]
        ),
        None,
    )
    two_years_ago_jobs = next(
        (
            item["jobs"]
            for item in cleaned_data
            if item["period"] == two_years_ago
            and item["area"] == record["area"]
            and item["industry"] == record["industry"]
        ),
        None,
    )
    if type(one_year_ago_jobs) == type(None):
        record["change1year"] = None
        record["percentchange1year"] = None
    else:
        one_year_ago_jobs = float(one_year_ago_jobs)
        change1year = float(record["jobs"]) - one_year_ago_jobs
        percentchange1year = (change1year / one_year_ago_jobs) * 100
        record["change1year"] = round(change1year, 2)
        record["percentchange1year"] = round(percentchange1year, 1)
    if type(two_years_ago_jobs) == type(None):
        record["change2year"] = None
        record["percentchange2year"] = None
    else:
        two_years_ago_jobs = float(two_years_ago_jobs)
        change2year = float(record["jobs"]) - two_years_ago_jobs
        percentchange2year = (change2year / two_years_ago_jobs) * 100
        record["change2year"] = round(change2year, 2)
        record["percentchange2year"] = round(percentchange2year, 1)

# enter into db or create CSV
if not args.csv:
    try:
        with psycopg.connect(PG_CREDS) as conn:
            for record in cleaned_data:
                # Insert new record or update rate/prelim if data is no longer preliminary.
                # Further explanation:
                # If a conflict on PERIOD and AREA (i.e. already a record for that
                # period/area), then update the values for RATE and PRELIMINARY
                # **if and only if**
                # previous value for PRELIMINARY (employment_by_industry.preliminary) was true
                # and current value for PRELIMINARY (excluded.preliminary) is false
                conn.execute(
                    """
                    INSERT INTO employment_by_industry
                        (   period,
                            area,
                            industry,
                            number,
                            change1year,
                            percentchange1year,
                            change2year,
                            percentchange2year,
                            preliminary
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (period, area, industry)
                    DO UPDATE
                    SET
                        number = %s,
                        change1year = %s,
                        percentchange1year = %s,
                        change2year = %s,
                        percentchange2year = %s,
                        preliminary = 'f'
                    WHERE
                        employment_by_industry.preliminary = 't' AND
                        excluded.preliminary = 'f'
                """,
                    (
                        record["period"],
                        record["area"],
                        record["industry"],
                        record["jobs"],
                        record["change1year"],
                        record["percentchange1year"],
                        record["change2year"],
                        record["percentchange2year"],
                        record["preliminary"],
                        record["jobs"],
                        record["change1year"],
                        record["percentchange1year"],
                        record["change2year"],
                        record["percentchange2year"],
                    ),
                )
    except psycopg.OperationalError:
        sys.exit("Database error.")
else:
    results_dir = "results"
    try:
        Path(results_dir).mkdir()
    except FileExistsError:
        pass

    with open("results/industry_employment.csv", "w", newline="") as f:
        fieldnames = [
            "period",
            "area",
            "industry",
            "jobs",
            "change1year",
            "percentchange1year",
            "change2year",
            "percentchange2year",
            "preliminary",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # customize header row to be more informative
        header = [
            "period",
            "area",
            "industry",
            "jobs (thousands)",
            "change (thousands, from 1 year ago)",
            "change (%, from 1 year ago)",
            "change (thousands, from 2 years ago)",
            "change (%, from 2 years ago)",
            "preliminary data",
        ]
        writer.writerow(dict(zip(fieldnames, header)))
        writer.writerows(cleaned_data)

    print("CSV created in results/ directory.")
