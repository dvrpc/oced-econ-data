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
        "startyear": "2013",
        "endyear": "2022",
        "registrationkey": BLS_API_KEY,
    }
)

p = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers)
if p.status_code != 200:
    sys.exit(f"Unable to fetch data from BLS API.")

json_data = json.loads(p.text)

if args.csv:
    cleaned_data = []

# go through results and either add to db or (for --csv), put into list for later
try:
    with psycopg.connect(PG_CREDS) as conn:
        for series in json_data["Results"]["series"]:
            if series["seriesID"][:10] == trenton:
                area = "Trenton MSA"
            if series["seriesID"][:10] == philadelphia:
                area = "Philadelphia MSA"
            industry = industries[series["seriesID"][10:]]

            for record in series["data"]:
                period = date.fromisoformat(
                    record["year"] + "-" + (str(record["period"][1:]) + "-01")
                )

                if args.csv:
                    cleaned_data.append([period, record["value"], industry, area])
                else:
                    conn.execute(
                        """
                        INSERT INTO employment_by_industry
                        (period, number, industry, area)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                        (
                            period,
                            record["value"],
                            industry,
                            area,
                        ),
                    )
except psycopg.OperationalError:
    sys.exit("Database error.")

if args.csv:
    results_dir = "results"
    try:
        Path(results_dir).mkdir()
    except FileExistsError:
        pass

    with open("results/industry_employment.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["period", "value", "industry", "area"])
        writer.writerows(cleaned_data)

    print("CSV created in results/ directory.")
