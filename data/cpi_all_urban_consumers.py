"""
Fetch inflation data (CPI) from BLS's API.

If --csv is passed to the program (python3 cpi_all_urban_consumers.py --csv), it will create a
CSV of the fetched data. Otherwise, it will insert it into the database specified in the PG_CREDS
variable in config.py.
"""

import argparse
import csv
from datetime import date
import json
from pathlib import Path
import sys

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

us = "CUUR0000SA0"
philadelphia = "CUURS12BSA0"

# Get data from API
headers = {"Content-type": "application/json"}
data = json.dumps(
    {
        "seriesid": [us, philadelphia],
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
            if series["seriesID"] == us:
                area = "United States"
            if series["seriesID"] == philadelphia:
                area = "Philadelphia MSA"

            for record in series["data"]:
                period = date.fromisoformat(
                    record["year"] + "-" + (str(record["period"][1:]) + "-01")
                )

                if args.csv:
                    cleaned_data.append([period, record["value"], area])
                else:
                    conn.execute(
                        """
                        INSERT INTO inflation_rate
                        (period, rate, area)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                        (
                            period,
                            record["value"],
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

    with open(results_dir + "/cpi.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "value", "area"])
        writer.writerows(cleaned_data)

    print("CSV created in results/ directory.")
