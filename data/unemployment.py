"""
Fetch unemployment data (CPS) from BLS's API.

If --csv is passed to the program (python3 unemployment.py --csv), it will create a CSV of
the fetched data. Otherwise, it will insert it into the database specified in the PG_CREDS
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

us = "LNS14000000"
philadelphia = "LAUMT423798000000003"
trenton = "LAUMT344594000000003"

# Get data from API
headers = {"Content-type": "application/json"}
data = json.dumps(
    {
        "seriesid": [us, philadelphia, trenton],
        "startyear": "2000",
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
            if series["seriesID"] == trenton:
                area = "Trenton MSA"

            for record in series["data"]:
                period = date.fromisoformat(
                    record["year"] + "-" + (str(record["period"][1:]) + "-01")
                )

                # determine if preliminary data
                for each in record["footnotes"]:
                    preliminary = True if each.get("code") == "P" else False

                if args.csv:
                    cleaned_data.append([period, area, record["value"], preliminary])
                else:
                    # Insert new record or update rate/prelim if data is no longer preliminary.
                    # Further explanation:
                    # If a conflict on PERIOD and AREA (i.e. already a record for that
                    # period/area), then update the values for RATE and PRELIMINARY
                    # **if and only if**
                    # the previous value for PRELIMINARY (unemployment_rate.preliminary) was true
                    # and current value for PRELIMINARY (excluded.preliminary) is false
                    conn.execute(
                        """
                        INSERT INTO unemployment_rate
                        (period, area, rate, preliminary)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (period, area)
                        DO UPDATE
                        SET
                            rate = %s,
                            preliminary = 'f'
                        WHERE
                            unemployment_rate.preliminary = 't' AND
                            excluded.preliminary = 'f'
                    """,
                        (
                            period,
                            area,
                            record["value"],
                            preliminary,
                            record["value"],
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

    with open(results_dir + "/unemployment.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["period", "area", "rate", "preliminary data"])
        writer.writerows(cleaned_data)

    print("CSV created in results/ directory.")
