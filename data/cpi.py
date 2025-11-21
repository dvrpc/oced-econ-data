"""
Fetch CPI (all urban consumers) data from BLS's API.

The base period for the index is 1982-84 (= 100).

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
        "registrationkey": BLS_API_KEY,
    }
)
p = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers)
if p.status_code != 200:
    sys.exit(f"Unable to fetch data from BLS API.")

json_data = json.loads(p.text)

data = []

# create list of dictionaries from data
# do this an intermediary step so we can then calculate year-over-year rates
for series in json_data["Results"]["series"]:
    if series["seriesID"] == us:
        area = "United States"
    if series["seriesID"] == philadelphia:
        area = "Philadelphia MSA"

    for record in series["data"]:
        period = date.fromisoformat(record["year"] + "-" + (str(record["period"][1:]) + "-01"))

        # determine if preliminary data
        for each in record["footnotes"]:
            preliminary = True if each.get("code") == "P" else False

        data.append(
            {"period": period, "area": area, "index": record["value"], "preliminary": preliminary}
        )

# calculate and add the year-over-year percentage change
for record in data:
    previous_year_period = date(
        record["period"].year - 1, record["period"].month, record["period"].day
    )
    # get previous year's index, or None if not available (before start of data)
    year_ago_index = next(
        (
            item["index"]
            for item in data
            if item["period"] == previous_year_period and item["area"] == record["area"]
        ),
        None,
    )
    try:
        rate = ((float(record["index"]) - float(year_ago_index)) / float(year_ago_index)) * 100
        record["rate_yoy"] = round(rate, 2)
    except TypeError:
        record["rate_yoy"] = None


# go through results and either add to db or (for --csv), put into list for later
if not args.csv:
    try:
        with psycopg.connect(PG_CREDS) as conn:
            for record in data:
                # Insert new record or update idx/prelim if data is no longer preliminary.
                # Further explanation:
                # If a conflict on PERIOD and AREA (i.e. already a record for that
                # period/area), then update the values for IDX and PRELIMINARY
                # **if and only if**
                # the previous value for PRELIMINARY (cpi.preliminary) was true
                # and current value for PRELIMINARY (excluded.preliminary) is false
                conn.execute(
                    """
                            INSERT INTO cpi
                            (period, area, idx, rate, preliminary)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (period, area)
                            DO UPDATE
                            SET
                                idx = %s,
                                rate = %s,
                                preliminary = 'f'
                            WHERE
                                cpi.preliminary = 't' AND
                                excluded.preliminary = 'f'
                        """,
                    (
                        record["period"],
                        record["area"],
                        record["index"],
                        record["rate_yoy"],
                        record["preliminary"],
                        record["index"],
                        record["rate_yoy"],
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

    with open(results_dir + "/cpi.csv", "w", newline="") as f:
        fieldnames = ["period", "area", "index", "rate_yoy", "preliminary"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # customize header row to be more informative
        header = [
            "period",
            "area",
            "index (1982-84=100)",
            "year-over-year percentage change",
            "preliminary data",
        ]
        writer.writerow(dict(zip(fieldnames, header)))
        writer.writerows(data)

    print("CSV created in results/ directory.")
