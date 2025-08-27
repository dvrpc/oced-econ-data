"""
Combine county-level authorized housing units into region-wide total from monthly data provided
via text files by U.S. Dept. of Census.

If --csv is passed to the program (python3 housing.py --csv), it will create a CSV of
the fetched data. Otherwise, it will insert it into the database specified in the PG_CREDS
variable in config.py.
"""

import argparse
import csv
from datetime import date
from pathlib import Path
import sys

from bs4 import BeautifulSoup
import pandas as pd
import psycopg
import requests


parser = argparse.ArgumentParser()
parser.add_argument("--csv", action="store_true")
args = parser.parse_args()

# we don't need a database connection if just trying to create a CSV
if args.csv:
    PG_CREDS = ""
else:
    from config import PG_CREDS

# get the county data filenames
url_to_scrape = "https://www2.census.gov/econ/bps/County/?C=N;O=D"
r = requests.get(url_to_scrape, verify=False)

if r.status_code != 200:
    sys.exit(f"Unable to get {url_to_scrape}")

soup = BeautifulSoup(r.text, features="html.parser")
table = soup.find("table")

county_files = []

for row in table.find_all("tr"):
    for cell in row:
        if cell.a:
            if cell.a.string.endswith("c.txt"):
                county_files.append(cell.a.string)

county_files = county_files[:36]

header_list = [
    "Date",
    "State_FIPS",
    "County_FIPS",
    "Region_Code",
    "Division_Code",
    "County",
    "buildings_1unit",
    "units_1unit",
    "permitVal_1unit",
    "buildings_2units",
    "units_2units",
    "permitVal_2units",
    "buildings_3to4units",
    "units_3to4units",
    "permitVal_3to4units",
    "buildings_5punits",
    "units_5punits",
    "permitVal_5punits",
    "rep_buildings_1unit",
    "rep_units_1unit",
    "rep_permitVal_1unit",
    "rep_buildings_2units",
    "rep_units_2units",
    "rep_permitVal_2units",
    "rep_buildings_3to4units",
    "rep_units_3to4units",
    "rep_permitVal_3to4units",
    "rep_buildings_5punits",
    "rep_units_5punits",
    "rep_permitVal_5punits",
]

all_counties_df_list = []

for file in county_files:
    all_counties_df_list.append(
        pd.read_csv(
            "https://www2.census.gov/econ/bps/County/" + file,
            dtype={
                "State_FIPS": str,
                "County_FIPS": str,
                "units_1unit": int,
                "units_2units": int,
                "units_3to4units": int,
                "units_5punits": int,
            },
            names=header_list,
            low_memory=False,
            skiprows=[0, 1],
        )
    )

all_counties = pd.concat(all_counties_df_list)
all_counties["FIPS"] = all_counties["State_FIPS"] + all_counties["County_FIPS"]
region = all_counties.loc[
    all_counties["FIPS"].isin(
        ["34005", "34007", "34015", "34021", "42017", "42029", "42045", "42091", "42101"]
    )
]

pd.set_option("mode.chained_assignment", None)  # to ignore the copy warning in next step

region["total_units"] = (
    region["units_1unit"]
    + region["units_2units"]
    + region["units_3to4units"]
    + region["units_5punits"]
)
region = region[["Date", "total_units"]]
region = region.groupby("Date").sum()

data = []
for row in region.itertuples():
    data.append([date.fromisoformat((str(row[0])[:4] + "-" + str(row[0])[4:] + "-01")), row[1]])

# enter into db or create CSV
if not args.csv:
    try:
        with psycopg.connect(PG_CREDS) as conn:
            for record in data:
                conn.execute(
                    """
                        INSERT INTO housing (period, units)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        record[0],
                        record[1],
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

    with open(results_dir + "/housing.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["period", "units"])
        writer.writerows(data)

    print("CSV created in results/ directory.")
