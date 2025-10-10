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
import logging
from pathlib import Path
import sys

from bs4 import BeautifulSoup
import psycopg
import requests
import urllib3

logger = logging.getLogger()

# Disable warnings about unverified https requests.
urllib3.disable_warnings()

parser = argparse.ArgumentParser()
parser.add_argument("--csv", action="store_true")
args = parser.parse_args()

# We don't need a database connection if just trying to create a CSV.
if args.csv:
    PG_CREDS = ""
else:
    from config import PG_CREDS

# Get the county data filenames.
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

# Limit to last 3 years of files.
county_files = county_files[:36]

# Interate through the files and create a dictionary of date: total.
data = {}
for file in county_files:
    url = "https://www2.census.gov/econ/bps/County/" + file
    with requests.get(url, stream=True, verify=False) as r:
        lines = (line.decode("utf-8") for line in r.iter_lines())
        # Skip the first three lines of the file.
        lines.__next__()
        lines.__next__()
        lines.__next__()

        for row in csv.reader(lines):
            # Only include rows that contain non-DVRPC counties.
            # Ignore data, by line, if there's an error in type or type conversion, but log it.
            if row[1] + row[2] in [
                "34005",
                "34007",
                "34015",
                "34021",
                "42017",
                "42029",
                "42045",
                "42091",
                "42101",
            ]:
                try:
                    data[row[0]] = (
                        data[row[0]] + int(row[7]) + int(row[10]) + int(row[13]) + int(row[16])
                    )
                except KeyError:
                    try:
                        data[row[0]] = int(row[7]) + int(row[10]) + int(row[13]) + int(row[16])
                    except Exception as e:
                        logger.error(f"Error in {file} for {row[0]}, {row[1]}{row[2]}: {e}")
                except Exception as e:
                    logger.error(f"Error in {file} for {row[0]}, {row[1]}{row[2]}: {e}")

# Convert to list, and then the date from YYYY-MM string to proper date.
data = [[key, value] for key, value in data.items()]
for each in data:
    each[0] = date.fromisoformat(each[0][:4] + "-" + each[0][4:] + "-01")
data.reverse()

# Enter into db or create CSV.
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
