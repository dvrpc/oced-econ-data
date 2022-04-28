import csv
from datetime import date
import sys

from bs4 import BeautifulSoup
import requests

url_to_scrape = "https://www.bls.gov/regions/mid-atlantic/data/xg-tables/ro3fx9527.htm"

r = requests.get(url_to_scrape)

if r.status_code != 200:
    sys.exit(f"Unable to get {url_to_scrape}")

soup = BeautifulSoup(r.text, features="html.parser")

# two tables on page; get the one we want by id, and then its body (ignore headers and notes)
table_body = soup.find("table", id="ro3fx9527").find("tbody")

# a simple and incomplete check that the structure hasn't changed
expected_row_count = 33
actual_row_count = len(table_body.find_all("tr"))
if actual_row_count != expected_row_count:
    sys.exit(f"Expected row count of {expected_row_count}, but got {actual_row_count}. Exiting.")

data = []

# loop over all rows and populate our data list
for i, row in enumerate(table_body.find_all("tr")):
    # data grouped into rows of three; determine where we are within each set of 3
    if i % 3 == 0:  # first row (industry name)
        industry = row.th.string
        industry = industry.replace("&", "and")

    if i % 3 == 1:  # second row (first year of data)
        year = row.th.string

    if i % 3 == 2:  # third row (second year of data)
        year = row.th.string

    cells = row.find_all("td")
    for j, cell in enumerate(cells[0:12]):  # last cell is annual average, don't include it
        # some cells are empty (single space), so use .text (unicode) rather than .string,
        # which gives us ability to use strip() (and also includes full text for
        # data that is noted to be preliminary)
        if cell.text.strip():
            # remove indication that this is preliminary data
            value = cell.text.lstrip("(p)")
            # remove any commas
            value = value.replace(",", "")
            # create date
            data_date = date.fromisoformat(year + "-" + str(j + 1).zfill(2) + "-01")
            # add it
            data.append([data_date, value, industry, "Philadelphia MSA"])

with open("results/industry_employment_from_scraping.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["period", "value", "industry", "area"])
    writer.writerows(data)
