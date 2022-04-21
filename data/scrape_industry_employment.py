import sys

from bs4 import BeautifulSoup
import requests

url_to_scrape = "https://www.bls.gov/regions/mid-atlantic/data/xg-tables/ro3fx9527.htm"

r = requests.get(url_to_scrape)

if r.status_code != 200:
    sys.exit(f"Unable to get {url_to_scrape}")

soup = BeautifulSoup(r.text, features="html.parser")

# there are two tables on the page, get the one we want by id
table = soup.find("table", id="ro3fx9527")

# get the table body (ignore header rows at top and notes at bottom)
table_body = table.find("tbody")

# print each row of the table
for i, row in enumerate(table_body.find_all("tr")):
    # data grouped into rows of three; determine where we are within each set of 3
    if i % 3 == 0:  # first row (industry name)
        industry = row.th.string

    if i % 3 == 1:  # second row (first year of data)
        year = row.th.string
        print(i, f"first year data ({year}) for {industry}")

    if i % 3 == 2:  # hird row (second year of data)
        year = row.th.string
        print(i, f"second year data ({year}) for {industry}")
