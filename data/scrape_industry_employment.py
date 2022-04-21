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

# print each row of the table body
for i, row in enumerate(table_body.find_all("tr")):
    # data grouped into rows of three; determine where we are within each set of 3
    if i % 3 == 0:  # first row (industry name)
        industry = row.th.string

    if i % 3 == 1:  # second row (first year of data)
        year = row.th.string
        print(i, f"first year data ({year}) for {industry}")

    if i % 3 == 2:  # third row (second year of data)
        year = row.th.string
        print(i, f"second year data ({year}) for {industry}")

    cells = row.find_all("td")
    for cell in cells[0:12]:  # last cell is annual average, don't include it
        # some cells are empty (single space), so use .text (unicode) rather than .string,
        # which gives us ability to use strip() (and also includes full text for
        # data that is noted to be preliminary)
        if cell.text.strip():
            print(cell.text)
