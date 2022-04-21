import sys

from bs4 import BeautifulSoup
import requests

url_to_scrape = "https://www.bls.gov/regions/mid-atlantic/data/xg-tables/ro3fx9527.htm"

r = requests.get(url_to_scrape)

if r.status_code != 200:
    sys.exit(f"Unable to get {url_to_scrape}")

soup = BeautifulSoup(r.text, features="html.parser")

print(soup.prettify())
