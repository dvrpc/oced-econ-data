import csv
from datetime import date
import json
from pathlib import Path
from typing import List

import requests

from config import BLS_API_KEY


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

# TODO: error handling around this
p = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers)

json_data = json.loads(p.text)

# go through results, make human-readable area and industry, create date
cleaned_data: List = []
for series in json_data["Results"]["series"]:
    if series["seriesID"][:10] == trenton:
        area = "Trenton MSA"
    if series["seriesID"][:10] == philadelphia:
        area = "Philadelphia MSA"
    industry = industries[series["seriesID"][10:]]

    for record in series["data"]:
        period = date.fromisoformat(record["year"] + "-" + (str(record["period"][1:]) + "-01"))
        cleaned_data.append([period, record["value"], industry, area])

results_dir = "results"
try:
    Path(results_dir).mkdir()
except FileExistsError:
    pass

with open("results/industry_employment_from_api.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["period", "value", "industry", "area"])
    writer.writerows(cleaned_data)
