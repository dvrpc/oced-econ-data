# Author: Brian Carney
# Purpose: This script uses the BLS and Census API to pull data needed for OCED's monthly economic update webpage.

from datetime import date
import json
from pathlib import Path

import pandas as pd
import requests

from config import BLS_API_KEY


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
p = requests.post(
    "https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers
)

json_data = json.loads(p.text)

# Parse API data into a list of dataframes
dataframes = []
for series in json_data["Results"]["series"]:
    series_name = series["seriesID"]
    df = pd.DataFrame(series["data"])
    df["seriesID"] = series_name
    df.drop(["periodName", "latest", "footnotes", "seriesID"], axis=1)
    # Creating a new column with date format
    df["date"] = date.fromisoformat(df["year"][0] + "-" + df["period"][0][1:] + "-01")
    dataframes.append(df)

# Merge all dataframes together and write to single file
merged_df = pd.concat(dataframes)

results_dir = "pandas_results"
try:
    Path(results_dir).mkdir()
except FileExistsError:
    pass

merged_df.to_csv(results_dir + "/result_cpi.csv", index=False)
