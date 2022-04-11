# Author: Brian Carney
# Purpose: This script uses the BLS and Census API to pull data needed for OCED's monthly economic update webpage.

import json
from pathlib import Path

import pandas as pd
import requests

from config import BLS_API_KEY


us = "LNS14000000"
philadelphia = "LAUMT423798000000003"
trenton = "LAUMT344594000000003"

# Get data from API
headers = {"Content-type": "application/json"}
data = json.dumps(
    {
        "seriesid": [us, philadelphia, trenton],
        "startyear": "2013",
        "endyear": "2022",
        "registrationkey": BLS_API_KEY,
    }
)

# TODO: error handling around this
p = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers)

json_data = json.loads(p.text)
print(json_data["Results"])

# TODO:
#   * create date field from year and month (using 01 for day)
#   * remove fields: periodName, latest, footnotes, seriesID

# Parse API data into a list of dataframes
dataframes = []
for series in json_data["Results"]["series"]:
    series_name = series["seriesID"]
    df = pd.DataFrame(series["data"])
    df["seriesID"] = series_name
    df.loc[df["seriesID"] == us, "geography"] = "United States"
    df.loc[df["seriesID"] == philadelphia, "geography"] = "Philadelphia MSA"
    df.loc[df["seriesID"] == trenton, "geography"] = "Trenton MSA"

    dataframes.append(df)

# Merge all dataframes together and write to single file
merged_df = pd.concat(dataframes)

# print(merged_df)

results_dir = "pandas_results"
try:
    Path(results_dir).mkdir()
except FileExistsError:
    pass

merged_df.to_csv(results_dir + "/result_unemployment.csv", index=False)
