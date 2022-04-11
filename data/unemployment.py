# Author: Brian Carney
# Purpose: This script uses the BLS and Census API to pull data needed for OCED's monthly economic update webpage.

import json
import os
from pathlib import Path

import pandas as pd
import requests

# Get data from API
headers = {"Content-type": "application/json"}
data = json.dumps(
    {
        "seriesid": ["LNS14000000", "LAUMT423798000000003", "LAUMT344594000000003"],
        "startyear": "2013",
        "endyear": "2022",
        "registrationkey": os.getenv("BLS_API_KEY"),
    }
)
p = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers)

json_data = json.loads(p.text)
print(json_data["Results"])

# Parse API data into a list of dataframes
dataframes = []
for series in json_data["Results"]["series"]:
    series_name = series["seriesID"]
    df = pd.DataFrame(series["data"])
    df["seriesID"] = series_name
    df.loc[df["seriesID"] == "LNS14000000", "variable"] = "unemployment_rate"
    df.loc[df["seriesID"] == "LAUMT423798000000003", "variable"] = "unemployment_rate"
    df.loc[df["seriesID"] == "LAUMT344594000000003", "variable"] = "unemployment_rate"
    df.loc[df["seriesID"] == "LNS14000000", "geography"] = "U.S."
    df.loc[df["seriesID"] == "LAUMT423798000000003", "geography"] = "Philadelphia MSA"
    df.loc[df["seriesID"] == "LAUMT344594000000003", "geography"] = "Trenton MSA"

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
