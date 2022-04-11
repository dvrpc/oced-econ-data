# Author: Brian Carney
# Purpose: This script uses the BLS and Census API to pull data needed for OCED's monthly economic update webpage.

import json
import os
from pathlib import Path

import requests
import pandas as pd

# Get data from API
headers = {"Content-type": "application/json"}
data = json.dumps(
    {
        "seriesid": ["CUUR0000SA0", "CUURS12BSA0"],
        "startyear": "2013",
        "endyear": "2022",
        "registrationkey": os.getenv("BLS_API_KEY"),
    }
)
p = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers)

json_data = json.loads(p.text)

# Parse API data into a list of dataframes
dataframes = []
for series in json_data["Results"]["series"]:
    series_name = series["seriesID"]
    df = pd.DataFrame(series["data"])
    df["seriesID"] = series_name

    dataframes.append(df)

# Merge all dataframes together and write to single file
merged_df = pd.concat(dataframes)

print(merged_df)

results_dir = "pandas_results"
try:
    Path(results_dir).mkdir()
except FileExistsError:
    pass

merged_df.to_csv(results_dir + "/result_cpi.csv", index=False)
