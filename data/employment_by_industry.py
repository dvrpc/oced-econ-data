# Author: Brian Carney
# Purpose: This script uses the BLS and Census API to pull data needed for OCED's monthly economic update webpage.

from datetime import date
import json
from pathlib import Path

import pandas as pd
import requests

from config import BLS_API_KEY

prefix = "EN"
adjusted = "S"
philadelphia = "C3798"
trenton = "C4594"
data_type = "1"
size_code = "0"
ownership = "5"
mining = "1011" # Natural resources and mining
construction = "1012" # Construction
manufacturing = "1013" # Manufacturing
trade = "1021" # Trade, transportation, and utilities
information = "1022" # Information
financial = "1023" # Financial activities
professional = "1024" # Professional and business services
education = "1025" # Education and health services
leisure = "1026" # Leisure and hospitality
other = "1027" # Other services
public = "1028" # Public administration




# Get data from API
headers = {"Content-type": "application/json"}
data = json.dumps(
    {
        "seriesid": [prefix + adjusted + philadelphia + data_type + size_code + ownership + mining],
        "startyear": "2013",
        "endyear": "2022",
        "registrationkey": BLS_API_KEY,
    }
)
p = requests.post(
    "https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers
)

json_data = json.loads(p.text)
print(json_data)

# Parse API data into a list of dataframes
dataframes = []
for series in json_data["Results"]["series"]:
    series_name = series["seriesID"]
    df = pd.DataFrame(series["data"])
    df["seriesID"] = series_name
    # Creating a new column with date format
    df["date"] = df.apply(
        lambda row: date.fromisoformat(
            str(row.year) + "-" + (str(row.period[1:]) + "-01")
        ),
        axis=1,
    )

    dataframes.append(df)

# Merge all dataframes together and write to single file
merged_df = pd.concat(dataframes)
print(merged_df)
list(merged_df)

results_dir = "pandas_results"
try:
    Path(results_dir).mkdir()
except FileExistsError:
    pass

merged_df.to_csv(results_dir + "/result_cpi.csv", index=False)
