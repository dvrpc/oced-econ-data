"""
Combine county-level authorized housing units into region-wide total from monthly data provided
via text files by U.S. Dept. of Census.
"""


from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import sys

# get the county data filenames
url_to_scrape = "https://www2.census.gov/econ/bps/County/?C=N;O=D"
r = requests.get(url_to_scrape)

if r.status_code != 200:
    sys.exit(f"Unable to get {url_to_scrape}")

soup = BeautifulSoup(r.text, features="html.parser")

table = soup.find("table")

county_files = []

for row in table.find_all("tr"):
    for cell in row:
        if cell.a:
            if cell.a.string.endswith("c.txt"):
                county_files.append(cell.a.string)

# just get the last 36 months of files
county_files = county_files[:36]

reg_header_list = [
    "Date",
    "State_FIPS",
    "County_FIPS",
    "Region_Code",
    "Division_Code",
    "County",
    "buildings_1unit",
    "units_1unit",
    "permitVal_1unit",
    "buildings_2units",
    "units_2units",
    "permitVal_2units",
    "buildings_3to4units",
    "units_3to4units",
    "permitVal_3to4units",
    "buildings_5punits",
    "units_5punits",
    "permitVal_5punits",
    "rep_buildings_1unit",
    "rep_units_1unit",
    "rep_permitVal_1unit",
    "rep_buildings_2units",
    "rep_units_2units",
    "rep_permitVal_2units",
    "rep_buildings_3to4units",
    "rep_units_3to4units",
    "rep_permitVal_3to4units",
    "rep_buildings_5punits",
    "rep_units_5punits",
    "rep_permitVal_5punits",
]


reg_df_list = []

for file in county_files:
    reg_df_list.append(
        pd.read_csv(
            "https://www2.census.gov/econ/bps/County/" + file,
            dtype={
                "State_FIPS": str,
                "County_FIPS": str,
                "units_1unit": float,
                "units_2units": float,
                "units_3to4units": float,
                "units_5punits": float,
            },
            names=reg_header_list,
            low_memory=False,
            skiprows=[0, 1],
        )
    )


# Concatenate DFs
reg_df = pd.concat(reg_df_list)

# Combine State and County FIPS column
reg_df["FIPS"] = reg_df["State_FIPS"] + reg_df["County_FIPS"]

# Region FIPS
regionFIPS = ["34005", "34007", "34015", "34021", "42017", "42029", "42045", "42091", "42101"]

# Select Region
region = reg_df.loc[reg_df["FIPS"].isin(regionFIPS)]

# Calculate total units authorized
region["total_units"] = (
    region["units_1unit"]
    + region["units_2units"]
    + region["units_3to4units"]
    + region["units_5punits"]
)


# Group by Date
region = region[["Date", "total_units"]]
region_grouped = region.groupby("Date").sum()
region_grouped["geography"] = "Region"
print(region_grouped)
