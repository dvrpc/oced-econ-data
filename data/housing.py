# Author: Brian Carney
# Purpose: This script uses the BLS and Census API to pull data needed for OCED's monthly economic update webpage.

import requests
import pandas as pd


data1 = pd.read_csv("https://www2.census.gov/econ/bps/Metro/ma0001c.txt")
print(data1)