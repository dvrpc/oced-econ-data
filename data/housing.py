# Author: Brian Carney
# Purpose: This script uses the BLS and Census API to pull data needed for OCED's monthly economic update webpage.

import requests
import pandas as pd

response = requests.get("https://www2.census.gov/econ/bps/Metro/ma0001c.txt")
data = response.text