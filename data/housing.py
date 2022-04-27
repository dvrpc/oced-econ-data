# Author: Brian Carney
# Purpose: This script uses the BLS and Census API to pull data needed for OCED's monthly economic update webpage.

import pandas as pd


data = pd.read_csv(
    "https://www2.census.gov/econ/bps/Metro/ma2203y.txt"
    # , names = ['Date', 'MSA', 'PMSA', 'Unnamed: 3', 'MA', 'Unnamed: 5', '1-unit', 'Unnamed: 7', 'Unnamed: 8', '2-units', 'Unnamed: 10', 'Unnamed: 11', '3-4 units', 'Unnamed: 13', 'Unnamed: 14', '5+ units', 'Unnamed: 16', 'Unnamed: 17', '1-unit rep', 'Unnamed: 19', 'Unnamed: 20', '2-units rep', 'Unnamed: 22', 'Unnamed: 23', '3-4 units rep', 'Unnamed: 25', 'Unnamed: 26', '5+ units rep']
)
print(data)
