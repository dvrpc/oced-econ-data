#Author: Brian Carney
#Purpose: This script cleans the housing units authorized data to be used for the region's monthly economic update.

#Import packages
import pandas as pd
import numpy as np


reg_header_list = ["Date", "State_FIPS", "County_FIPS", "Region_Code", "Division_Code", "County", "buildings_1unit", "units_1unit", "permitVal_1unit", "buildings_2units", "units_2units", "permitVal_2units", "buildings_3to4units", "units_3to4units", "permitVal_3to4units", "buildings_5punits", "units_5punits", "permitVal_5punits", "rep_buildings_1unit", "rep_units_1unit", "rep_permitVal_1unit", "rep_buildings_2units", "rep_units_2units", "rep_permitVal_2units", "rep_buildings_3to4units", "rep_units_3to4units", "rep_permitVal_3to4units", "rep_buildings_5punits", "rep_units_5punits", "rep_permitVal_5punits"]


# Region
reg_mar22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2203c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_feb22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2202c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_jan22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2201c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_dec21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2112c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_nov21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2111c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_oct21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2110c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_sep21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2109c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_aug21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2108c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_jul21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2107c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_jun21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2106c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_may22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2105c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_apr22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2104c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_mar21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2103c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_feb21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2102c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])
reg_jan21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2101c.txt", dtype={'State_FIPS': str, 'County_FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= reg_header_list, low_memory=False, skiprows=[0,1])


# Concatenate DFs
reg_df_list = [reg_mar22_month, reg_feb22_month,reg_jan22_month, reg_dec21_month, reg_nov21_month, reg_oct21_month, reg_sep21_month, reg_aug21_month, reg_jul21_month, reg_jun21_month, reg_may22_month, reg_apr22_month, reg_mar21_month, reg_feb21_month, reg_jan21_month]
reg_df = pd.concat(reg_df_list)
print(reg_df)


# Create FIPS column
reg_df['FIPS'] = reg_df['State_FIPS'] + reg_df['County_FIPS']


# Region FIPS
regionFIPS = ["34005", "34007", "34015", "34021", "42017", "42029", "42045", "42091", "42101"]

# Select Region
region = reg_df.loc[reg_df['FIPS'].isin(regionFIPS)]
print(region)


# Calculate total units authorized
region['total_units'] = region['units_1unit'] + region['units_2units'] + region['units_3to4units'] + region['units_5punits']
print(region)

# Group by Date
region = region[['Date', 'total_units']]
region_grouped = region.groupby('Date').sum()
region_grouped['geography'] = 'Region'
print(region_grouped)


# MSA
msa_header_list = ["Date", "CSA", "FIPS", "Alpha", "Alpha_Name", "buildings_1unit", "units_1unit", "permitVal_1unit", "buildings_2units", "units_2units", "permitVal_2units", "buildings_3to4units", "units_3to4units", "permitVal_3to4units", "buildings_5punits", "units_5punits", "permitVal_5punits", "rep_buildings_1unit", "rep_units_1unit", "rep_permitVal_1unit", "rep_buildings_2units", "rep_units_2units", "rep_permitVal_2units", "rep_buildings_3to4units", "rep_units_3to4units", "rep_permitVal_3to4units", "rep_buildings_5punits", "rep_units_5punits", "rep_permitVal_5punits"]


msa_mar22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2203c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_feb22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2202c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_jan22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2201c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_dec21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2112c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_nov21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2111c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_oct21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2110c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_sep21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2109c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_aug21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2108c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_jul21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2107c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_jun21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2106c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_may22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2105c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_apr22_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2104c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_mar21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2103c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_feb21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2102c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])
msa_jan21_month = pd.read_csv(r"https://www2.census.gov/econ/bps/Metro/ma2101c.txt", dtype={'FIPS': str, 'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float}, names= msa_header_list, low_memory=False, skiprows=[0,1])


# Concatenate DFs
msa_df_list = [msa_mar22_month, msa_feb22_month,msa_jan22_month, msa_dec21_month, msa_nov21_month, msa_oct21_month, msa_sep21_month, msa_aug21_month, msa_jul21_month, msa_jun21_month, msa_may22_month, msa_apr22_month, msa_mar21_month, msa_feb21_month, msa_jan21_month]
msa_df = pd.concat(msa_df_list)
print(msa_df)

# Select region MSAs
msa_fips = ['37980', '45940']
reg_msa = msa_df.loc[msa_df['FIPS'].isin(msa_fips)]


reg_msa.loc[reg_msa['FIPS'] == '45940', 'geography'] = 'Trenton MSA'
reg_msa.loc[reg_msa['FIPS'] == '37980', 'geography'] = 'Philadelphia MSA'

print(reg_msa)


"""
ToDo: 
-Combine raw20_annual and raw21_annual
-Add 'FIPS' column
-Filter out non-region geographies
-Append monthly data to annual? Not sure yet
"""

"""
monthly_header_list = ["Date", "State_FIPS", "County_FIPS", "FIPS1_Code" "Region_Code", "Division_Code", "County", "buildings_1unit", "units_1unit", "permitVal_1unit", "buildings_2units", "units_2units", "permitVal_2units", "buildings_3to4units", "units_3to4units", "permitVal_3to4units", "buildings_5punits", "units_5punits", "permitVal_5punits", "rep_buildings_1unit", "rep_units_1unit", "rep_permitVal_1unit", "rep_buildings_2units", "rep_units_2units", "rep_permitVal_2units", "rep_buildings_3to4units", "rep_units_3to4units", "rep_permitVal_3to4units", "rep_buildings_5punits", "rep_units_5punits", "rep_permitVal_5punits"]

raw_jan22 = pd.read_csv("G:\\Shared drives\\Community & Economic Development\\EconomicUpdate\\data\\counties\\BuildingPermitsSurvey\\monthly\\co2101c.txt", dtype={'FIPS': str}, names=monthly_header_list)
raw_feb22 = pd.read_csv("G:\\Shared drives\\Community & Economic Development\\EconomicUpdate\\data\\counties\\BuildingPermitsSurvey\\monthly\\co2102c.txt", dtype={'FIPS': str}, names=monthly_header_list)
raw_mar22 = pd.read_csv("G:\\Shared drives\\Community & Economic Development\\EconomicUpdate\\data\\counties\\BuildingPermitsSurvey\\monthly\\co2103c.txt", dtype={'FIPS': str}, names=monthly_header_list)
"""


"""
permits_Jan21toOct21 = pd.concat([raw_jan22, raw_feb22, raw_mar22])
permits_Jan21toOct21['FIPS'] = permits_Jan21toOct21['State_FIPS'] + permits_Jan21toOct21['County_FIPS']
region_permits_Jan21toOct21 = permits_Jan21toOct21.loc[permits_Jan21toOct21['FIPS'].isin(regionFIPS)]
prfloat(region_permits_Jan21toOct21)


region_permits_Jan21toOct21_condensed = region_permits_Jan21toOct21[['Date', 'units_1unit', 'units_2units', 'units_3to4units', 'units_5punits']]
prfloat(region_permits_Jan21toOct21_condensed)

region_permits_Jan21toOct21_condensed = region_permits_Jan21toOct21_condensed.astype({'units_1unit': float, 'units_2units': float, 'units_3to4units': float, 'units_5punits': float})
region_permits_Jan21toOct21_condensed.dtypes

region_permits_Jan21toOct21_grouped = region_permits_Jan21toOct21_condensed.groupby('Date').sum()
prfloat(region_permits_Jan21toOct21_grouped)

region_permits_Jan21toOct21_grouped['total_units'] = region_permits_Jan21toOct21_grouped['units_1unit'] + region_permits_Jan21toOct21_grouped['units_2units'] + region_permits_Jan21toOct21_grouped['units_3to4units'] + region_permits_Jan21toOct21_grouped['units_5punits']
prfloat(region_permits_Jan21toOct21_grouped)

region_permits_Jan21toOct21_grouped.to_csv("G:\\Shared drives\\Community & Economic Development\\EconomicUpdate\\region_buildingPermits_2021.csv")
"""