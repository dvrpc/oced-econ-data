#Author: Brian Carney
#Purpose: This script cleans the housing units authorized data to be used for the region's monthly economic update.

#Import packages
import pandas as pd
import numpy as np


#Region FIPS
regionFIPS = ["34005", "34007", "34015", "34021", "42017", "42029", "42045", "42091", "42101"]

annual_header_list = ["Date", "State_FIPS", "County_FIPS", "Region_Code", "Division_Code", "County", "buildings_1unit", "units_1unit", "permitVal_1unit", "buildings_2units", "units_2units", "permitVal_2units", "buildings_3to4units", "units_3to4units", "permitVal_3to4units", "buildings_5punits", "units_5punits", "permitVal_5punits", "rep_buildings_1unit", "rep_units_1unit", "rep_permitVal_1unit", "rep_buildings_2units", "rep_units_2units", "rep_permitVal_2units", "rep_buildings_3to4units", "rep_units_3to4units", "rep_permitVal_3to4units", "rep_buildings_5punits", "rep_units_5punits", "rep_permitVal_5punits"]

raw21_annual = pd.read_csv(r"https://www2.census.gov/econ/bps/County/co2021a.txt", dtype={'FIPS': str}, names=annual_header_list)
print(raw21_annual)

raw20_annual = pd.read_csv("G:\\Shared drives\\Community & Economic Development\\EconomicUpdate\\data\\counties\\BuildingPermitsSurvey\\annual\\co2020a.txt", dtype={'FIPS': str}, names=annual_header_list)
print(raw20_annual)

"""
ToDo: 
-Combine raw20_annual and raw21_annual
-Add 'FIPS' column
-Filter out non-region geographies
-Append monthly data to annual? Not sure yet
"""

raw20['FIPS'] = raw20['State_FIPS'] + raw20['County_FIPS']

region20 = raw20.loc[raw20['FIPS'].isin(regionFIPS)]
print(region20)

monthly_header_list = ["Date", "State_FIPS", "County_FIPS", "FIPS1_Code" "Region_Code", "Division_Code", "County", "buildings_1unit", "units_1unit", "permitVal_1unit", "buildings_2units", "units_2units", "permitVal_2units", "buildings_3to4units", "units_3to4units", "permitVal_3to4units", "buildings_5punits", "units_5punits", "permitVal_5punits", "rep_buildings_1unit", "rep_units_1unit", "rep_permitVal_1unit", "rep_buildings_2units", "rep_units_2units", "rep_permitVal_2units", "rep_buildings_3to4units", "rep_units_3to4units", "rep_permitVal_3to4units", "rep_buildings_5punits", "rep_units_5punits", "rep_permitVal_5punits"]

raw_jan22 = pd.read_csv("G:\\Shared drives\\Community & Economic Development\\EconomicUpdate\\data\\counties\\BuildingPermitsSurvey\\monthly\\co2101c.txt", dtype={'FIPS': str}, names=monthly_header_list)
raw_feb22 = pd.read_csv("G:\\Shared drives\\Community & Economic Development\\EconomicUpdate\\data\\counties\\BuildingPermitsSurvey\\monthly\\co2102c.txt", dtype={'FIPS': str}, names=monthly_header_list)
raw_mar22 = pd.read_csv("G:\\Shared drives\\Community & Economic Development\\EconomicUpdate\\data\\counties\\BuildingPermitsSurvey\\monthly\\co2103c.txt", dtype={'FIPS': str}, names=monthly_header_list)



"""
permits_Jan21toOct21 = pd.concat([raw_jan22, raw_feb22, raw_mar22])
permits_Jan21toOct21['FIPS'] = permits_Jan21toOct21['State_FIPS'] + permits_Jan21toOct21['County_FIPS']
region_permits_Jan21toOct21 = permits_Jan21toOct21.loc[permits_Jan21toOct21['FIPS'].isin(regionFIPS)]
print(region_permits_Jan21toOct21)


region_permits_Jan21toOct21_condensed = region_permits_Jan21toOct21[['Date', 'units_1unit', 'units_2units', 'units_3to4units', 'units_5punits']]
print(region_permits_Jan21toOct21_condensed)

region_permits_Jan21toOct21_condensed = region_permits_Jan21toOct21_condensed.astype({'units_1unit': int, 'units_2units': int, 'units_3to4units': int, 'units_5punits': int})
region_permits_Jan21toOct21_condensed.dtypes

region_permits_Jan21toOct21_grouped = region_permits_Jan21toOct21_condensed.groupby('Date').sum()
print(region_permits_Jan21toOct21_grouped)

region_permits_Jan21toOct21_grouped['total_units'] = region_permits_Jan21toOct21_grouped['units_1unit'] + region_permits_Jan21toOct21_grouped['units_2units'] + region_permits_Jan21toOct21_grouped['units_3to4units'] + region_permits_Jan21toOct21_grouped['units_5punits']
print(region_permits_Jan21toOct21_grouped)

region_permits_Jan21toOct21_grouped.to_csv("G:\\Shared drives\\Community & Economic Development\\EconomicUpdate\\region_buildingPermits_2021.csv")
"""