#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 11:48:00 2018

@author: ejreidelbach

:DESCRIPTION:

:REQUIRES:
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import os
import pandas as pd
import requests
import time
  

#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================
import re
def multiple_replace(text, adict):
    rx = re.compile('|'.join(map(re.escape, adict)))
    def one_xlat(match):
        return adict[match.group(0)]
    return rx.sub(one_xlat, text)

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
os.chdir(r'/home/ejreidelbach/projects/HHS/Data/')

# Read in locations DF
df = pd.read_csv('April2018_locations_with_counties.csv')

###############################################################################
# create a DF with FIPS codes for every county

# import codes for every state
df_state_fips = pd.read_excel('state_fips_codes.xlsx', converters={'State_Code':str})
# import codes for eveyr county
df_fips = pd.read_excel('all-geocodes-v2016.xlsx', converters={'State_Code':str,'County_Code':str})
# combine these two datasets
df_counties = pd.merge(df_fips, df_state_fips, how='left', on='State_Code')
# drop any rows that don't contain county codes
df_counties = df_counties[df_counties['County_Code'] != '000']
# create a column which contains the full FIPS code (i.e. state + county)
df_counties['fips'] = df_counties['State_Code'] + df_counties['County_Code']

# drop unnecessary columns (i.e. `State_Code`, `County_Code`, `Name`)
df_counties.drop(['State_Code', 'County_Code', 'Name'], axis=1, inplace=True)
# rename the `USPS_Code` column to state
df_counties = df_counties.rename(columns={'USPS_Code':'state'})

###############################################################################
# Correct formatting of df_counties to match df

# fix Alaska by removing: Area, Borough, Census Area, County, City and Borough,
#                         Municipality, and Town
replace_dict = dict.fromkeys([' Area'
                              ,' Borough'
                              ,' Census Area'
                              ,' County'
                              ,' City and Borough'
                              ,' Municipality'
                              ,' Town'
                              ], '')

#df_counties[df_counties['state']=='AK'].apply(
#                lambda row: multiple_replace(row['Area_Name'], replace_dict), 
#                axis=1)
df_counties['Area_Name'] =  df_counties.apply(lambda row: multiple_replace(
        row['Area_Name'], replace_dict), axis=1)
    
###############################################################################
# set county names to lower case in both dataframes
df_counties['Area_Name_lower'] = df_counties['Area_Name'].str.lower()
df['county_lower'] = df['county'].str.lower()

# Merge the FIPS codes for each county with the original location data
df_final = pd.merge(df, df_counties, how='left', 
                    left_on=['county_lower', 'state'],
                    right_on=['Area_Name_lower', 'state'])

# Drop unnecessary columns
df_final.drop(['county_lower'
               ,'Area_Name'
               ,'Area_Name_lower'], axis=1, inplace=True)

# Export the dataframe
df_final.to_csv('April2018_with_counties_and_fips.csv', index=False)





