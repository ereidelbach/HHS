#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 23 13:07:29 2018

@author: ejreidelbach

:SUMMARY:
    - This script will ingest all the data from a normal DEA dropbox location
    batch of .csv files and place them in one, de-duped Pandas DataFrame. Once
    the files have been ingested, they are then translated into the .json format 
    required by the "Take Back America" website by Visionist, Inc.
    
:REQUIRES:
   
:TODO:
"""

#==============================================================================
# Package Import
#==============================================================================
import json
import os  
import pandas as pd

#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
os.chdir(r'/home/ejreidelbach/projects/HHS/Data/April2018')

# Read in all Files in the specified directory
files = [f for f in os.listdir('.') if f.endswith(('.csv'))]
files = sorted(files)

mergedList = []

for f in files:
    print("Reading in "+f)
    
    # Open the .csv file and add it to a list `merged`
    readDF = pd.read_csv(f)
    mergedList.append(readDF)

# Combine all the ingested .csv files into one master DataFrame
newDF = pd.concat(mergedList)

# De-dup the dataframe
newDF.drop_duplicates(inplace=True)

#####################################

# Start transforming the data into a format that matches what we is needed
#   back Take-Back America

# Divide the `City, State Zip` column up into separate columns for each value
city = []
state = []
postal = []
for result in newDF['City, State Zip'].str.split(', '):
    city.append(result[0])
    state.append(result[1].split(' ')[0])
    postal.append(result[1].split(' ')[1])
newDF['state'] = state
newDF['postal'] = pd.to_numeric(postal)

# Rename the `Business Name` column to `name` and `Map URL` to `googleMapsUrl`
newDF.rename(columns={'Business Name':'name','Map URL':'googleMapsUrl'}, inplace=True)

# Add the source column to state that the data was obtained from the DEA
newDF['source'] = 'DEA'

# Create the `address` column by combining columns: `Address 1`, `Address 2` and
#   `City, State Zip`
newDF['Address 2'].fillna('', inplace=True)
temp = newDF['Address 1'] + ', ' + newDF['Address 2'] + ', ' + newDF['City, State Zip']
split_list = [x.replace(' ,',',') for x in temp]
split_list = [x.split() for x in split_list]
corrected_list = []
for address in split_list:
    corrected_address = ''
    count = 0
    while count < len(address)-2:
        if count == 0:
            corrected_address = address[count].upper()
        else:
            corrected_address = corrected_address + ' ' + address[count].upper()
        count+=1
    corrected_address = corrected_address + ', ' + address[count].strip(',') + ' ' + address[count+1]
    corrected_address = corrected_address.replace(',,',',')
    corrected_list.append(corrected_address)
newDF['address'] = corrected_list
split_list = []

newDF.reset_index(drop=True)

# Output the dataframe to a .csv file
newDF.to_csv('combined.csv', index=False)

# Drop columns that will not be used in the Take-Back America format
newDF.drop(['Address 1', 'Address 2','City, State Zip'], axis=1, inplace=True)

newDF.reset_index(drop=True)

# Output the updated dataframe to another .csv file
newDF.to_csv('combined_TakeBack_Format.csv', index=False)

#####################################

# Read in the latest version of the map markers from Take-Back America (April 2018)
with open(r'/home/ejreidelbach/projects/HHS/Data/map_markers.json') as fmap:
    data = fmap.read()
currentMarkersJSON = json.loads(data)
currentDF = pd.DataFrame(currentMarkersJSON)

# Correct spacing/formatting issues in current data that will prevent it from
#   merging properly with new data
split_list = [x.replace(' ,',',') for x in list(currentDF['address'])]
split_list = [x.split() for x in split_list]
corrected_list = []
for address in split_list:
    corrected_address = ''
    count = 0
    while count < len(address)-2:
        if count == 0:
            corrected_address = address[count].upper()
        else:
            corrected_address = corrected_address + ' ' + address[count].upper()
        count+=1
    corrected_address = corrected_address + ', ' + address[count].strip(',') + ' ' + address[count+1]
    corrected_address = corrected_address.replace(',,',',')
    corrected_list.append(corrected_address)
currentDF['address'] = corrected_list

#####################################

# Find which locations are new between the `current` markers and the `new` markers
mergedDF = pd.merge(currentDF, newDF, how='outer', on=['name','address'], indicator=True)

# Output the merged file to a .csv
mergedDF.to_csv('merged.csv', index=False)

#####################################

# A tremendous amount of corrections were made to merged at this point, by hand,
#   to match addresses between old and new dataframes based on small changes
#   to the company names and/or address names.  The corrected file was saved
#   as merged_user.csv to prevent any accidental overwrites.  

# Import the new dataframe
updatedDF = pd.read_csv('merged_user.csv')

# Rename the source_date value for all `right_only` values to `April2018`
# Rename the source_date value for all `both` values to `Dec2018`
updatedDF['source_date'] = updatedDF['source_date'].replace({'both':'Dec2018', 'right_only':'April2018'})

# Output the finalized file to a .csv
updatedDF.to_csv('/home/ejreidelbach/projects/HHS/Data/April2018.csv', index=False)


