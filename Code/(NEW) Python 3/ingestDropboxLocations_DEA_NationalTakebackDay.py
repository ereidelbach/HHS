#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 24 18:46:20 2018

@author: ejreidelbach

:DESCRIPTION:
    - This script will ingest all the data for the National Takeback Day dropbox location
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
os.chdir(r'/home/ejreidelbach/projects/HHS/Data/NTBI_April2018')

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

# Remove the unnamed column (indexes from each file)
newDF.drop(['Unnamed: 0'], axis=1, inplace=True)

# Create the `address` column by combining columns: `Address`, `City`, `State` 
#   and `Zip`
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

# Rename the `State` column to `state`, `Zip` to `postal`, `CollectionSite` to
#   `name`, and `MapURL` to `googleMapsUrl`
newDF.rename(columns={'State':'state','Zip':'postal', 'CollectionSite':'name',
                      'MapURL':'googleMapsUrl'}, inplace=True)

# Insert the column: `source` and set every value to `DEA` to state
#   that the data was obtained from the DEA
newDF['source'] = 'DEA'

# Insert the column: `source_date` and set every value to `NTBD2018`
newDF['source_date'] = 'NTBD2018'

# Drop the columns:  `City` and `Participant`
newDF.drop(['City', 'Participant'], axis=1, inplace=True)

# Reset the dataframe index
newDF.reset_index(drop=True)

# Output the dataframe to a .csv file
newDF.to_csv('combined_TakeBack_Format.csv', index=False)