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
import csv
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
#os.chdir(r'/home/ejreidelbach/projects/HHS/Data/NTBI_April2018')

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

# Output the dataframe to a .csv file
newDF.to_csv('combined.csv')

#####################################

# Read in the latest version of the map markers from Take-Back America (April 2018)
with open(r'/home/ejreidelbach/projects/HHS/Data/map_markers.json') as fmap:
    data = fmap.read()
currentMarkersJSON = json.loads(data)
currentDF = pd.DataFrame(currentMarkersJSON)

#####################################

# Find which locations are new between the `current` markers and the `new` markers
