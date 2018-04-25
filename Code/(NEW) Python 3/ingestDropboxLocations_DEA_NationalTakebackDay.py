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
from geopy.geocoders import Nominatim

#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================
def geopy_lat_long(df):
    geolocator = Nominatim()
    lat = []
    lng = []
    
    for index, row in df.iterrows():
        location = geolocator.geocode(row['address'])
        if location is None:
            lat.append('')
            lng.append('')
        else:
            lat.append(location.latitude)
            lng.append(location.longitude)
        print(str(index))
    return [lat, lng]

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
#newDF.drop(['Unnamed: 0'], axis=1, inplace=True)

# Create the `address` column by combining columns: `Address`, `City`, `State` 
#   and `Zip`
zips = newDF['Zip'].astype(str).str.zfill(5)
#new_zips = []
#for zip in zips:
#    zip = zip.replace('.0','')
#    if len(zip) == 4:
#        zip = '0'+zip
#    new_zips.append(zip)
#new_zips_series = pd.Series(new_zips)
temp = list(newDF['Address'] + ', ' + newDF['City'] + ', ' + newDF['State'] + ' ' + zips)
newDF['address'] = temp

# Rename the `State` column to `state`, `Zip` to `postal`, `CollectionSite` to
#   `name`, and `MapURL` to `googleMapsUrl`
newDF.rename(columns={'State':'state','Zip':'postal', 'CollectionSite':'name',
                      'MapURL':'googleMapsUrl'}, inplace=True)

# Insert the column: `source` and set every value to `DEA` to state
#   that the data was obtained from the DEA
newDF['source'] = 'DEA'

# Insert the column: `source_date` and set every value to `NTBD2018`
newDF['source_date'] = 'NTBD2018'

# Reset the dataframe index
newDF.reset_index(drop=True, inplace=True)

# Remove anymore duplicates that may not have been removed earlier due to the
#   column `Unnamed: 0`
newDF.drop_duplicates(inplace=True)

# Fix issues with the formatting of the `name` column
name_fix = newDF['name']
split_list = [x.split() for x in name_fix]
corrected_list = []
for address in split_list:
    corrected_address = address[0]
    for element in address[1:]:
        corrected_address = corrected_address + ' ' + element
    corrected_list.append(corrected_address)
newDF['name'] = corrected_list
    
# Remove anymore duplicates that may not have been removed earlier due to the
#   column `Unnamed: 0`
newDF.drop_duplicates(inplace=True)

# Output a version for the census
newDF.to_csv('etc/census.csv')

# Drop the columns:  `City` and `Participant`
newDF.drop(['City', 'Participant', 'Address'], axis=1, inplace=True)

# Output the dataframe to a .csv file
newDF.to_csv('etc/combined_TakeBack_Format.csv', index=False)

###############################################################################
# After using the geocoding census website:
#   https://geocoding.geo.census.gov/geocoder/locations/addressbatch?form
#   Ingest the data, and move the contents to the original data frame
censusDF = pd.read_csv('/home/ejreidelbach/Downloads/GeocodeResults(1).csv')
censusDF.drop(['index'], axis=1, inplace=True)

coordinates = censusDF['geocode']
lat_list = []
lng_list = []
for coords in coordinates:
    if type(coords) is float:
        lat_list.append('')
        lng_list.append('')
    else:
        lat_list.append(coords.split(',')[1])
        lng_list.append(coords.split(',')[0])
        
newDF['lat'] = lat_list
newDF['lng'] = lng_list
newDF[['lat','lng']] = newDF[['lat','lng']].apply(pd.to_numeric)

# Backup the data by exporting the results
newDF.to_csv('etc/combined_TakeBack_Format_with_geos.csv', index = False)

###############################################################################

# Try using a python library to help us out by geocoding addresses
coordinates = geopy_lat_long(newDF)
newDF['lat'] = coordinates[0]
newDF['lng'] = coordinates[1]
newDF[['lat','lng']] = newDF[['lat','lng']].apply(pd.to_numeric)

###############################################################################

# Extract out the rows missing lat/long values from the original dataframe
needGeoDF = newDF[newDF.isnull().any(axis=1)]
needGeoDF.to_csv('etc/need_geos.csv')

###############################################################################

# Ingest the original DF for all National TakeBack Day Sites
takeBackDF = pd.read_csv('etc/combined_TakeBack_Format_with_geos.csv')
takeBackDF.set_index('index', inplace=True)

# Ingest the data with newly obtained geo codes
compGeoDF = pd.read_csv('etc/completed_geocodes.csv')
compGeoDF.set_index('Unnamed: 0', inplace=True)

# Merge the datasets to bring in the new lat long values
#mergedDF = pd.merge(takeBackDF, compGeoDF, how='outer', left_on=index, right_on=index, indicator=True)
mergedDF = takeBackDF.merge(compGeoDF, how='left', on=['address'])
mergedDF.drop_duplicates(inplace=True)

# swap new lat/long values into old ones for all addresses that were missing values
mergedDF.lat_x.fillna(mergedDF.lat_y, inplace=True)
mergedDF.lng_x.fillna(mergedDF.lng_y, inplace=True)
mergedDF.drop(['lat_y', 'lng_y', 'name_y', 'state_y',
               'postal_y','googleMapsUrl_y', 'source_y',
               'source_date_y'], axis=1, inplace=True)
mergedDF.rename(columns={'lat_x':'lat','lng_x':'lng',
                         'name_x':'name','state_x':'state',
                         'postal_x':'postal','googleMapsUrl_x':'googleMapsUrl',
                         'source_date_x':'source_date','source_x':'source'}, inplace=True)

mergedDF.to_csv('etc/merged.csv')

###############################################################################

# Ingest the current merged data from April 2018
currentDF = pd.read_csv('/home/ejreidelbach/projects/HHS/Data/April2018_FINAL.csv')

# Find which locations are new between the `current` markers and the `new` markers
mergedDF = pd.merge(currentDF, newDF, how='outer', on=['address','googleMapsUrl',
                                                       'name','postal','source','state','source_date'], indicator=True)
mergedDF.drop_duplicates(inplace=True)

# Output the merged file to a .csv
mergedDF.to_csv('/home/ejreidelbach/projects/HHS/Data/April_NTBI_2018.csv', index=False)

#####################################

finalDF = pd.read_csv('/home/ejreidelbach/projects/HHS/Data/April2018_and_NTBI2018.csv')
exportDF = finalDF[finalDF['source_date'] != 'April2018']

exportDF.to_csv('/home/ejreidelbach/projects/HHS/Data/HHS_Demo_Dec2017_and_TakeBackDay.csv', index=False)
# convert the dataframe to a dictionary for export to .json (nicer formatting)
exportDict = exportDF.to_dict('records')
filename = '/home/ejreidelbach/projects/HHS/Data/HS_Demo_Dec2017_and_TakeBackDay.json'
with open(filename, 'wt') as out:
    json.dump(exportDict, out, sort_keys=True, indent=4, separators=(',', ': '))