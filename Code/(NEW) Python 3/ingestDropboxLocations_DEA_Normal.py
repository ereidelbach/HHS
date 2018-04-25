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
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
import time
from geopy.geocoders import Nominatim
import numpy as np

#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================
def lat_long(df):
    '''
        When you utilize a link to Google Maps, Google will update the URL after
        about 2-3 seconds of processing with their own internal version of the URL
        which includes latitutde and longitude coordinates.  We're going to wait
        for this URL to update, retrieve said URL, and extract the lat/long 
        coordinates.  This is a work around for the limit on the number of
        Google Map API queries you can make each day.
    '''
    
    # createa  web browser for retrieving lat-long info from Google Map URLs
    options = Options()
    options.set_headless(headless=True)
    browser = webdriver.Firefox(firefox_options=options)
    #browser = webdriver.Firefox()
    browser.implicitly_wait(100)
       
    lat = []
    lng = []
    for index, row in df.iterrows():
        # Go to the URL contained within the specific row of the DataFrame
        mapURL = row['googleMapsUrl']
        browser.get(mapURL)    
        #time.sleep(3)
        
        # Retrieve the updated url with lat / long information
#        wait(browser, 10).until(EC.url_changes(mapURL))
#        wait(browser, 10).until(lambda browser: browser.current_url != mapURL)
        wait(browser, 30).until(EC.url_contains('/place/'))
        locationURL = browser.current_url
#        print(locationURL)
        parts = locationURL.split('@')[1].split(',')
        lat.append(parts[0])
        lng.append(parts[1])
        
        print('Done with: ' + str(index) + '.  Lat = ' + str(parts[0]) + 
              ', Lng = ' + str(parts[1]) + '.')
        time.sleep(4)
    browser.close()
    return [lat, lng]

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

#####################################

# Read in the updatedDF to obtain lat/long's for each new location
aprilDF = pd.read_csv('/home/ejreidelbach/projects/HHS/Data/April2018.csv')
need_geosDF = aprilDF[aprilDF.isnull().any(axis=1)]

# Try using a python library to help us out by geocoding addresses
coordinates = geopy_lat_long(need_geosDF)
need_geosDF['lat'] = coordinates[0]
need_geosDF['lng'] = coordinates[1]
need_geosDF[['lat','lng']] = need_geosDF[['lat','lng']].apply(pd.to_numeric)

# Merge this data back with our original data
mergeDF = pd.merge(aprilDF, need_geosDF, how='outer', 
                   on=['name','address','postal','source','state','source_date',
                       'googleMapsUrl'])

# swap new lat/long values into old ones for all addresses that were missing values
mergeDF.lat_x.fillna(mergeDF.lat_y, inplace=True)
mergeDF.lng_x.fillna(mergeDF.lng_y, inplace=True)
mergeDF.drop(['lat_y', 'lng_y'], axis=1, inplace=True)
mergeDF.rename(columns={'lat_x':'lat','lng_x':'lng'}, inplace=True)

# Save the file by outputting it to a csv
mergeDF.to_csv('/home/ejreidelbach/projects/HHS/Data/April2018_with_geos.csv')
    
# Let's see how many missing geos we still have
need_geosDF = mergeDF[mergeDF.isnull().any(axis=1)]
need_geosDF.drop(['lat','lng','name','source','source_date'], axis=1, inplace=True)

split_address = need_geosDF['address'].str.split(',')
address_list = []
city_list = []
for address in split_address:
    if len(address) == 3:
        address_list.append(address[0])
        city_list.append(address[1])
    elif len(address) == 4:
        address_list.append(address[1])
        city_list.append(address[2])
    elif len(address) == 5:
        address_list.append(address[1] + ',' + address[2])
        city_list.append(address[3])
    elif len(address) == 6:
        address_list.append(address[2] + ',' + address[3])
        city_list.append(address[4])
    else:
        print('Whoops')
        
need_geosDF['address'] = address_list
need_geosDF['city'] = city_list
# Use this file to scrape:  
#   https://geocoding.geo.census.gov/geocoder/locations/addressbatch?form
need_geosDF.to_csv('/home/ejreidelbach/projects/HHS/Data/April2018_need_geos.csv')

# Read in newly scraped data and transform the retrieved geocodes into the proper format
googleGeosDF = pd.read_csv('/home/ejreidelbach/projects/HHS/Data/April2018_need_geos.csv')
coordinates = googleGeosDF['geo'].str.split(',')
lat_list = []
lng_list = []
for coords in coordinates:
    if type(coords) is float:
        lat_list.append('')
        lng_list.append('')
    else:
        lat_list.append(coords[1])
        lng_list.append(coords[0])
googleGeosDF['lat'] = lat_list
googleGeosDF['lng'] = lng_list
googleGeosDF.drop(['geo',], axis=1, inplace=True)

# At this point, I manually copied in google URLs into the new column `newURL`
#   We will use this URL to retrieve `lat` and `lng` values
locationURL = googleGeosDF['newURL']
lat_list = []
lng_list = []
for location in locationURL:
    if type(location) == float:
        lat_list.append('')
        lng_list.append('')
    else:
        try:
            lat_list.append(location.split('@')[1].split(',')[0])
            lng_list.append(location.split('@')[1].split(',')[1])
        except:
            print(location)

googleGeosDF['lat_new'] = lat_list
googleGeosDF['lng_new'] = lng_list

# move values from new columns into missing values in original columns
googleGeosDF.loc[googleGeosDF['lat'] == '','lat'] = googleGeosDF['lat_new']
googleGeosDF.loc[googleGeosDF['lng'] == '','lng'] = googleGeosDF['lng_new']

# Set the index to be the column `Unnamed: 0`
googleGeosDF.set_index('Unnamed: 0', inplace=True)

# delete unnecessary columns
googleGeosDF.drop(['lat_new', 'lng_new', 'newURL','googleMapsUrl','city'], axis=1, inplace=True)

# backup file
googleGeosDF.to_csv('/home/ejreidelbach/projects/HHS/Data/April2018_google_geos.csv')
awaitingGeosDF = pd.read_csv('/home/ejreidelbach/projects/HHS/Data/April2018_with_geos.csv')

merge2DF = pd.merge(mergeDF, googleGeosDF, how='outer', left_index=True, right_index=True, indicator=True)
merge2DF.to_csv('/home/ejreidelbach/projects/HHS/Data/April2018_ALL_geos.csv',index=False)

# swap new lat/long values into old ones for all addresses that were missing values
merge2DF.lat_x.fillna(merge2DF.lat_y, inplace=True)
merge2DF.lng_x.fillna(merge2DF.lng_y, inplace=True)
merge2DF.drop(['lat_y', 'lng_y', 'address_y', 'postal_y','state_y', '_merge'], axis=1, inplace=True)
merge2DF.rename(columns={'lat_x':'lat','lng_x':'lng','address_x':'address',
                         'postal_x':'postal','state_x':'state'}, inplace=True)
merge2DF.to_csv('/home/ejreidelbach/projects/HHS/Data/April2018_FINAL.csv', index=False)

# convert the dataframe to a dictionary for export to .json (nicer formatting)
mergedDict = merge2DF.to_dict('records')
filename = '/home/ejreidelbach/projects/HHS/Data/April2018_FINAL.json'
with open(filename, 'wt') as out:
    json.dump(mergedDict, out, sort_keys=True, indent=4, separators=(',', ': '))