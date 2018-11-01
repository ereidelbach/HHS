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
from googlemaps import GoogleMaps
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
import time
from geopy.geocoders import Nominatim
import numpy as np
import difflib
import requests
pd.options.mode.chained_assignment = None  # default='warn'
#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================
def geopy_lat_long(df):
    '''
        Purpose: TBD
                 
        Input:
            (1) df: dataframe containing dropbox locations (DataFrame)
            
        Output: 
            (1) TBD   
    '''
    geolocator = Nominatim()
    lat = []
    lng = []
    
    for index, row in df.iterrows():
        try:
            location = geolocator.geocode(row['address'])
        except:
            location = None
            print('Service Error')
        if location is None:
            lat.append('')
            lng.append('')
        else:
            lat.append(location.latitude)
            lng.append(location.longitude)
        print(str(index))
    return [lat, lng]

def ingestDropboxLocations(folder_name):
    '''
        Purpose: Ingest scraped dropbox locations for a variety of zip code
                 files in csv format, compile them together in a list, convert
                 that list to a Pandas DataFrame, de-duplicate the DatFrame,
                 and return it from the function.
        
                 This function also restructures the data to match the format
                 used by Visionist's Dropbox Tool: "Take-Back America"
                 
        Input:
            (1) folder_name: folder path containing csv files of dropbox 
                    locations (string)
            
        Output: 
            (1) newDF: dataframe containing dropbox locations (DataFrame)
    '''
    # Set the project working directory
    os.chdir(folder_name)
    
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
    
    # correct the issue where the row only contains a google map link
    newDF = newDF.dropna(thresh=2) # require at least 2 non-NA values    
    
    # correct for issues with missing spots for address 2
    for index, row in newDF.iterrows():
        # check if the maps url column is null
        if pd.isnull(row.at['MapURL']):
            # if so, slide everything right one cell
            row['Map URL'] = row['City, State Zip']
            row['City, State Zip'] = row['Address 2']
            row['Address 2'] = np.nan
    
    #------------------------------------------------------------------------------
    # Start transforming the data into a format that matches what we is needed
    #   back Take-Back America
    #------------------------------------------------------------------------------
    
    # Create the `address` column by combining: `Address`, `City`, `State` & 'Zip'
    zips = newDF['Zip'].astype(str).str.zfill(5)
    temp = list(newDF['Address'] + ', ' + newDF['City'] + ', ' 
                + newDF['State'] + ' ' + zips)
    newDF['address'] = temp
    
    # Rename the `State` column to `state`, `Zip` to `postal`, `Address` to
    #   `Address 1`, `Participant` to `name` and `MapURL` to `googleMapsUrl`
    newDF.rename(columns={'Participant':'name', 'Address':'Address 1',
                          'MapURL':'googleMapsUrl', 'State':'state',
                          'Zip':'postal'}, inplace=True)
    
    # Insert the column: `source` and set every value to `DEA` to state
    #   that the data was obtained from the DEA
    newDF['source'] = 'DEA'
    
    # Insert the column: `source_date` and set every value to `NTBD_YYYY_MMM`
    newDF['source_date'] = 'NTBD_2018_OCT'
    
    # Reset the dataframe index
    newDF.reset_index(drop=True, inplace=True)
    
    # Output a version for the census
    newDF.to_csv('combined.csv', index=False)
    
    return newDF

def lat_long(df):
    '''
        Purpose: When you utilize a link to Google Maps, Google will update the 
                 URL after about 2-3 seconds of processing with their own 
                 internal version of the URL which includes latitutde and 
                 longitude coordinates.  We're going to wait for this URL to 
                 update, retrieve said URL, and extract the lat/long 
                 coordinates.  This is a work around for the limit on the 
                 number of Google Map API queries you can make each day.
                 
        Input:
            (1) df (DataFrame): dataframe containing dropbox locations
            
        Output: 
            (1) TBD   
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

def googleGeoCode(df):
    '''
    Purpose: Use the Google Maps API to geocode (retrieve lat/long coordinates)
                for a given address
                
    Input:
        (1) df (DataFrame): dataframe containing dropbox locations
        
    Output:
        (1) TBD
    '''
    lat = []
    lng = []
    key = 'AIzaSyBKVoIWv01QsbMr2pHLT0q6Z56oUYc37rE'
    GOOGLE_MAPS_API_URL = 'http://maps.googleapis.com/maps/api/geocode/json?address='

    url = GOOGLE_MAPS_API_URL+key
    
    # Do the request and get the response data
    req = requests.get(GOOGLE_MAPS_API_URL + address + '&key=' + key)
    res = req.json()
    
    # Use the first result
    result = res['results'][0]
    
    geodata = dict()
    geodata['lat'] = result['geometry']['location']['lat']
    geodata['lng'] = result['geometry']['location']['lng']
    geodata['address'] = result['formatted_address']


    for index, row in df.iterrows():
        try:
            x, y = gmaps.address_to_latlng(row['address'])
            lat.append(x)
            lng.append(y)
        except:
            lat.append('')
            lng.append('')
            print('Service Error')
        print('Done with index: ' + str(index))
    
        time.sleep(2)
    return [lat, lng]

#==============================================================================
# Working Code
#==============================================================================
# Import all scraped files and merge into a single dataframe
dropbox_data_new = ingestDropboxLocations(
        '/home/ejreidelbach/projects/HHS/Data/October2018_NTBI')

# Set the project working directory
os.chdir('/home/ejreidelbach/projects/HHS/Data/')    

# Read in the `new` version of the map markers from Take-Back America
dropbox_data_new = pd.read_csv('October2018_NTBI/combined.csv')

#------------------------------------------------------------------------------
# Data Correction #1: Use the following website to geolocate remaining sites
#       https://geocoding.geo.census.gov/geocoder/locations/addressbatch?form
#------------------------------------------------------------------------------
dfNeedGeosV1 = dropbox_data_new.copy()

# Remove columns that won't meet criteria for use in website scraping
dfNeedGeosV1.drop(['name','CollectionSite','googleMapsUrl','address',
                   'source', 'source_date'], axis=1, inplace=True)
dfNeedGeosV1.rename(columns={'Address 1':'address'}, inplace=True)

# correct the type of postal from float to string
dfNeedGeosV1['postal'] = dfNeedGeosV1['postal'].apply(
        lambda x: str(int(x)).zfill(5)).astype(str)

# reorder columns in dataframe to match desired format of website
#   Index -- Address -- City -- State -- Zip Code
dfNeedGeosV1 = dfNeedGeosV1[['address', 'city', 'state', 'postal']]

# export to csv for use in web scraping
dfNeedGeosV1.to_csv('NTBI_October2018_need_geos.csv', header=False)

# Read in newly geolocated data and transform the geocodes into the proper format
# Need to manually insert header
dfNeedGeosV2 = pd.read_csv('NTBI_October2018_need_geos_results.csv',
                           names = ['index','address','match_status','match_type',
                                    'match_address','geo','7','8'], usecols=['index', 'address',
                                             'match_status', 'match_type',
                                             'match_address','geo'])
dfNeedGeosV2.set_index('index', inplace=True)
dfNeedGeosV2.sort_index(inplace=True)

# split `lat` and `lng` values into their own columns
dfNeedGeosV2['geo'] = dfNeedGeosV2['geo'].astype(str)
dfNeedGeosV2[['lng','lat']] = dfNeedGeosV2['geo'].str.split(',',expand=True)

# drop the unneeded `geo` column
dfNeedGeosV2.drop(['geo',], axis=1, inplace=True)

# obtain a csv that's "easier" to work with manually
dfNeedGeosV2.to_csv('NTBI_October2018_need_geos_results_manual.csv', index=False)

#------------------------------------------------------------------------------
# Data Correction #2: Use the `geopy_lat_long` function to find lat/long values
#------------------------------------------------------------------------------ 
# read in manually corrected addresses
dfNeedGeosV3 = pd.read_csv('NTBI_October2018_need_geos_results_manual.csv')

# merge original addresses with `lat` / `lng` info for those rows
dfMergeV1 = pd.merge(dropbox_data_new, dfNeedGeosV3, left_index=True, right_index=True, on=['address'])

# Isolate rows that still don't have `lat`/`lng` matches from the `merged` version
dfNeedGeosV4 = dfMergeV1[dfMergeV1['lat'].isna() != False]

# Try using a python library to help us out by geocoding addresses
coordinates = geopy_lat_long(dfNeedGeosV4)
dfNeedGeosV4['lat'] = coordinates[0]
dfNeedGeosV4['lng'] = coordinates[1]
dfNeedGeosV4[['lat','lng']] = dfNeedGeosV4[['lat','lng']].apply(pd.to_numeric)

# Merge this data back with our original data
dfMergeV2 = pd.merge(dfMergeV1, dfNeedGeosV4, how='left', left_index=True, 
                     right_index=True, on=['name','CollectionSite','Address 1',
                                           'City','state','postal',
                                           'googleMapsUrl','address','source',
                                           'source_date',])

# swap new lat/long values into old ones for all addresses that were missing values
dfMergeV2.lat_x.fillna(dfMergeV2.lat_y, inplace=True)
dfMergeV2.lng_x.fillna(dfMergeV2.lng_y, inplace=True)
dfMergeV2.drop(['lat_y', 'lng_y'], axis=1, inplace=True)
dfMergeV2.rename(columns={'lat_x':'lat','lng_x':'lng'}, inplace=True)
dfMergeV2.to_csv('NTBI_October2018_still_need_geos_v2.csv', index=False)

#------------------------------------------------------------------------------
# Data Correction #2: Use the `geopy_lat_long` function to find lat/long values
#   one final time as we had time-out issues using the full list
#------------------------------------------------------------------------------

# Isolate rows that still don't have `lat`/`lng` matches from the `merged` version
dfNeedGeosV5 = dfMergeV2[dfMergeV2['lat'].isna() != False]

# Try using a python library to help us out by geocoding addresses
coordinates = geopy_lat_long(dfNeedGeosV5)
coordinates = googleGeoCode(dfNeedGeosV5)
dfNeedGeosV5['lat'] = coordinates[0]
dfNeedGeosV5['lng'] = coordinates[1]
dfNeedGeosV5[['lat','lng']] = dfNeedGeosV5[['lat','lng']].apply(pd.to_numeric)

# Merge this data back with our original data
dfMergeV3 = pd.merge(dfMergeV2, dfNeedGeosV5, how='left', left_index=True, 
                     right_index=True, on=['name','CollectionSite','Address 1',
                                           'City','state','postal',
                                           'googleMapsUrl','address','source',
                                           'source_date',])

# swap new lat/long values into old ones for all addresses that were missing values
dfMergeV3.lat_x.fillna(dfMergeV3.lat_y, inplace=True)
dfMergeV3.lng_x.fillna(dfMergeV3.lng_y, inplace=True)
dfMergeV3.drop(['lat_y', 'lng_y'], axis=1, inplace=True)
dfMergeV3.rename(columns={'lat_x':'lat','lng_x':'lng'}, inplace=True)









exportDF.to_csv('/home/ejreidelbach/projects/HHS/Data/HHS_Demo_Dec2017_and_TakeBackDay.csv', index=False)
# convert the dataframe to a dictionary for export to .json (nicer formatting)
exportDict = exportDF.to_dict('records')
filename = '/home/ejreidelbach/projects/HHS/Data/HS_Demo_Dec2017_and_TakeBackDay.json'
with open(filename, 'wt') as out:
    json.dump(exportDict, out, sort_keys=True, indent=4, separators=(',', ': '))