#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 23 13:07:29 2018

@author: ejreidelbach

:DESCRIPTION:
    This script will ingest all the data from a normal DEA dropbox location
    batch of .csv files and place them in one, de-duped Pandas DataFrame. Once
    the files have been ingested, they are then translated into the .json format 
    required by the "Take Back America" website by Visionist, Inc.

:REQUIRES:
   
:TODO:
    
:NOTES:
      
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
import difflib
import requests

#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================
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
            (1) df: dataframe containing dropbox locations (DataFrame)
            
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

#cleanDropboxLocations('/home/ejreidelbach/projects/HHS/Data/October2018')

def cleanDropboxLocations(folder_name):
    '''
        Purpose: Ingest scraped dropbox locations for a variety of zip code
                 files in csv format, convert them into to a Pandas DataFrame, 
                 de-duplicate the DatFrame, and save it to a 'clean' csv.
        
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
    
    # Open every .csv file and add it to a list `merged`
    for f in files:
        print("Reading in "+f)
        readDF = pd.read_csv(f)
    
        # De-dup the dataframe
        readDF.drop_duplicates(inplace=True)
        
        # correct for issues with missing spots for address 2
        for index, row in readDF.iterrows():
            # check if the maps url column is null
            if pd.isnull(row.at['Map URL']):
                # if so, slide everything right one cell
                row['Map URL'] = row['City, State Zip']
                row['City, State Zip'] = row['Address 2']
                row['Address 2'] = np.nan
        
        # Divide the `City, State Zip` column into separate columns for each value
        city = []
        state = []
        postal = []
        for result in readDF['City, State Zip'].str.split(', '):
            try:
                city.append(result[0])
                state.append(result[1].split(' ')[0])
                postal.append(result[1].split(' ')[1])
            except:
                pass
        readDF['state'] = state
        readDF['postal'] = pd.to_numeric(postal)    
    
        # Rename `Business Name` column to `name` & `Map URL` to `googleMapsUrl`
        readDF.rename(columns={'Business Name':'name', 
                              'Map URL':'googleMapsUrl'}, inplace=True)
        
        # Add the source column to state that the data was obtained from the DEA
        readDF['source'] = 'DEA'
        
        # Add the source_date column to the data that was obtained from the DEA
        # string manipulation to obtain MMM-YYYY
        date = (folder_name.split('/')[-1].split('20')[0][0:3] + 
                folder_name.split('/')[-1][-4:])
        readDF['source_date'] = date
        
        # Create the `address` column by combining columns: 
        #   `Address 1`, `Address 2` and `City, State Zip`
        readDF['Address 2'].fillna('', inplace=True)
        temp = (readDF['Address 1'] + ', ' + readDF['Address 2'] + 
                ', ' + readDF['City, State Zip'])
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
                    corrected_address = (corrected_address + ' ' + 
                                         address[count].upper())
                count+=1
            corrected_address = (corrected_address + ', ' + 
                                 address[count].strip(',') + 
                                 ' ' + address[count+1])
            corrected_address = corrected_address.replace(',,',',')
            corrected_list.append(corrected_address)
        readDF['address'] = corrected_list
        split_list = []
        
        readDF.reset_index(drop=True)
        
#        # Output the dataframe to a .csv file
#        readDF.to_csv('combined.csv', index=False)
#        
#        # Drop columns that will not be used in the Take-Back America format
#        readDF.drop(['Address 1', 'Address 2','City, State Zip'], axis=1, inplace=True)
#        
#        readDF.reset_index(drop=True)
#        
        # Output the updated dataframe to another .csv file
        readDF.to_csv('cleaned/' + str(f.split('.csv')[0]) + '_clean.csv', index=False)

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
    
    # Open every .csv file and add it to a list `merged`
    for f in files:
        print("Reading in "+f)
        readDF = pd.read_csv(f)
        mergedList.append(readDF)
    
    # Combine all the ingested .csv files into one master DataFrame
    newDF = pd.concat(mergedList)
    
    # De-dup the dataframe
    newDF.drop_duplicates(inplace=True)
    
    # correct for issues with missing spots for address 2
    for index, row in newDF.iterrows():
        # check if the maps url column is null
        if pd.isnull(row.at['Map URL']):
            # if so, slide everything right one cell
            row['Map URL'] = row['City, State Zip']
            row['City, State Zip'] = row['Address 2']
            row['Address 2'] = np.nan
    
    # Divide the `City, State Zip` column into separate columns for each value
    city = []
    state = []
    postal = []
    for result in newDF['City, State Zip'].str.split(', '):
        try:
            city.append(result[0])
            state.append(result[1].split(' ')[0])
            postal.append(result[1].split(' ')[1])
        except:
            pass
    newDF['state'] = state
    newDF['postal'] = pd.to_numeric(postal)    

    # Rename `Business Name` column to `name` & `Map URL` to `googleMapsUrl`
    newDF.rename(columns={'Business Name':'name', 
                          'Map URL':'googleMapsUrl'}, inplace=True)
    
    # Add the source column to state that the data was obtained from the DEA
    newDF['source'] = 'DEA'
    
    # Add the source_date column to the data that was obtained from the DEA
    # string manipulation to obtain MMM-YYYY
    date = (folder_name.split('/')[-1].split('20')[0][0:3] + 
            folder_name.split('/')[-1][-4:])
    newDF['source_date'] = date
    
    # Create the `address` column by combining columns: 
    #   `Address 1`, `Address 2` and `City, State Zip`
    newDF['Address 2'].fillna('', inplace=True)
    temp = (newDF['Address 1'] + ', ' + newDF['Address 2'] + 
            ', ' + newDF['City, State Zip'])
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
                corrected_address = (corrected_address + ' ' + 
                                     address[count].upper())
            count+=1
        corrected_address = (corrected_address + ', ' + 
                             address[count].strip(',') + 
                             ' ' + address[count+1])
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
    
    return newDF

def fixNameErrors(row):
    '''
    Correct missing letters from names in dropbox location datafarmes
    
    Input:
        (1) row (series) - row of the dataframe to evaluate/clean up
    '''
    if row['name'].split(' ')[-1] == 'FOUNDATIO':
        row['name'] = row['name'].replace('FOUNDATIO','FOUNDATION')
    elif row['name'].split(' ')[-1] == 'CORPORATIO':
        row['name'] = row['name'].replace('CORPORATIO','CORPORATION')
    return row

#def similarityRatio(df_row, address_list):
#    '''
#        Calculate the percentage of similarity between an address and all other
#        possible addressed contained in a list
#        
#        Input:
#            (1) df_row (series):  row of a dataframe
#            (2) address_list (list of strings): list of addresses
#    '''
#    score_list = []
#    for address in address_list:
#        score = difflib.SequenceMatcher(None, df_row['address'], address).ratio()
#        if score == 1.0:
#            break
#        if score > 0.75 and score < 1.0:
#            score_list.append([df_row['address'], address_list.index(address), address, score])
#    if df_row.name%100 == 0:
#        print(str(df_row.name))
#    return score_list

#==============================================================================
# Correct spacing/formatting issues in current data that will prevent it from
#   merging properly with new data
#split_list = [x.replace(' ,',',') for x in list(dropbox_data_old['address'])]
#split_list = [x.split() for x in split_list]
#corrected_list = []
#for address in split_list:
#    corrected_address = ''
#    count = 0
#    while count < len(address)-2:
#        if count == 0:
#            corrected_address = address[count].upper()
#        else:
#            corrected_address = corrected_address + ' ' + address[count].upper()
#        count+=1
#    corrected_address = corrected_address + ', ' + address[count].strip(',') + ' ' + address[count+1]
#    corrected_address = corrected_address.replace(',,',',')
#    corrected_list.append(corrected_address)
#dropbox_data_old['address'] = corrected_list
#==============================================================================

#==============================================================================
# Working Code
#==============================================================================
# Set the project working directory
os.chdir('/home/ejreidelbach/projects/HHS/Data/')    

# Import all scraped files and merge into a single dataframe
cleanDropboxLocations('/home/ejreidelbach/projects/HHS/Data/October2018')

dropbox_data_new = ingestDropboxLocations(
        '/home/ejreidelbach/projects/HHS/Data/October2018')

# Read in the `new` version of the map markers from Take-Back America
#dropbox_data_new = pd.read_csv('October2018/combined_TakeBack_Format.csv')

#------------------------------------------------------------------------------
# Data Correction #1: Handle missing 'n' letters from end of words
#------------------------------------------------------------------------------
# several location names were missing the last 'n' from the words 'foundation' 
#   or 'corporation' ---> Fill that in
dropbox_data_new = dropbox_data_new.apply(lambda x: fixNameErrors(x), axis=1)

#------------------------------------------------------------------------------
# Merge Attempt #1:  On `googleMapsUrl`
#------------------------------------------------------------------------------
# Read in the `old` version of the map markers from Take-Back America
dropbox_data_old = pd.read_csv('map_markers_latest.csv')

# Merge the `new` data with the `old` to copy over existing county and geo info
dropbox_data_merged = dropbox_data_new.merge(dropbox_data_old, 
                    how='left', on=['googleMapsUrl'], indicator=True)

# Drop columns that are no longer needed post-merge and rename columns
#   to match the proper formatting for use in Take-Back America
dropbox_data_merged.drop(['name_y', 'address_y', 'state_y', 'postal_y', 'source_y',
                          'source_date_y', '_merge'], axis=1, inplace=True)
dropbox_data_merged.rename(columns={'state_x':'state', 'postal_x':'postal', 
                                    'source_x':'source', 'name_x':'name',
                                    'source_date_x':'source_date',
                                    'address_x':'address'}, inplace=True)

    
#------------------------------------------------------------------------------
# Data Correction #2:  Calculate Simlilarity Scores between rows
#------------------------------------------------------------------------------  
# To help detect possible formatting errors preventing exact matches, we'll
#   employ a similarity score in the form of sequenceMatcher

# Isolate rows that don't have `lat`/`lng` matches from the `merged` version
dfV1 = dropbox_data_merged[dropbox_data_merged['lat'].isna() != False]
    
# Isolate addresses from the `old` version that are not in the `merged` version
dfOuter = dropbox_data_new.merge(dropbox_data_old, 
                    how='outer', on=['googleMapsUrl'], indicator=True)
dfOuter = dfOuter[dfOuter['_merge'] == 'right_only']
dfOuter.drop(['name_x', 'state_x', 'postal_x', 'source_x', 'source_date_x',
              'address_x', '_merge'], axis=1, inplace=True)
dfOuter.rename(columns={'name_y':'name', 'address_y':'address', 
                        'state_y':'state', 'postal_y':'postal', 
                        'source_y':'source', 'source_date_y':'source_date',
                        }, inplace=True)

# Employ a similarity matching scheme on the addresses without matches
score_list = []
address_list = list(dfOuter['address'])
for df_index, df_row in dfV1.iterrows():
    for dfOuter_index, dfOuter_row in dfOuter.iterrows():
        score = difflib.SequenceMatcher(None, df_row['address'], 
                                        dfOuter_row['address']).ratio()
        if score == 1.0:
            break
        if score > 0.75 and score < 1.0:
            score_list.append([score, df_index, dfOuter_index, 
                              df_row['address'], dfOuter_row['address']])
    if df_index%100 == 0:
        print(str(df_row.name))
# convert the list to a dataframe
dfScore = pd.DataFrame(score_list) 
dfScore.columns = ['score','indexNew','indexOld','addressNew','addressOld']

# After manually reviewing scores, determined that all scores (minus a few 
#   outliers) below 0.8 are not matches and should be dropped
dfScore = dfScore[dfScore['score'] >= 0.8]

# Manully drop a few `matches` that proved to not be correct
dropList = [1, 42, 45, 58, 59, 60, 71, 116, 145, 167]
dfScore.drop(index=dropList, inplace=True)

#------------------------------------------------------------------------------
# Data Correction #3: Manually copy matched rows based on similarity scores
#------------------------------------------------------------------------------    
dfCorrected = dfV1[dfV1.index.isin(dfScore['indexNew'])]
for index, row in dfScore.iterrows():
    indexNew = row['indexNew']
    indexOld = row['indexOld']
    dfCorrected.loc[indexNew] = dfOuter.loc[indexOld]

# merge corrected rows back into main dataframe
for index, row in dfCorrected.iterrows():
    dropbox_data_merged.loc[index] = dfCorrected.loc[index]

#------------------------------------------------------------------------------
# Data Correction #4: Use the `geopy_lat_long` function to find lat/long values
#------------------------------------------------------------------------------ 
# Isolate rows that don't have `lat`/`lng` matches from the `merged` version
dfNeedGeos = dropbox_data_merged[dropbox_data_merged['lat'].isna() != False]

# Try using a python library to help us out by geocoding addresses
coordinates = geopy_lat_long(dfNeedGeos)
dfNeedGeos['lat'] = coordinates[0]
dfNeedGeos['lng'] = coordinates[1]
dfNeedGeos[['lat','lng']] = dfNeedGeos[['lat','lng']].apply(pd.to_numeric)

# Merge this data back with our original data
dfMerge = pd.merge(dropbox_data_merged, dfNeedGeos, how='outer', 
                   on=['name','address','postal','source','state','source_date',
                       'googleMapsUrl','county'])

# swap new lat/long values into old ones for all addresses that were missing values
dfMerge.lat_x.fillna(dfMerge.lat_y, inplace=True)
dfMerge.lng_x.fillna(dfMerge.lng_y, inplace=True)
dfMerge.drop(['lat_y', 'lng_y'], axis=1, inplace=True)
dfMerge.rename(columns={'lat_x':'lat','lng_x':'lng'}, inplace=True)

#------------------------------------------------------------------------------
# Data Correction #5: Use the following website to geolocate remaining sites
#       https://geocoding.geo.census.gov/geocoder/locations/addressbatch?form
#------------------------------------------------------------------------------

# Let's see how many missing geos we still have
dfNeedGeos = dfMerge[dfMerge['lat'].isna() != False]

# Remove columns that won't meet criteria for use in website scraping
dfNeedGeos.drop(['lat','lng','name','source','source_date', 'county', 
                 'googleMapsUrl'], axis=1, inplace=True)

#split_address = dfNeedGeos['address'].str.split(',')
address_list = []
city_list = []
for address in dfNeedGeos['address']:
    # remove city
    city = address.split(' ')[-3]
    # strip trailing comma (if necessary)
    if city[-1] == ',':
        city = city[:-1]    
    city_list.append(city)
    
    # extract address while ignoring city, state and zip code
    address_temp = " ".join(str(x) for x in address.split(' ')[:-3])
    # strip trailing comma (if necessary)
    if address_temp[-1] == ',':
        address_temp = address_temp[:-1]
    address_list.append(address_temp)
            
dfNeedGeos['address'] = address_list
dfNeedGeos['city'] = city_list

# correct the type of postal from float to string
dfNeedGeos['postal'] = dfNeedGeos['postal'].apply(lambda x: str(int(x)).zfill(5)).astype(str)

# reorder columns in dataframe to match desired format of website
#   Index -- Address -- City -- State -- Zip Code
dfNeedGeos = dfNeedGeos[['address', 'city', 'state', 'postal']]

# export to csv for use in web scraping
dfNeedGeos.to_csv('October2018_need_geos.csv', header=False)

# Read in newly geolocated data and transform the geocodes into the proper format
# Need to manually insert header
dfNeedGeosV2 = pd.read_csv('October2018_need_geos_geocode_results.csv',
                           names = ['index','address','match_status','match_type',
                                    'match_address','geo','7','8','9','10',
                                    '11','12'], usecols=['index', 'address',
                                             'match_status', 'match_type',
                                             'match_address','geo'])
dfNeedGeosV2.set_index('index', inplace=True)

# split `lat` and `lng` values into their own columns
coordinates = dfNeedGeosV2['geo'].str.split(',')
lat_list = []
lng_list = []
for coords in coordinates:
    if type(coords) is float:
        lat_list.append('')
        lng_list.append('')
    else:
        lat_list.append(coords[1])
        lng_list.append(coords[0])
dfNeedGeosV2['lat'] = lat_list
dfNeedGeosV2['lng'] = lng_list
dfNeedGeosV2.drop(['geo',], axis=1, inplace=True)

# obtain a csv that's "easier" to work with manually
dfNeedGeosV2.to_csv('October2018_need_geos_manual.csv')
#------------------------------------------------------------------------------
# Data Correction #6: Conduct manual location of addresses via Google Maps and
#   use Google's URL to retrieve lat/long coordinates
#------------------------------------------------------------------------------
# read in file once I've finished editing it
dfNeedGeosV3 = pd.read_csv('October2018_need_geos_manual_with_coords.csv')

#   We will use the manually created column `googleMapsUrl` to retrieve 
#       `lat` and `lng` values for each row
locationURL = dfNeedGeosV3['googleMapsUrl']
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
dfNeedGeosV3['lat_new'] = lat_list
dfNeedGeosV3['lng_new'] = lng_list

# move values from new columns into missing values in original columns
dfNeedGeosV3['lat'].fillna(dfNeedGeosV3['lat_new'], inplace=True)
dfNeedGeosV3['lng'].fillna(dfNeedGeosV3['lng_new'], inplace=True)

# Set the index to be the column `Unnamed: 0`
dfNeedGeosV3.set_index('index', inplace=True)

# delete unnecessary columns
dfNeedGeosV3.drop(['lat_new', 'lng_new', 'address','match_address'], axis=1, inplace=True)

# backup file
dfNeedGeosV3.to_csv('October2018_need_geos_manual_with_coords_clean.csv')

# merge values back with the completed values in `dfMerge`
dfMergeV2 = pd.merge(dfMerge, dfNeedGeosV3, how='left', left_index=True, 
                     right_index=True)
dfMergeV2['lat_x'].fillna(dfMergeV2['lat_y'], inplace=True)
dfMergeV2['lng_x'].fillna(dfMergeV2['lng_y'], inplace=True)

# delete unnecessary columns and rename columns back to original form
dfMergeV2.drop(['lat_y', 'lng_y'], axis=1, inplace=True)
dfMergeV2.rename(columns={'lat_x':'lat', 'lng_x':'lng'}, inplace=True)

# Output finalized file with all lat lng coords
dfMergeV2.to_csv('October2018_only_need_counties.csv')

#------------------------------------------------------------------------------
# Data Correction #7: Obtain county level information for all rows without it
#------------------------------------------------------------------------------
# subset the data to only pull out rows that don't have county info
dfCounties = dfMergeV2[dfMergeV2['county'].isnull()]

# Query the FCC API for every row in the database
#   The site can be found here:  https://geo.fcc.gov/api/census/#!/area/get_area
county_list = []
for index, row in dfCounties.iterrows():
    url = 'https://geo.fcc.gov/api/census/area?lat='+ str(
            row['lat']) + '&lon=' + str(row['lng']) + '&format=json'
    r = requests.get(url)
    try:
        county_name = r.json()['results'][0]['county_name']
        county_list.append(county_name)
    except:
        county_list.append('')
    print('County Name for ' + str(index) + ': ' + str(county_name))
    time.sleep(1)
    
dfCounties['county'] = county_list

# Merge the main DF with the county DF and copy over values where the
#   main DF is lacking county level info
dfMergeV3 = pd.merge(dfMergeV2, dfCounties, how='left', left_index=True, 
                     right_index=True, on=['name', 'googleMapsUrl', 'state', 
                                           'postal', 'source', 'source_date',
                                           'address', 'lat', 'lng']) 
dfMergeV3['county_x'].fillna(dfMergeV3['county_y'], inplace=True)
dfMergeV3.drop(['county_y'], axis=1, inplace=True)
dfMergeV3.rename(columns={'county_x':'county'}, inplace=True)

# correct the type of postal from float to string
dfMergeV3['postal'] = dfMergeV3['postal'].apply(lambda x: int(x))

# Export the dataframe
dfMergeV3.to_csv('map_markers_October2018.csv')

#------------------------------------------------------------------------------
# Data Correction #7: Finalize the file by outputting it to JSON format
#------------------------------------------------------------------------------

# convert the dataframe to a dictionary for export to .json (nicer formatting)
mergedDict = dfMergeV3.to_dict('records')
filename = 'map_markers_October2018.json'
with open(filename, 'wt') as out:
    json.dump(mergedDict, out, sort_keys=True, indent=4, separators=(',', ': '))