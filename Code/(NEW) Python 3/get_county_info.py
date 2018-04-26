#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 14:44:46 2018

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

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
os.chdir(r'/home/ejreidelbach/projects/HHS/Data/')

# Read in DF
df = pd.read_csv('April2018_FINAL.csv')

# Query the FCC API for every row in the database
#   The site can be found here:  https://geo.fcc.gov/api/census/#!/area/get_area

county_list = []
for index, row in df.iterrows():
    url = 'https://geo.fcc.gov/api/census/area?lat='+ str(
            row['lat']) + '&lon=' + str(row['lng']) + '&format=json'
    if index == 3281:
        county_list.append('El Paso')
        continue
    r = requests.get(url)
    county_name = r.json()['results'][0]['county_name']
    county_list.append(county_name)
    print('County Name for ' + str(index) + ': ' + str(county_name))
    time.sleep(1)


#df.drop(['name', 'source', 'source_date', 'lat', 'lng', 'googleMapsUrl'], axis=1, inplace=True)
#
#addresses = df['address']
#split_list = []
#for address in addresses:
#    temp = address.split(',')
#    storage = [''] * 6
#    for i in range(1,len(temp)+1):
#        storage[-i] = temp[-i].strip()
#    split_list.append(storage)
#    
#addressesDF = pd.DataFrame(split_list)
#addressesDF.to_csv()