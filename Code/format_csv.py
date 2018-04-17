#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 12:23:47 2017

@author: ejreidelbach
"""

import pandas as pd
import os
import csv

'''
    This file ingests CSV files that have been scraped from various
    websites and aligns their formatting to match that required
    by Dillon's UI code
'''

# Set the project working directory
os.chdir('/home/ejreidelbach/projects/HHS')

# Read in the DEA data
dropboxDF = pd.read_csv('dropbox_addresses.csv', index_col = None)
dropboxDF = dropboxDF.fillna('')

# Create a new dataframe in which we will store the newly created columns
deaDF = pd.DataFrame()

# Insert `name` column
deaDF['name'] = dropboxDF['Business Name']

# Insert `source` column
deaDF['source'] = 'DEA'

# Create `address` column by merging `Address 1`, `Address 2`, & `City,State,Zip`
address_list = []
for row in dropboxDF.iterrows():
    if row[1][2] == '':
        address_list.append(str(row[1][1]) + str(', ') + str(row[1][3]))
    else:
        address_list.append(str(row[1][1]) + str(', ') + str(row[1][2]) + str(', ') + str(row[1][3]))
deaDF['address'] = address_list

# Place the data from the `City,State,Zip` column into a list
zip_postal_list = dropboxDF['City,State,Zip'].str.split(', ',1).tolist()

# Insert `postal` column
postalList = [x[1].split(' ')[1] for x in zip_postal_list]
deaDF['postal'] = postalList

# Insert `state` column
stateList = [x[1].split(' ')[0] for x in zip_postal_list]
deaDF['state'] = stateList

# Insert `city` column
cityList = [x[0] for x in zip_postal_list]
deaDF['city'] = cityList

# Create a file that meets the format of the US Census GeoCoder
colList = ['unique id','street address','state','city','zip code']
censusDF = pd.DataFrame(columns=colList)
censusDF['unique id'] = deaDF.index
address_listv2 = []
for row in dropboxDF.iterrows():
    if row[1][2] == '':
        address_listv2.append(str(row[1][1]))
    else:
        address_listv2.append(str(row[1][1]) + str(', ') + str(row[1][2]))
censusDF['street address'] = address_listv2
censusDF['state'] = stateList
censusDF['city'] = cityList
censusDF['zip code'] = postalList
censusDF.to_csv('censusDF.csv',index=False,quoting = csv.QUOTE_NONNUMERIC)

# Insert `latitude` column
deaDF['lat'] = latitude_list

# Insert `longitude` column
deaDF['lng'] = longitude_list

deaDF.to_csv('deaDF.csv',index=False)