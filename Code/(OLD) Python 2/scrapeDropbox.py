#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 14:36:45 2017

@author: ejreidelbach
"""

import pandas as pd
import os
from selenium import webdriver
from bs4 import BeautifulSoup

'''
    NOTE 1: For the Selenium driver to function properly on Ubuntu, I had to 
            download the most up-to-date geckodriver found at:
            https://github.com/mozilla/geckodriver/releases
                
            Once that is complete, extract the driver and place it in the
            /us/local/bin folder
           
    NOTE 2: An effective selenium guide can be found here:
            https://automatetheboringstuff.com/chapter11/
            
            The relevant contents begin roughly 3/4 down the page.
            
    NOTE 3: Alternative source for dropbox data:
            http://rxdrugdropbox.org/
'''

###############################################################################
# Function Definitions / Reference Variable Declaration

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

dtype_dict = {'zip':str,
         'state':str}

def zip_code_list():
    '''
        Read in the US zip codes found at https://www.unitedstateszipcodes.org
        and stored locally in the project folder in the file `zip_code_database.xls`
        
        Extract the zip code column, remove any potential duplicates, and store
        return the codes as a list.
    '''
    tempZipDF = pd.read_csv('zip_code_database.csv', usecols=[0,6], dtype=dtype_dict)
    tempZipDF = tempZipDF.loc[tempZipDF['state'].isin(states)]
    tempZipDF.drop_duplicates(inplace=True)
    tempList = tempZipDF['zip']   
    return tempList
    
###############################################################################
# Working Code

# Set the project working directory
os.chdir('/home/ejreidelbach/projects/HHS/')

# Obtain a list of all US zip codes
zipList = zip_code_list()

# -----------------------------------------------------------------------------
# Start working with Selenium to iterate through the DEA website
#   The website requires the user to enter in zip codes in order to obtain
#   dropboxes that are "near" that location.  We will automate this task
#   using the list of 40,000+ zip codes (`zipList`)
# -----------------------------------------------------------------------------

# Alternative:  use the headless browser `PhantomJS`
#   Use the instructions found here to install PhantomJS on Ubuntu: 
#       https://www.vultr.com/docs/how-to-install-phantomjs-on-ubuntu-16-04
# browser = webdriver.PhantomJS()

# Open a Firefox web browser and direct it to the DEA's dropbox search page
browser = webdriver.Firefox()
browser.get('https://apps.deadiversion.usdoj.gov/pubdispsearch')
browser.implicitly_wait(500)

# storage variable for table column names
columnNames = []

# final storage container for dropbox locations
dropboxList = []

# For every zip code in the US, run the dropbox location search on the site
for code in zipList:

    try:
        # Input the zip code into the page
        zipElem = browser.find_element_by_id('searchForm:zipCodeInput')
        zipElem.clear() # clear the box in case any previous data exists
        zipElem.send_keys(code)
        
        # Specify the maximum radius of 50 miles
        desired_button = browser.find_element_by_xpath(
                '/html/body/div[1]/div[2]/div/div/div[2]/form/div[10]/table/tbody/tr/td[7]/div/div[2]/span')
        desired_button.click()
        
        # Click the submit button
        search_button = browser.find_element_by_id('searchForm:submitSearchButton')
        search_button.click()
        
        # Use beautifulSoup to extract the dropbox data from the generated page
        html = browser.page_source
        soup = BeautifulSoup(html, 'lxml')
        dropboxTable = soup.findAll('table', role='grid')[0]
        
        # On the first iteration, grab column names from the dropbox location table
        if code == zipList[0]:
            tableHeader = dropboxTable.find('tr')
            th = tableHeader.findAll('th')
            for col in th:
                columnNames.append(col.text)
        
        # For every column in a row in the dropbox location table, grab the data 
        #   and place it in the list `rowList`.  After each row is read, add that 
        #   data to the master list `dropboxList`.  
        for tr in dropboxTable.findAll('tr')[1:]:
            rowList = []
            for td in tr.findAll('td'):
                rowList.append(td.text)
            dropboxList.append(rowList)
        
        # Move back to the search page and start over
        browser.back()
    except:
        pass
    
# Convert storage container into pandas dataframe
dropboxDF = pd.DataFrame(dropboxList, columns = columnNames)

# Delete the `map` and `distance` columns as they are not relevant
dropboxDF.drop('Map ', axis=1, inplace=True)
dropboxDF.drop('Distance', axis=1, inplace=True)

# Remove any duplicate entries caused by querying nearby zip codes
dropboxDF.drop_duplicates(inplace=True)

# Export dataframe to CSV file in the working directory
dropboxDF.to_csv('dropbox_addresses.csv', index=False)