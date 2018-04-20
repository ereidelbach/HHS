#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 14:36:45 2017

@author: ejreidelbach
"""

import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from multiprocessing import Pool
import itertools
from itertools import groupby
from operator import itemgetter


'''
    NOTE 1: For the Selenium driver to function properly on Ubuntu, I had to 
            download the most up-to-date geckodriver found at:
            https://github.com/mozilla/geckodriver/releases
                
            Once that is complete, extract the driver and place it in the
            /us/local/bin folder
           
    NOTE 2: An effective selenium guide can be found here:
            https://automatetheboringstuff.com/chapter11/
            
            The relevant contents begin roughly 3/4 down the page.
            
    NOTE 3: Instructions for multiprocessing in Python:
            https://blog.dominodatalab.com/simple-parallelization/
            
    NOTE 4: Instructions for installing the PhantomJS headless web browser
            on Ubuntu can be found here:
            https://www.vultr.com/docs/how-to-install-phantomjs-on-ubuntu-16-04    
'''

###############################################################################
# Function Definitions / Reference Variable Declaration

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

dtype_dict = {'zip':str, 'state':str}

def zip_code_list():
    '''
        Read in the US zip codes found at https://www.unitedstateszipcodes.org
        and stored locally in the project folder in the file `zip_code_database.xls`
        
        Extract the zip code column, remove any potential duplicates, and store
        return the codes as a list.
    '''
    tempZipDF = pd.read_csv('Data/zip_code_database_US.csv', usecols=[0], dtype=dtype_dict)
    #tempZipDF = tempZipDF.loc[tempZipDF['state'].isin(states)]
    tempZipDF.drop_duplicates(inplace=True)
    tempList = list(tempZipDF['zip'])
    
    # Add leading zeros to all combinations less than 10000
    tempList = [j.zfill(5) for j in tempList]
    
    return tempList
    
def dedup_list(temp_list):
    temp_list.sort()
    return list(k for k,_ in itertools.groupby(temp_list))

def scrape_page(browser, code):
    '''
        Purpose: Function that will scrape a the DEA Diversion Control Division
                 National Take Back Initiative website for April 2018
                 in order to obtain an up-to-date listing of all 
                 controlled substance public disposal locations for one specific
                 zip code. This will be accomplished by entering a zip code and 
                 specifying a search radius of 50 miles which will then return 
                 an html table of all dropbox addresses within that search radius.
    
                National Take Back Day is Saturday, April 28, 2018 from 
                    10:00 am to 2:00 pm
    
        Input:   browser page
        
        Output:  beautifulSoup data containing dropbox addresses
                 - may also return 'NONE_FOUND' or 'BAD_ZIP' if the search
                 did not result in valid dropbox locations
    '''
    # Input the zip code into the page
    zipElem = browser.find_element_by_xpath('//*[@id="zip"]')
    zipElem.clear() # clear the box in case any previous data exists
    zipElem.send_keys(code)
    
    # Specify the maximum radius of 100+ miles
    desired_button = browser.find_element_by_xpath('//*[@id="radius100"]')
    desired_button.click()
    
    # Click the submit button
    search_button = browser.find_element_by_xpath('/html/body/div[3]/div/form/input[1]')
    search_button.click()
    
    # Use beautifulSoup to extract the dropbox data from the generated page
    html = browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    # Test to make sure there are valid results
    status_validation = soup.find('div',{'class':'validation'})
    if status_validation is not None:
        return 'NONE_FOUND'
    else:    
        return(soup.findAll('table')[0])

def scrape_dea(zips):
    '''
        Purpose: Function that will scrape dropbox locations for all zip 
                 codes contained in the list `zips`.
    
        Input:   zips (list): list of zip codes
        
        Output:  CSV file containing all unique dropbox locations for the 
                 zip codes specified in `zips`
    '''
    # Open a Headless Firefox web browser and direct it to the DEA's dropbox search page
    options = Options()
    options.set_headless(headless=True)
    browser = webdriver.Firefox(firefox_options=options)
    #browser = webdriver.Firefox()
    browser.get('https://apps.deadiversion.usdoj.gov/NTBI/ntbi-pub.pub?_flowId=public-flow')
    browser.implicitly_wait(100)
    
    # final storage container for dropbox locations
    dropboxList = []
    count = 0
    # For every zip code in the US, run the dropbox location search on the site
    for code in zips:
        status = ''
        status = scrape_page(browser, code)
        
        # If we find a bad zip code or no dropbox locations are found in a zip,
        #   skip to the next zip code
        if status == 'NONE_FOUND' or status == 'BAD_ZIP':
            browser.get('https://apps.deadiversion.usdoj.gov/NTBI/ntbi-pub.pub?_flowId=public-flow')
            continue
        else:
            # For every column in a row in the dropbox location table, grab the data 
            #   and place it in the list `rowList`.  After each row is read, add that 
            #   data to the master list `dropboxList`.  
            for tr in status.findAll('tr')[2:-1]:
                rowList = []
                for td in tr.findAll('td')[:4]:
                    rowList.append(td.text.strip())
                # Convert state, zip to separate columns
                stateAndZip = tr.findAll('td')[4]
                rowList.append(stateAndZip.text.split(', ')[0])
                rowList.append(stateAndZip.text.split(', ')[1])
                # Convert map data to a google link
                mapURL = 'http://maps.google.com/maps?q=' + tr.find(
                        'a', href=True)['href'].split('daddr=')[1]
                rowList.append(mapURL)
                dropboxList.append(rowList) 
            count+=1
            if count%50 == 0:
                print(count)
                print('length of list (BEFORE):' + str(len(dropboxList)))
                dropboxList = dedup_list(dropboxList)
                print('length of list (AFTER):' + str(len(dropboxList)))
            elif count%5 == 0:
                dropboxList = dedup_list(dropboxList)                
            if count%100 == 0:
                print('Printing file: ' + code)
                # Convert storage container into pandas dataframe
                dropboxDF = pd.DataFrame(dropboxList, columns = ['Participant',
                                                                 'CollectionSite',
                                                                 'Address',
                                                                 'City',
                                                                 'State',
                                                                 'Zip',
                                                                 'MapURL'])
                
                # Remove any duplicate entries caused by querying nearby zip codes
                dropboxDF.drop_duplicates(inplace=True)
                
                # Export dataframe to CSV file in the working directory
                filename = 'Data/NTBI_April2018/dropbox_addresses_April2018_' + str(code) + '.csv'
                dropboxDF.to_csv(filename, index=False)
                dropboxList = []
                dropboxDF = pd.DataFrame()  
            browser.back()
    browser.close()
    return

###############################################################################
# Working Code

# Set the project working directory
os.chdir('/home/ejreidelbach/projects/HHS/')

# Obtain a list of all US zip codes
zipList = zip_code_list()

# Separate zip codes by their starting number (i.e. 0, 1, 2, etc...)
zipBreakDown = []
for digit, zips in groupby(sorted(zipList), key=itemgetter(0)):
    tmp_list = []
    for zip in zips:
        tmp_list.append(zip)
    zipBreakDown.append(tmp_list)

# Run the DEA scrape function (`scrape_dea`) for every zip code on file
#   This code spreads the work over all available cores on the computer
p = Pool(2)  # Pool tells how many at a time
p.map(scrape_dea, zipBreakDown)
p.terminate()
p.join()


