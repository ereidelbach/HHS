#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 14:36:45 2017

@author: ejreidelbach

:DESCRIPTION:

:REQUIRES:
   
:TODO:
    
:NOTES:
    
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
"""

#==============================================================================
# Package Import
#==============================================================================
import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
#from joblib import Parallel, delayed
from multiprocessing import Pool
import itertools
from itertools import groupby
from operator import itemgetter

#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

dtype_dict = {'zip':str, 'state':str}

def zip_code_list():
    '''
        Purpose: Read in the US zip codes found at 
                 https://www.unitedstateszipcodes.org and stored locally in the 
                 project folder in the file `zip_code_database.xls`, extract 
                 the zip code column, remove any potential duplicates, and 
                 store return the codes as a list.
                 
        Input:
            -NONE-
            
        Output: 
            (1) tempList: a list containing all the zipcodes in the U.S. (list)
    '''
    # Read in the csv from a local directory and drop any duplicates
    tempZipDF = pd.read_csv('Data/zip_code_database_US.csv', 
                            usecols=[0], dtype=dtype_dict)
    tempZipDF.drop_duplicates(inplace=True)
    tempList = list(tempZipDF['zip'])
    
    # Add leading zeros to all combinations less than 10000
    tempList = [j.zfill(5) for j in tempList]
    
    return tempList
    
def dedup_list(temp_list):
    '''
        Purpose: Function that iterates through a list and removes any 
                 duplicate entires (similar to the 'drop_duplicates' function
                 available in Pandas DataFrames).
                 
        Input:
            (1) temp_list: list to be de-duped (list)
            
        Output: list in which no duplicate entries exist
    '''
    temp_list.sort()
    return list(k for k,_ in itertools.groupby(temp_list))

def scrape_page(browser, zipcode):
    '''
        Purpose: Function that will scrape a the DEA Diversion Control Division
                 website in order to obtain an up-to-date listing of all 
                 controlled substance public disposal locations for one 
                 specific zip code. This will be accomplished by entering a zip 
                 code and specifying a search radius of 50 miles which will 
                 then return an html table of all dropbox addresses within that 
                 search radius.
    
        Input:   
            (1) browser: browser page to be scraped (webdriver)
            (2) zipcode: zipcode to be searched for new locations (string)
        
        Output:  beautifulSoup data containing dropbox addresses
                 - may also return 'NONE_FOUND' or 'BAD_ZIP' if the search
                 did not result in valid dropbox locations
    '''
    # Input the zip code into the page
    zipElem = browser.find_element_by_xpath('//*[@id="zip"]')
    zipElem.clear() # clear the box in case any previous data exists
    zipElem.send_keys(zipcode)
    
    # Specify the maximum radius of 100 miles
    desired_button = browser.find_element_by_xpath(
            '//*[@id="radius100"]')          
    desired_button.click()
    
    # Click the submit button
    search_button = browser.find_element_by_xpath('/html/body/div[3]/div/form/input[1]')
    search_button.click()
    
    # Use beautifulSoup to extract the dropbox data from the generated page
    html = browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    # Test to make sure there are valid results
    status_noresults = len(soup.findAll('tr'))
    status_badzip = soup.find('div',{'class':'validation'})
    if status_noresults <= 1:
        return 'NONE_FOUND'
    elif status_badzip is not None:
        return 'BAD_ZIP'
    else:    
        #dropboxTable = soup.findAll('table', role='grid')[0]
        return(soup.findAll('table')[0])

def scrape_dea(zips):
    '''
        Purpose: Function that will scrape dropbox locations for all zip 
                 codes contained in the list `zips`.
    
        Input:   zips (list): list of zip codes
        
        Output:  CSV file containing all unique dropbox locations for the 
                 zip codes specified in `zips`
    '''
    # Open a Headless Firefox web browser and direct it to the DEA's dropbox 
    #   search page
    options = Options()
    options.set_headless(headless=True)
    browser = webdriver.Firefox(firefox_options=options)
#    browser = webdriver.Firefox()
    browser.get('https://apps.deadiversion.usdoj.gov/SEARCH-NTBI')
    browser.implicitly_wait(10)
    
    # final storage container for dropbox locations
    dropboxList = []
    
    count = 0
    # For every zip code in the US, run the dropbox location search on the site
    for code in zips:
        status = ''
        status = scrape_page(browser, code)
        
        # If we find a bad zip code or no dropbox locations are found in a 
        #   zip, skip to the next zip code
        if status == 'NONE_FOUND' or status == 'BAD_ZIP':
            browser.get('https://apps.deadiversion.usdoj.gov/SEARCH-NTBI')
            continue
        else:
            # For every column in a row in the dropbox location table, grab  
            #   the data and place it in the list `rowList`.  After each  
            #   row is read, add that data to the master list `dropboxList`.  
            for tr in status.findAll('tr')[2:-1]:
                rowList = []
                for td in tr.findAll('td')[:4]:
                    # Removes special characters from any entries
#                    text = td.text.encode('utf-8').replace('\xd1',"'").replace(
#                            '\xbf',"'").decode('ascii','ignore').strip()
                    text = td.text.strip()
                    rowList.append(' '.join(text.split()))
                # Convert state, zip to separate columns
                stateAndZip = tr.findAll('td')[4]
                rowList.append(stateAndZip.text.split(', ')[0])
                rowList.append(stateAndZip.text.split(', ')[1])
                # Convert map data to a google link
                mapURL = 'http://maps.google.com/maps?q=' + tr.find(
                        'a', href=True)['href'].split('daddr=')[1]
                rowList.append(mapURL)
                dropboxList.append(rowList)       

            # Deduplicate the list whenever it exceeds a size greater than 500
            if len(dropboxList) > 500:
                dropboxList = dedup_list(dropboxList)
            
            # Print status update after every 25 zip codes have been scraped
            count+=1
            if count%25 == 0:
                print('Completed ' + str(
                        round(((zips.index(code) / len(zips)) * 100), 2)) 
                    + '% of list')
                
            # After scraping every 100 zip codes, export the list to a csv file                
            if count%100 == 0:
                print('Printing file: ' + code)
                # Convert storage container into pandas dataframe
                dropboxDF = pd.DataFrame(dropboxList, 
                                         columns = ['Participant',
                                                    'CollectionSite',
                                                    'Address',
                                                    'City',
                                                    'State',
                                                    'Zip',
                                                    'MapURL'])
                
                # Remove duplicate entries caused by querying nearby zip codes
                dropboxDF.drop_duplicates(inplace=True)
                
                # Export dataframe to CSV file in the working directory
                filename = ('Data/October2018_NTBI/dropbox_addresses_October2018_' 
                            + str(code) + '.csv')
                dropboxDF.to_csv(filename, index=False)
                dropboxList = []
                dropboxDF = pd.DataFrame()  
            browser.back()
            
    # Output the final batch of locations
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
    filename = ('Data/October2018_NTBI/dropbox_addresses_October2018_' 
                + str(code) + '.csv')
    dropboxDF.to_csv(filename, index=False)    
    
    # Close the browser
    browser.close()
    return

#==============================================================================
# Working Code
#==============================================================================

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

#for codes in zipBreakDown:
#    scrape_dea(codes)

# Run the DEA scrape function (`scrape_dea`) for every zip code on file
#   This code spreads the work over all available cores on the computer
p = Pool(1)  # Pool tells how many at a time
p.map(scrape_dea, zipBreakDown)
p.terminate()
p.join()