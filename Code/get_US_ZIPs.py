#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 09:16:04 2018

@author: ejreidelbach

:DESCRIPTION:
    - This script will scrape the USPS postal service website to obtain an 
    accurate and up-to-date listing of all US Zip Codes
    - relies on the link: https://tools.usps.com/zipcodelookup/citybyzipcode

:REQUIRES:
    - Selenium
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
            
:TODO:
    - None
"""
 
#==============================================================================
# Package Import
#==============================================================================
import os  
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
import time
import pandas as pd

#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
os.chdir(r'/home/ejreidelbach/projects/HHS')

# Establish the full range of possible zipe code combinations
zip_code_list = list(range(1,99999))

# Convert the list to strings
zip_code_list = [str(i) for i in zip_code_list]

# Add leading zeros to all combinations less than 10000
zip_code_list = [j.zfill(5) for j in zip_code_list]

# establish default header information
headers = {"User-agent":
           "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}

# establish the main link for all searches
url = 'https://tools.usps.com/go/zip-code-lookup.htm'

# Open a Firefox web browser and direct it to the USPS' zipcode search page
#options = Options()
#options.set_headless(headless=True)
#browser = webdriver.Firefox(firefox_options=options)

browser = webdriver.Firefox()

#options = webdriver.ChromeOptions()
#options.binary_location = 'usr/bin/google-chrome-stable'
#options.add_argument('headless')
#browser = webdriver.Chrome(chrome_options=options)

# wait up to 10 seconds for the elements to become available
browser.implicitly_wait(10)

# Load the initial look up page
browser.get(url)

# Specify that we want to search by zip code
zip_search_button = browser.find_element_by_xpath(
        '/html/body/div[2]/div/div/div[3]/div[3]/a')
zip_search_button.click()

official_zip_list = []
count = 0
for zip in zip_code_list[500:]:
    try:        
        # Input the zip code into the page
        zipElem = browser.find_element_by_xpath(
                '//*[@id="tZip"]')
        zipElem.clear() # clear the box in case any previous data exists
        zipElem.send_keys(zip)
        
        # Click the submit button
        search_button = browser.find_element_by_xpath(
                '//*[@id="cities-by-zip-code"]')
        search_button.click()
        
        # Use beautifulSoup to extract the dropbox data from the generated page
        html = browser.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # Keep track of where we are
        if count%100==0:
            print('Searched: ' + str(count) + ' zip codes')
            print('List size is: ' + str(len(official_zip_list)))
        
        count+=1        
        
        # If results were retrieved, the zip code is valid and it will be added
        #   to the official zip list.  If the error message: "You did not enter
        #   a valid ZIP code" then the zip code is discarded.
        errorSoup = soup.find('div',{'class':'server-error cities-by-zipcode-tZip help-block'})
        if errorSoup is None:
            tempList = []
            tempList.append(zip) # Zip Code
            tempList.append(soup.find('p',{'class':'row-detail-wrapper'}).text.split(' ')[0]) # City
            tempList.append(soup.find('p',{'class':'row-detail-wrapper'}).text.split(' ')[1]) # State
            official_zip_list.append(tempList)
            time.sleep(.300)
        
        # reset the page by selecting the `Look Up Another ZIP Code` button
        look_up_button = browser.find_element_by_xpath(
                '//*[@id="look-up-another-zip-code-citybyzipcode"]')
        look_up_button.click()
    except:
        pass
    
# Write the contents of the playerList to a .csv file
filename = 'Data/us_zips_2018.csv'
df = pd.DataFrame(official_zip_list, columns = ['Zip','City','State'])
            
# Remove any duplicate entries caused by querying nearby zip codes
df.drop_duplicates(inplace=True)

# Export dataframe to CSV file in the working directory
df.to_csv(filename, index=False)