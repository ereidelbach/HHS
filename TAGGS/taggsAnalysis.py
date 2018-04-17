#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  9 12:04:19 2018

@author: ejreidelbach
"""

#------------------------------------------------------------------------------
# Analyze the HHS TAGGS data to determine what are commonalities among winning
#   bids for SBIR proposals.
#
#   Website:  https://taggs.hhs.gov/SearchAward
#
#------------------------------------------------------------------------------

# Import the Required Libraries
import pandas as pd
import os
import pandas_profiling

###############################################################################
# Function Definitions

###############################################################################
# Working Code

# Set the project working directory
os.chdir('/home/ejreidelbach/projects/HHS')

# Read in the TAGGS data
taggsDF = pd.read_excel('taggsExport2018.xlsx', index_col = None)

# Delete several columns which only have missing data
del taggsDF['Unnamed: 4']
del taggsDF['Unnamed: 7']
del taggsDF['Unnamed: 9']
del taggsDF['Unnamed: 11']
del taggsDF['Unnamed: 15']

# Start Counts of Individual Variables
taggsDF['OPDIV'].value_counts()[:11]                # Operating Division
taggsDF['CFDA Num'].value_counts()                  # CFDA Number
taggsDF['CFDA Program Title'].value_counts()[:25]   # CFDA Program Title
taggsDF['State'].value_counts()[:10]                # State
taggsDF['Award Title'].value_counts()[:30]          # Award Title
taggsDF['Recipient Name'].value_counts()[:30]       # Recipient Name

pandas_profiling.ProfileReport(taggsDF)
