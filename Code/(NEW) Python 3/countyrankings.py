#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 10:07:17 2018

@author: ejreidelbach

:DESCRIPTION:

:REQUIRES:
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import os  
import json
import pandas as pd
import seaborn as sb
import matplotlib.pylab as plt
from scipy import stats
#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
os.chdir(r'/home/ejreidelbach/projects/takeback-america/workspaces/hhs-api/src/scripts')

# Set the desired file to read data frame
file = ('sanitizedCounties.json')

# Read in the .json file with county information for each state
with open(file, 'r') as f:
    jsonFile = json.load(f)  
    
# For every state in the data, create state-level rankings
for key, value in jsonFile.items():
    # convert state dictionary to a dataframe
    stateDF = pd.DataFrame.from_dict(value, orient='index')
    # handle Take Back Sites per 100,000
    stateDF['DropBox_Rate_rank_state'] = stateDF[
            'DropBox_Rate'].rank(ascending=0, method='min')
    # handle Drug Poisoning Death Rate Ranking
    stateDF['Drug_Poison_Rate_rank_state'] = stateDF[
            'Drug_Poison_Rate'].rank(ascending=1, method='min')
    # handle Adults Health Ranking
    stateDF['Fair_Poor_Rate_rank_state'] = stateDF[
            'Fair_Poor_Rate'].rank(ascending=1, method='min')
    # handle Opioid Prescriptions Per 100 Ranking
    stateDF['OpioidRX_Rate_rank_state'] = stateDF[
            'OpioidRX_Rate'].rank(ascending=1, method='min')
    # handle Overall Risk Score Ranking
    stateDF['Overall_rank_state'] = stateDF[
            'Overall'].rank(ascending=1, method='min')   
    # convert dataframe with rankings info back into dictionary format
    jsonFile[key] = stateDF.to_dict(orient='index')
    
# Create nation-level rankings
nationDF = pd.DataFrame()
for key, value in jsonFile.items():
    # convert state dictionary to a dataframe
    stateDF = pd.DataFrame.from_dict(value, orient='index')
    # retrieve the FIPS codes for every county and append the 2 digit state code
    fips = stateDF.index.tolist()
    fips_dict = {}
    for county in fips:
        fips_dict[county] = key+county
    # set the index of the state values to include the full 5 digit code 
    stateDF.rename(index=fips_dict, inplace=True)
    # append state to national dataframe
    nationDF = nationDF.append(stateDF)

###############################################################################
# Create Ranking variables at a national level
    
# handle Take Back Sites per 100,000
nationDF['DropBox_Rate_rank_nation'] = nationDF[
        'DropBox_Rate'].rank(ascending=0, method='max')
# handle Drug Poisoning Death Rate Ranking
nationDF['Drug_Poison_Rate_rank_nation'] = nationDF[
        'Drug_Poison_Rate'].rank(ascending=1, method='min')
# handle Adults Health Ranking
nationDF['Fair_Poor_Rate_rank_nation'] = nationDF[
        'Fair_Poor_Rate'].rank(ascending=1, method='min')
# handle Opioid Prescriptions Per 100 Ranking
nationDF['OpioidRX_Rate_rank_nation'] = nationDF[
        'OpioidRX_Rate'].rank(ascending=1, method='min')
# handle Overall Risk Score Ranking
nationDF['Overall_rank_nation'] = nationDF[
        'Overall'].rank(ascending=1, method='min')    

# computer percentile ranks

# handle Take Back Sites per 100,000
nationDF['DropBox_Rate_pct_nation'] = nationDF[
        'DropBox_Rate'].rank(ascending=1, pct=True, method='min')
# handle Drug Poisoning Death Rate Ranking
nationDF['Drug_Poison_Rate_pct_nation'] = nationDF[
        'Drug_Poison_Rate'].rank(ascending=0, pct=True, method='max')
# handle Adults Health Ranking
nationDF['Fair_Poor_Rate_pct_nation'] = nationDF[
        'Fair_Poor_Rate'].rank(ascending=0, pct=True, method='max')
# handle Opioid Prescriptions Per 100 Ranking
nationDF['OpioidRX_Rate_pct_nation'] = nationDF[
        'OpioidRX_Rate'].rank(ascending=0, pct=True, method='max')
# handle Overall Risk Score Ranking
nationDF['Overall_pct_nation'] = nationDF[
        'Overall'].rank(ascending=0, pct=True, method='max')    

# sort nationDF by index
nationDF.index = nationDF.index.astype(int)
nationDF = nationDF.sort_index()
nationDF.index = nationDF.index.astype(str)

# create new json file with state and national rankings
state_fips = ''
nation_dict = {}
state_dict = {}
for row in nationDF.iterrows():
    index = row[0]
    if len(index) < 5:
        index = '0' + index
    if index[:2] != state_fips:
        # add state to national dict
        if len(state_dict) > 0:
            nation_dict[state_fips] = state_dict
        # reset state fips code and state dictionary
        state_fips = index[:2]
        state_dict = {}
    state_dict[index[2:]] = row[1].to_dict()
nation_dict[state_fips] = state_dict

# Output dictionary to file
output_fname = 'sanitzedCountiesWithRankings.json'
with open(output_fname, 'wt') as out:
    json.dump(nation_dict, out, sort_keys=True)
#    json.dump(nation_dict, out, sort_keys=True, indent=4, separators=(',', ': '))