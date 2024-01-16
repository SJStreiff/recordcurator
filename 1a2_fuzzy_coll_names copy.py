#!/usr/bin/env python3.10
# -*- coding: utf-8 -*-
"""

2022-11-15 sjs

Now i have nice collector names from 1a_name...
I do a fuzzy match with the master list *continuously updated version, of clean names, 
and then have more complete names

"""


import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import x_1_cols

import rapidfuzz
from rapidfuzz.process import extractOne
#from rapidfuzz.string_metric import levenshtein, normalized_levenshtein
from rapidfuzz.distance.Levenshtein import normalized_similarity
#from rapidfuzz.distance.Levenshtein import normalized_similarity
from rapidfuzz.fuzz import ratio
from rapidfuzz.process import extract



print(os.getcwd())
os.chdir("/Users/fin/Sync/1_Annonaceae/4_DB")
print('The working directory is: \n', os.getcwd())









###########----_--------###########---------_-----###########----_--------###########---------_-----###########----_--------###########---------_-----
# PSEUDOCODE

"""
read dataframe (database)
get unique names in recorded_by + original + and huh
    go through names with fuzzy where match
    
    d o huh with  rest, if multiple matches get all
        redo fuzzy on all options


    done


"""
# PSEUDOCODE
###########----_--------###########---------_-----###########----_--------###########---------_-----###########----_--------###########---------_-----


# read data
#-------------------------------------------------------------------------------
#importfile = './1_WIP_data/GBIF_run1.csv'
in_file = './1_WIP_data/1a_GBIF_names_standardised.csv'
out_dir = './1_WIP_data/'
run_name = '1a2_GBIF_'
# in theory it should not need a distinction between the different data types here....
#-------------------------------------------------------------------------------
#importfile = "./1_WIP_data/HERBO_names_standardised.csv"
#importfile = "./1_WIP_data/Asia_HERBO_run1.csv"
out_dir = './1_WIP_data/'
check_dir = 'to_check/'
# out_file = 'HERBO_names_checked_nodup.csv'
#data_source_type = 'P'
#-------------------------------------------------------------------------------

imp = codecs.open(in_file, 'r', 'utf-8')  # open for reading with "universal" type set
occs = pd.read_csv(imp, sep=';')

# check reading correct
print(occs.head())

# the namelist is the cleaned unique collector names that is curated.
standard_list_file = './1_standard_data/collectors_Philipp.csv'
standards = pd.read_csv(standard_list_file)
print('Standards:', standards.head())
# TODO
# find potential matches (i.e. SUrname and some additionions)

# create new column to mess around with
occs['name_test'] = occs['recordedBy']
# not sure if this makes total sense, but cluster by surname
#occs[['test-surname', 'test-givens']] = occs['name_test'].astype(str).str.split(',', expand = True)
#occs = occs.groupby('test-surname')
print(occs.head())
# if surname not identical, then identical initials required



'''I would like to query the following out of my collector names:
- See if surname matches:
    if yes
    - see if the first letter matches,
        if yes
        - find the match with the most information, check for collection number similarity and collection period(?)
        if years within ~40(?)and collection numbers within 100? then take the biggest match and fill.

        if no
        ?- other letters matching?

    if no
    - ignore

'''

stand_names = pd.read_csv('./1_standard_data/mast_test.csv', sep = ';')

print(stand_names)
# something where we can say non-identity fails the match...
#stand_names = stand_names.dropna()

# always take longest version....




test_name = occs['name_test'].dropna()

def change(bad_name, name_list):

    names=[] #create an empty list where we strore the correct names based on the search
    proportion=[] #create an empty list where we strore the ratio values of the fuzzy search

    for i in bad_name:
        print(i)
        x=extractOne(i, name_list, scorer=normalized_similarity, processor=None)
        #print(x[0])
        print(x[1])
        names.append(x[0])
        proportion.append(x[1])
    return names, proportion

nms, prop = change(test_name, stand_names)
print(nms, prop)

#
