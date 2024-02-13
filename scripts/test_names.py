#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for namechecking the Harvard University Herbarium collectors database

2023-01-10 sjs

CHANGELOG:
    2023-01-10: created
    2023-01-12: it works. BUT it doesn't give what i want. I do not get access to the full name of the collector or ID I need to access more deta

    2024-01-29: cleaned and curated
CONTAINS:

    country_crossfill():
      crossfils between the two country identifier columns (i.e. country_id (ISO 2 letter abbreviation), and country (full name))

'''


import pandas as pd
import numpy as np
import regex as re

import rapidfuzz
from rapidfuzz import process, fuzz
#from rapidfuzz.string_metric import levenshtein, normalized_levenshtein
from rapidfuzz.distance.Levenshtein import normalized_similarity
#from rapidfuzz.distance.Levenshtein import normalized_similarity
from rapidfuzz.fuzz import ratio
from rapidfuzz.process import extract




def prepare_reference_frame(reference_names):
    """
    Takes a series/list of names (e.g. from HUH) and formats into Full, Surname and Firstnames for 
    easy crossreferencing with Regex names.
    """

# Initialize empty lists for surnames and first names
    surnames2 = []
    first_names2 = []
    # Iterate through the names and split them
    for name in reference_names:
        parts = name.split(', ')

        # Check if there's a first name, and handle accordingly
        if len(parts) == 2:
            surname, first_name = parts
        else:
            surname, first_name = parts[0], '0'

        # Append to the respective lists
        surnames2.append(surname)
        first_names2.append(first_name)


    reference = pd.DataFrame({'Full':reference_names, 'surname': surnames2, 'firstnames': first_names2})

    return reference



def extract_best_name(name_to_check, reference_df):
        """
        takes a name and checks for best match within reference dataframe.
        Reference dataframe is formatted as in function above. (i.e. columns Full, surname and firstnames)
        """

        # Extract surname from the first lis
       # print(name_to_check)

        if len(name_to_check.split(',')) == 2:
            #print(name_to_check.split(','))
            surname_to_check = name_to_check.split(', ')[0]
            initials1 = name_to_check.split(', ')[1]
        else:
            surname_to_check = name_to_check.split(', ')[0]
            initials1 = ' '

        # Perform a fuzzy match based on the surname
        # matches = pd.DataFrame(process.extract(surname_to_check, reference_df['surname'], scorer=ratio, score_cutoff=80)) # 50 is very low. but who know
        matches = pd.DataFrame(process.extractOne(surname_to_check, reference_df['surname'], scorer=ratio)) # 50 is very low. but who know
        matched_sn = matches.iloc[:,0]
        # print('match1:', matched_sn)
        # then subset just the matching surnames
        reference_subset = reference_df[reference_df['surname'].isin(matched_sn)]
        # print('REFERENCE', reference_subset)
        # Calculate similarity between the provided initials and reference initials
        test = process.extract(initials1, reference_subset['firstnames'], scorer=normalized_similarity)
        # print('test\n', test)
        bestname = test[0][0]
        #print('RES:', bestname, '\nINPUT:', initials1)

        #Check if the first initial mathces!
        # try:
        init_input = initials1[0]
        init_result = bestname[0]
        if init_input == init_result:
             print('SUCCES?')
        else:
            print('FAIL?')
            print('I:',init_input)
            print('R:', init_result)
        # except:
        #      init_input = pd.NA

        bestratio = test[0][1]

        # print('NEW:\n', bestname, bestratio)
        newname = reference_subset.loc[reference_subset.firstnames == bestname, 'Full']



        print(f"Original Name: {name_to_check}, Best Match: {newname}")
    
    #return

# Example usage



names = pd.read_csv('~/Desktop/names_test.csv', sep =';')
names = names.drop_duplicates(keep='first')
#names = names[names.geo_col.notna()]
#names = names.head(15)
ref_basic = names[names.geo_col.notna()]
print(names)
ref = prepare_reference_frame(ref_basic['recorded_by'])
print(ref)

names = names.apply(lambda row: extract_best_name(row['recby_regex'], ref), axis = 1)
#compare_names(names['recby_regex'], names['recorded_by'])


#TODO
"""
check for inverted names in reference:
     input reference script into HUH query
test with LAtin-American names!

stuff like this:

Original Name: Darbyshire, I, Best Match: 700    Iain Darbyshire
FAIL?
I: C
R: 0
Original Name: Doumenge, C, Best Match: 711    Charles Doumenge

"""