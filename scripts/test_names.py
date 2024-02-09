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


import rapidfuzz
from rapidfuzz import process, fuzz
#from rapidfuzz.string_metric import levenshtein, normalized_levenshtein
from rapidfuzz.distance.Levenshtein import normalized_similarity
#from rapidfuzz.distance.Levenshtein import normalized_similarity
from rapidfuzz.fuzz import ratio
from rapidfuzz.process import extract





def change(bad_name, name_list):
    """ Function written by TLPC
    """
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

# nms, prop = change(test_name, stand_names)
# print(nms, prop)





# mdb = pd.read_csv('~/Sync/1_Annonaceae/GLOBAL_final/GDB/master_db.csv', sep = ';', na_values = '-9999')


# names = mdb[['recorded_by', 'huh_name', 'orig_recby', 'geo_col']]
# names = names.drop_duplicates(keep='first')

# names['orig_recby'] = names['orig_recby'].apply(lambda x: ', '.join(set(filter(lambda s: s.lower() != '<na>', str(x).split(', ')))) if pd.notna(x) else pd.NA)    # this combines all duplicated values within a cell
# names = names[names.recorded_by.notna()]
# newnames = names[['recorded_by', 'orig_recby']]
# names = names[names.geo_col.notna()]



# names['orig_recby'] = names['orig_recby'].astype(str).str.replace('Collector(s):', '', regex=False)
# names['orig_recby'] = names['orig_recby'].astype(str).str.replace('Unknown', '', regex=False)
# names['orig_recby'] = names['orig_recby'].astype(str).str.replace('&', ';', regex=False)
# names['orig_recby'] = names['orig_recby'].astype(str).str.replace(' y ', ';', regex=False)
# names['orig_recby'] = names['orig_recby'].astype(str).str.replace(' and ', ';', regex=False)
# names['orig_recby'] = names['orig_recby'].astype(str).str.replace('Jr.', 'JUNIOR', regex=False)
# names['orig_recby'] = names['orig_recby'].astype(str).str.replace('et al.', '', regex=False)
# names['orig_recby'] = names['orig_recby'].astype(str).str.replace('et al', '', regex=False)
# names['orig_recby'] = names['orig_recby'].astype(str).str.replace('etal', '', regex=False)
# #names['orig_recby'] = names['orig_recby'].astype(str).str.replace('Philippine Plant Inventory (PPI)', 'Philippines, Philippines Plant Inventory', regex=False)
# #we will need to find a way of taking out all the orig_recby with (Dr) Someone's Collector

# #isolate just the first collector (before a semicolon)
# names['orig_recby'] = names['orig_recby'].astype(str).str.split(';').str[0]

# names['orig_recby'] = names['orig_recby'].str.strip()

# newnames.loc[newnames['orig_recby'] == '', 'orig_recby'] = pd.NA
# newnames.loc[newnames['orig_recby'] == ' ', 'orig_recby'] = pd.NA
# newnames.loc[newnames['orig_recby'] == '-9999', 'orig_recby'] = pd.NA
# newnames.loc[newnames['orig_recby'] == 'nan', 'orig_recby'] = pd.NA
# newnames.loc[newnames['orig_recby'] == 'None', 'orig_recby'] = pd.NA
# newnames.loc[newnames['orig_recby'] == 'NaN', 'orig_recby'] = pd.NA
# newnames.loc[newnames['orig_recby'] == '<NA>', 'orig_recby'] = pd.NA
# #newnames.orig_recby = newnames.orig_recby.replace(' ', pd.NA)
# newnames = newnames[newnames.orig_recby.notna()]
# newnames = newnames.head(10)


names = pd.read_csv('~/Desktop/names_test.csv', sep =';')
names = names.drop_duplicates(keep='first')
names = names[names.geo_col.notna()]
#names['surname'] = names['recby_regex'].str.split(', ', expand=True)[0]
#names['initials'] = names['recby_regex'].str.split(', ', expand=True)[1]

print(names)
#print(newnames)


# names = names.head(2)



def initials_to_full_name(initials, reference):
    best_match = None
    highest_similarity = 0

    return best_match, similarity




def compare_names(list1, list2):

# Initialize empty lists for surnames and first names
    surnames2 = []
    first_names2 = []

    # Iterate through the names and split them
    for name in list2:
        parts = name.split(', ')

        # Check if there's a first name, and handle accordingly
        if len(parts) == 2:
            surname, first_name = parts
        else:
            surname, first_name = parts[0], 'NA'

        # Append to the respective lists
        surnames2.append(surname)
        first_names2.append(first_name)



    reference = pd.DataFrame({'Full':list2, 'surname': surnames2, 'firstnames': first_names2})


    #print(reference)



    for name1 in list1:
        # Extract surname from the first lis
        #print(name1)
        surname1 = name1.split(', ')[0]
        initials1 = name1.split(', ')[1]
        # print('Surname query:\n', surname1)
        # Perform a fuzzy match based on the surname
        matches = pd.DataFrame(process.extract(surname1, reference['surname'], scorer=ratio, score_cutoff=50)) # 50 is very low. but who knows
        # print(matches)
        matched_sn = matches.iloc[:,0]
        # print('HERE matching surnames\n',matched_sn)


        # print('New reference\n',reference[reference['surname'].isin(matched_sn)])

        reference_subset = reference[reference['surname'].isin(matched_sn)]




        #print(matches[0])
        # best_surname_match = matches[0][0]
        # surname_sim = matches[0][1]
        

        # for name in reference_subset['firstnames']:
            # Extract initials from the full name
            #name_initials = ''.join(part[0] for part in name.split())
            
            # Calculate similarity between the provided initials and name initials
        test = process.extract(initials1, reference_subset['firstnames'], scorer=ratio, limit=1)
        # print('test\n', test)
        bestname = test[0][0]
        bestratio = test[0][1]

        # print('NEW:\n', bestname, bestratio)
        newname = reference_subset.loc[reference_subset.firstnames == bestname, 'Full']
        # print('THISISIT:\n', newname)
            # similarity = ratio(initials1, name_initials)

            # highest_similarity = similarity
            # print('BEST:\n',highest_similarity)
            # best_match = reference[reference.]



        # # Check if the best match contains initials
        # if any(initial in best_surname_match for initial in initials1.split()):
        #     best_match = initials_to_full_name(initials1, reference_subset)
        # else:
        #     best_match = best_surname_match
        


        print(f"Original Name: {name1}, Best Match: {newname}")


# Example usage
compare_names(names['recby_regex'], names['recorded_by'])


# nms, prop = change(newnames['recorded_by'], names['huh_name'])
# print(nms)
