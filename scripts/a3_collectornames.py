#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
2022-12-13 sjs

CHANGELOG:
    2022-12-13: created
    2024-01-29: cleaned & curated

    collector_names():
        takes collector names column ("recorded_by") and rearranges it into the format
        Surname, F (firstname just as initials)
    reinsertion():
        read manually checked data that didn't match in collector_names() and reintegrate.
        
'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import logging

#custom dependencies
import z_dependencies # can be replaced at some point, but later...




def collector_names(occs, working_directory, prefix, verbose=True, debugging=False):
    """
    With an elaborate regex query, match all name formats I have been able to come up with.
    This is applied to both recorded_by and det_by columns. All those records that failed are
    written into a debugging file, to be manually cleaned.
    """
    print('NAMES FORMAT:', occs.columns)
    pd.options.mode.chained_assignment = None  # default='warn'
    # this removes this warning. I am aware that we are overwriting stuff with this function.
    # the column in question is backed up


    #print(occs.dtypes) # if you want to double check types again
    occs = occs.astype(dtype = z_dependencies.final_col_type)
    occs['recorded_by'] = occs['recorded_by'].replace('nan', pd.NA)
    occs['det_by'] = occs['det_by'].replace('nan', pd.NA)
    #print(occs.head)
    # -------------------------------------------------------------------------------
    # Clean some obvious error sources, back up original column
    occs['orig_recby'] = occs['recorded_by'] # keep original column...
    occs['orig_detby'] = occs['det_by']
    # remove the introductory string before double point :
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Collector(s):', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Unknown', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('&', ';', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace(' y ', ';', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace(' and ', ';', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Jr.', 'JUNIOR', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('et al.', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('et al', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('etal', '', regex=False)
    #occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Philippine Plant Inventory (PPI)', 'Philippines, Philippines Plant Inventory', regex=False)
    #we will need to find a way of taking out all the recorded_by with (Dr) Someone's Collector

    #isolate just the first collector (before a semicolon)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.split(';').str[0]

    occs['recorded_by'] = occs['recorded_by'].str.strip()

    print('---------------- \n There are still weird exceptions which I do not catch. These have to be handled manually and reintegrated.',
        '\n -------------------')

    # sep collectors by ';', multiple collectors separated like this
    # 'Collector(s):' is also something that pops up in GBIF....

    # now we need to account for many different options
    # 1 Meade, C.     --> ^[A-Z][a-z]*\s\[A-Z]   ^([A-Z][a-z]*)\,
    # 2 Meade, Chris  -->
    # 3 Meade C       -->
    # 4 C. Meade      -->
    # 5 Chris Meade   -->


    #-------------------------------------------------------------------------------
    # regex queries: going from most specific to least specific.

   
    extr_list = {
            #r'^([A-ZÀ-Ÿ][a-zà-ÿ]\-[A-ZÀ-Ÿ][a-zà-ÿ]\W+[A-ZÀ-Ÿ][a-zà-ÿ])' : r'\1', # a name with Name-Name Name
            #r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ]{2,5})' : r'\1, \2', #Surname FMN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([a-zà-ÿ]{0,3})' : r'\1, \2\3\4\5 \6',  # all full full names with sep = ' ' plus Surname F van
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+' : r'\1, \2\3\4\5',  # all full full names with sep = ' '

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([a-zà-ÿ]{0,3})' : r'\1, \2\3\4 \5',  # all full full names with sep = ' ' plus Surname F van
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+' : r'\1, \2\3\4',  # all full full names with sep = ' '

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([a-zà-ÿ]{0,3})': r'\1, \2\3 \4',  # all full names: 2 given names # + VAN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+': r'\1, \2\3',  # all full names: 2 given names

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]{2,20}\s+([a-zà-ÿ]{0,3})': r'\1, \2 \3',  # just SURNAME, Firstname  # + VAN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]{2,20}': r'\1, \2',  # just SURNAME, Firstname

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W*\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2\3\4\5 \6',  # Surname, F(.) M(.) M(.)M(.) # VAN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W*': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.)M(.)


            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W*\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2\3\4 \5',  # Surname, F(.) M(.) M(.) # VAN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W*': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.)

            r'(^[A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W\s+([a-zà-ÿ]{0,3})\,.+': r'\1, \2\3\4 \5',  # Surname, F(.) M(.) M(.), other collectors
            r'(^[A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W\,.+': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.), other collectors

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2\3 \4',  # Surname, F(.) M(.)
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+\Z': r'\1, \2\3',  # Surname, F(.) M(.)

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\s+([a-zà-ÿ]{0,3})': r'\1, \2\3\4\5 \6',  # Surname FMMM
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])': r'\1, \2\3\4\5',  # Surname FMMM

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\s+([a-zà-ÿ]{0,3})': r'\1, \2\3\4 \5',  # Surname FMM
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])': r'\1, \2\3\4',  # Surname FMM

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2\3 \4',  # Surname FM
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\Z': r'\1, \2\3',  # Surname FM

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2 \3',  # Surname F
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])\Z': r'\1, \2',  # Surname F

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W*\Z': r'\1',  # Surname without anything
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+\s+([a-zà-ÿ]{0,3})': r'\1, \2 \3',  # Surname, F(.)
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+': r'\1, \2',  # Surname, F(.)

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\6, \1\2\3\4 \5', # Firstname Mid Nid Surname ...
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3\4', # Firstname Mid Nid Surname ...

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3 \4', # Firstname Mid Nid Surname ...
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2\3', # Firstname Mid Nid Surname ...

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2 \3', # Firstname Mid Surname ...
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1\2', # Firstname Mid Surname ...

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2 \3', # Firstname M. Surname ...
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1\2', # Firstname M. Surname ...

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1 \2', # Firstname Surname
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\2, \1', # Firstname Surname

            r'^([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\2, \1', # F. Surname ...
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\2, \1', # F. Surname ...

            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2 \3', #F. M. van  Surname
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1\2', #F. M. Surname
            
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3 \4', #F. M. M. van Surname
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2\3', #F. M. M. van Surname

            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\6, \1\2\3\4 \5', #F. M. M. M. van Surname
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3\4', #F. M. M. M. van Surname

            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\6, \1\2\3\4 \5', #FMMM Surname
            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3\4', #FMM Surname


            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3 \4', #FMM Surname
            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2\3', #FMM Surname

            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2 \3', #FM Surname
            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1\2', #FM Surname

            r'^([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1 \2', #F Surname
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\2, \1', #F Surname
            #r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ]{2,5})' : r'\1, \2', #Surname FMN

        }


    # The row within the extr_list corresponds to the column in the debugging dataframe printed below


    names_WIP = occs[['recorded_by']] #.astype(str)

    #print(names_WIP)
    i = 0
    for key, value in extr_list.items():
        i = i+1
        # make a new panda series with the regex matches/replacements
        X1 = names_WIP.recorded_by.str.replace(key, value, regex = True)
        # replace any fields that matched with empty string, so following regex cannot match.
        names_WIP.loc[:,'recorded_by'] = names_WIP.loc[:,'recorded_by'].str.replace(key, '', regex = True)
        # make new columns for every iteration
        names_WIP.loc[:,i] = X1 #.copy()

    # debugging dataframe: every column corresponds to a regex query
    if debugging:
        names_WIP.to_csv(working_directory + prefix + 'DEBUG_regex.csv', index = False, sep =';', )
        logging.debug(f'debugging dataframe printed to {working_directory + prefix}DEBUG_regex.csv')

    #####
    # Now i need to just merge the columns from the right and bobs-your-uncle we have beautiful collector names...
    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')

    #-------------------------------------------------------------------------------
    # For all names that didn't match anything:
    # extract and then check manually
    TC_occs = occs.copy()
    TC_occs['to_check'] = names_WIP['recorded_by']

    # #-------------------------------------------------------------------------------


    # mask all values that didn't match for whatever reason in the dataframe (results in NaN)
    names_WIP = names_WIP.mask(names_WIP.recorded_by.notna())

    # now merge all columns into one
    while(len(names_WIP.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
        i = i+1
        names_WIP.iloc[:,-1] = names_WIP.iloc[:,-1].fillna(names_WIP.iloc[:,-2])
        names_WIP = names_WIP.drop(names_WIP.columns[-2], axis = 1)
        #print(names_WIP) # for debugging, makes a lot of output
        #print('So many columns:', len(names_WIP.columns), '\n')
    #print(type(names_WIP))


    #print('----------------------\n', names_WIP, '----------------------\n')
    # just to be sure to know where it didn't match
    names_WIP.columns = ['corrnames']
    names_WIP = names_WIP.astype(str)

    # now merge these cleaned names into the output dataframe
    occs_newnames = occs.assign(recorded_by = names_WIP['corrnames'])



    occs_newnames['recorded_by'] = occs_newnames['recorded_by'].replace('nan', 'ZZZ_THIS_NAME_FAILED')
    occs_newnames['recorded_by'] = occs_newnames['recorded_by'].replace('<NA>', 'ZZZ_THIS_NAME_FAILED')

    #print(occs_newnames.recorded_by)
    # remove records I cannot work with...
    occs_newnames = occs_newnames[occs_newnames['recorded_by'] != 'ZZZ_THIS_NAME_FAILED']


    # ------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------
    # repeat the story with the det names
    logging.debug(f'The cleaned name format: {occs_newnames.recorded_by}')
    names_WIP = occs_newnames[['det_by']] #.astype(str)

    #print(names_WIP)
    i = 0
    for key, value in extr_list.items():
        i = i+1
        # make a new panda series with the regex matches/replacements
        X1 = names_WIP.det_by.str.replace(key, value, regex = True)
        # replace any fields that matched with empty string, so following regex cannot match.
        names_WIP.loc[:,'det_by'] = names_WIP.loc[:,'det_by'].str.replace(key, '', regex = True)
        # make new columns for every iteration
        names_WIP.loc[:,i] = X1 #.copy()

    # debugging dataframe: every column corresponds to a regex query
    if debugging:
        names_WIP.to_csv(working_directory + prefix + 'DEBUG_detby_regex.csv', index = False, sep =';', )
        logging.debug(f'debugging dataframe for det bys printed to {working_directory + prefix}DEBUG_detby_regex.csv')

    #####
    # Now i need to just merge the columns from the right and bobs-your-uncle we have beautiful collector names...
    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')

    #-------------------------------------------------------------------------------
    # For all names that didn't match anything:
    # extract and then check manually

    TC_occs1 = occs_newnames.copy()
    TC_occs1['to_check_det'] = names_WIP['det_by']
    # print('CHECK', TC_occs1.to_check_det)

    # print('TEST', TC_occs.colnum_full)
    # print(TC_occs.to_check)
    # print('NAs:',sum(pd.isna(TC_occs.to_check)))
    try:
        TC_occs1['to_check_det'] = TC_occs1['to_check_det'].str.replace('NaN', pd.NA)
    #     print('THIS TIME IT WORKED')
    except:
        logging.debug(f'TC_occs no NA transformation possible')

    # print('det NAs:',sum(pd.isna(TC_occs1.to_check_det)))
    # ###




    #-------------------------------------------------------------------------------


    # mask all values that didn't match for whatever reason in the dataframe (results in NaN)
    names_WIP = names_WIP.mask(names_WIP.det_by.notna())

    # now merge all columns into one
    while(len(names_WIP.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
        i = i+1
        names_WIP.iloc[:,-1] = names_WIP.iloc[:,-1].fillna(names_WIP.iloc[:,-2])
        names_WIP = names_WIP.drop(names_WIP.columns[-2], axis = 1)
        #print(names_WIP) # for debugging, makes a lot of output
        #print('So many columns:', len(names_WIP.columns), '\n')
    #print(type(names_WIP))

    # just to be sure to know where it didn't match
    names_WIP.columns = ['corrnames']
    names_WIP = names_WIP.astype(str)

    # now merge these cleaned names into the output dataframe
    occs_newnames = occs_newnames.assign(det_by = names_WIP['corrnames'])

    #leave just problematic names (drop NA)
    TC_occs.dropna(subset= ['to_check'], inplace = True)
    # print('HERE 1', TC_occs.to_check)
    TC_occs.det_by = occs_newnames.det_by

    # print(TC_occs1.to_check_det)
    # some datasets have problem with NA strings (vs NA type)
    try:
        TC_occs1['to_check_det'] = TC_occs1['to_check_det'].replace('<NA>', pd.NA)
        # print('<NA> handled')
    except:
        print('<NA> issues')
    TC_occs1.dropna(subset= ['to_check_det'], inplace = True)
    # print('HERE 2', TC_occs1.to_check_det)
    # print('NAs:',sum(pd.isna(TC_occs1.to_check_det)))

    #print(TC_occs.to_check)
    TC_occs['to_check_det'] = 'recby_problem'
    TC_occs1['to_check'] = 'detby_problem'

    TC_occs = pd.concat([TC_occs, TC_occs1])#, on = 'orig_recby', how='inner')
    logging.debug(f'To check: {TC_occs.shape}')
    TC_occs = TC_occs.drop_duplicates(subset = ['barcode'], keep = 'first')
    
    logging.debug(f'To check (deduplicated by barcode) {TC_occs.shape}')
    # print(TC_occs.columns)
    TC_occs_write = TC_occs[['recorded_by', 'orig_recby', 'colnum_full', 'det_by', 'orig_detby', 'to_check', 'to_check_det']]

    # output so I can go through and check manually
    if len(TC_occs)!= 0:
        TC_occs_write.to_csv(working_directory +'TO_CHECK_' + prefix + 'probl_names.csv', index = True, sep = ';', )
        print(len(TC_occs), ' records couldn\'t be matched to the known formats.',
        '\n Please double check these in the separate file saved at: \n', working_directory+'TO_CHECK_'+prefix+'probl_names.csv')



    # ------------------------------------------------------------------------------#
    # ------------------------------------------------------------------------------

    logging.info(f'It used to look like this: {occs.recorded_by}')
    logging.info('---------------------------------------------------')
    logging.info(f'Now it looks like this: {occs_newnames.recorded_by}')

    logging.info(f'DETS: It used to look like this: {occs.det_by}')
    logging.info(f'---------------------------------------------------')
    logging.info(f'DETS: Now it looks like this: {occs_newnames.det_by}')

    logging.info(f'I removed {len(occs) - len(occs_newnames)} records because I could not handle the name.')
   
    occs_newnames = occs_newnames.astype(z_dependencies.final_col_type)
    # save to file
    #occs_newnames.to_csv(working_directory+prefix+'names_standardised.csv', index = False, sep = ';', )
    occs_newnames['coll_surname'] = occs_newnames['recorded_by'].str.split(',', expand=True)[0]

    if debugging:
        unique_names = occs_newnames['recorded_by'].unique()
        unique_names = pd.DataFrame(unique_names)
        unique_names.to_csv(working_directory + prefix+'collectors_unique.csv', index = False, sep =';', )
        logging.debug(f'I have saved {len(unique_names)} Collector names to the file {working_directory + prefix}collectors_unique.csv.')

    # done
    return occs_newnames, TC_occs



def reinsertion(occs_already_in_program, frame_to_be_better, names_to_reinsert, verbose=True, debugging=False):
    '''
    Quickly read in data for reinsertion, test that nothing went too wrong, and append to the data already in the system.
    occs_already_in_program: Data not flagged in previous steps
      frame_to_be_better: Data flagged in previous step
      names_to_reinsert: subset of frame the above, but corrected by user (hopefully!)
    '''
    logging.info('REINSERTING...')
    logging.info(f'To reinsert: {names_to_reinsert}')
    imp = codecs.open(names_to_reinsert,'r','utf-8') #open for reading with "universal" type set
    re_occs = pd.read_csv(imp, sep = ';',  dtype = str, index_col=0) # read the data
    logging.debug(f'The read dataframe: {re_occs}')
    logging.debug(f'The read dataframe columns: {re_occs.columns}')
    
    re_occs = re_occs.drop(['to_check', 'to_check_det'], axis = 1)
    re_occs.sort_index(inplace=True)
    re_occs = re_occs.replace({'NaN': pd.NA}, regex=False)
    
    
    logging.debug(f'Reordered read data: {re_occs.recorded_by}')
    frame_to_be_better.sort_index(inplace=True)
    logging.debug(f'The problem data before user correction: {frame_to_be_better.orig_recby}')
    frame_to_be_better['recorded_by'] = re_occs['recorded_by']
    frame_to_be_better['det_by'] = re_occs['det_by']
    logging.debug(f'The problem data after user correction: {frame_to_be_better.recorded_by}') #[['recordedBy', 'orig_recby']])
    logging.debug(f'Original problematic values{frame_to_be_better.orig_recby}')
    
    frame_to_be_better = frame_to_be_better.astype(z_dependencies.final_col_type)

    
    logging.info('Reinsertion data read successfully')

    occs_merged = pd.concat([occs_already_in_program, frame_to_be_better])
    logging.info(f'TOTAL {len(occs_merged)} in Prog {len(occs_already_in_program)} reintegrated {len(re_occs)}')
    if len(occs_merged) == len(occs_already_in_program) + len(re_occs):
        logging.info('Data reinserted successfully.')
        print('Data integrated successfully!')
    else:
        #raise Exception("Something weird happened, please check input and code.")
        print('Data integration anomalous! Maybe the input and original output aren\'t matched?')
        logging.info('data integration anomalous, either error or discrepancy')
        logging.info('This might for example be if the integrated data is not exactly the same size of the data i wrote in previous steps')
    return occs_merged







#
