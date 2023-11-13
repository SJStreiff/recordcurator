#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Functions for expert det

2023-08-23 sjrs


- format input data

- check for missing data etc.

- integrate with master/find duplicated BC
"""

import codecs
import pandas as pd
import numpy as np
import logging
import pykew.ipni as ipni
import swifter              # not used at the moment


from pykew.ipni_terms import Filters as ipni_filter
from pykew.ipni_terms import Name as ipni_name              # for taxonomic additional info
from tqdm import tqdm                                       # progress bar for loops

def read_expert(importfile, verbose=True):
    """
    read file, check columns
    """
    print('EXPERT file integration. \n',
          'Please assure that your columns are the following:',
          'ddlat, ddlong, locality, country or ISO2, recorded_by, colnum_full, det_by, det_date, barcode')
    imp = codecs.open(importfile,'r','utf-8')
    exp_dat = pd.read_csv(imp, sep = ';',  dtype = str)
    exp_dat['source_id'] = 'specialist'

    # make prefix from colnum
    exp_dat['prefix'] = exp_dat.colnum_full.str.extract('^([a-zA-Z]*)')
    exp_dat['prefix'] = exp_dat['prefix'].str.strip()

    # make sufix from colnum
    # going from most specific to most general regex, this list takes all together in the end
    regex_list_sufix = [
        r'(?:[a-zA-Z ]*)$', ## any charcter at the end
        r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
        r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
    ]
    exp_dat['sufix'] = exp_dat['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
    exp_dat['sufix'] = exp_dat['sufix'].str.strip()

    # extract only digits without associated stuff, but including some characters (colNam)
    regex_list_digits = [
        r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
        r'(?:\d+\s\d+\s\d+)', # 00 00 00 or so
        r'(?:\d+\.\d+)', # 00.00
        r'(?:\d+)', # 00000
    ]
    exp_dat['colnum']  = exp_dat.colnum_full.str.extract('(' + '|'.join(regex_list_digits) + ')')
    exp_dat['colnum'] = exp_dat['colnum'].str.strip()

    logging.info('col_nums modified')

# det date
    exp_dat[['det_year', 'det_month', 'det_day']] = exp_dat['det_date'].str.split("/", expand=True)
    try:
        exp_dat[['col_year', 'col_month', 'col_day']] = exp_dat['col_date'].str.split("/", expand=True)
    except:
        print('no col_date found')

    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.replace('Collector(s):', '', regex=False)
    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.replace('Unknown', '', regex=False)
    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.replace('&', ';', regex=False)
    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.replace(' y ', ';', regex=False)
    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.replace(' and ', ';', regex=False)
    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.replace('Jr.', '', regex=False)
    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.replace('et al.', '', regex=False)
    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.replace('et al', '', regex=False)
    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.replace('etal', '', regex=False)
    #isolate just the first collector (before a semicolon)
    exp_dat['recorded_by'] = exp_dat['recorded_by'].astype(str).str.split(';').str[0]
    exp_dat['recorded_by'] = exp_dat['recorded_by'].str.strip() # remove whitespace

    print(exp_dat.recorded_by)


    exp_dat['orig_recby'] = exp_dat['recorded_by']
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


    names_WIP = exp_dat[['recorded_by']] #.astype(str)

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

    #####
    # Now i need to just merge the columns from the right and bobs-your-uncle we have beautiful collector names...
    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')

    #-------------------------------------------------------------------------------
    # For all names that didn't match anything:
    # extract and then check manually
    TC_exp_dat = exp_dat.copy()
    TC_exp_dat['to_check'] = names_WIP['recorded_by']


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
    exp_dat_newnames = exp_dat.assign(recorded_by = names_WIP['corrnames'])
    exp_dat_newnames['recorded_by'] = exp_dat_newnames['recorded_by'].replace('nan', 'ZZZ_THIS_NAME_FAILED')
    exp_dat_newnames['recorded_by'] = exp_dat_newnames['recorded_by'].replace('<NA>', 'ZZZ_THIS_NAME_FAILED')

    #print(exp_dat_newnames.recorded_by)
    # remove records I cannot work with...
    exp_dat_newnames = exp_dat_newnames[exp_dat_newnames['recorded_by'] != 'ZZZ_THIS_NAME_FAILED']


    logging.debug(f'The cleaned name format: {exp_dat_newnames.recorded_by}')
    # ------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------
    print(exp_dat_newnames.recorded_by)


    exp_dat_newnames[['huh_name', 'geo_col', 'wiki_url']] = '0'
   
    exp_dat_newnames['col_year'] = pd.NA

    return exp_dat_newnames


# do HUH
# get ipni numbers?




# deduplication....
# we will do this by barcodes and or recorded_by + colnum + ??

def deduplicate_small_experts(master_db, exp_dat, verbose=True):
    """
    Find duplicates based on barcode, and collector name,

    Any values in these records found are overwritten by 'expert' data. This is assuned to be of (much) better quality.
    """

    # first some housekeeping: remove duplicated barcodes in input i.e. [barcode1, barcode2, barcode1] becomes [barcode1, barcode2]
    print(sum(pd.isna(exp_dat.barcode)))
    exp_dat = exp_dat.dropna(subset = ['barcode'])
    exp_dat.barcode = exp_dat.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell
    master_db.barcode = master_db.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell

    # drop any determinations that are empty. This would make a mess.
    exp_dat = exp_dat[pd.notna(exp_dat.accepted_name)] 
    # add column for flag if data found or not
    exp_dat['matched'] = '0'

    #----- prep barcodes (i.e. split multiple barcodes into seperate values <BC001, BC002> --> <BC001>, <BC002>)
    # split new record barcode fields (just to check if there are multiple barcodes there)
    bc_dupli_split = exp_dat['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
    bc_dupli_split.columns = [f'bc_{i}' for i in range(bc_dupli_split.shape[1])] # give the columns names..
    bc_dupli_split = bc_dupli_split.apply(lambda x: x.str.strip())
    print(bc_dupli_split.shape)
    # some information if there are issues
    logging.debug(f'NEW OCCS:\n {bc_dupli_split}')
    logging.debug(f'NEW OCCS:\n {type(bc_dupli_split)}')
    # repeat for master data
    # split potential barcodes separated by ','
    master_bc_split = master_db['barcode'].str.split(',', expand = True) 
    master_bc_split.columns = [f'bc_{i}' for i in range(master_bc_split.shape[1])]
    master_bc_split = master_bc_split.apply(lambda x: x.str.strip())  # strip all leading/trailing white spaces!
    logging.debug(f'master OCCS:\n {master_bc_split}')

    # to make an exceptions dataframe get structure from master

    master_db.recorded_by = master_db.recorded_by.fillna('')
    exp_dat.recorded_by = exp_dat.recorded_by.fillna('')
    

    exp_dat_except = exp_dat[exp_dat.barcode == '']
    exp_dat = exp_dat[exp_dat.barcode != '']


    exceptions = master_db.head(1)
    exceptions = pd.concat([exceptions, exp_dat_except])

    # for progress bar in console output
    total_iterations = len(exp_dat)
    print('Crosschecking barcodes...\n', total_iterations, 'records in total.')
    for i in tqdm(range(total_iterations), desc = 'Processing', unit= 'iteration'):
        # the tqdm does the progress bar

        barcode = list(bc_dupli_split.loc[i].astype(str))
        print('INFO', barcode)
        print('MASTER:', master_db.shape)
            # logging.info(f'BARCODE1: {barcode}')
        # if multiple barcodes in the barcode field, iterate across them
        for x in  range(len(barcode)):
            bar = barcode[x]
            if bar == 'None':
                # this happens a lot. skip if this is the case.
                a = 'skip'
                #logging.info('Values <None> are skipped.')
            else:
                selection_frame = pd.DataFrame()  # df to hold resulting True/False mask  
                # now iterate over columns to find any matches
                for col in master_bc_split.columns:
                    # iterate through rows. the 'in' function doesn't work otherwise
                    #logging.info('checking master columns')
                    f1 = master_bc_split[col] == bar # get true/false column
                    selection_frame = pd.concat([selection_frame, f1], axis=1) # and merge with previos columns
                # end of loop over columns

                # when selection frame finished, get out the rows we need including master value
                sel_sum = selection_frame.sum(axis = 1)
                sel_sum = sel_sum >= 1 # any value >1 is a True == match of barcodes between dataframes 
            
                if sel_sum.sum() == 0:
                    # logging.info('NO MATCHES FOUND!')
                    logging.info('NO MATCH')
                    out_barcode = pd.DataFrame([bar])

                # in this case we do not modify anything!

                else:
                    logging.info(bar)
                    #print('SELSUM', sel_sum.shape)
                    logging.info(master_db.shape)
                    out_barcode = pd.Series(master_db.barcode[sel_sum]).astype(str)
                    out_barcode.reset_index(drop = True, inplace = True)


                    # replace i-th element of the new barcodes with the matched complete range of barcodes from master
                
                    input = str(exp_dat.at[i, 'barcode'])
                    master = str(out_barcode[0])
                    new = input + ', ' + master

                    # reduce duplicated values
                    new = ', '.join(set(new.split(', ')))
                    logging.info(f'Barcode matching:')
                    logging.info(input)
                    logging.info(master)
                    logging.info(new)

                    # QUICK CHECK if recorded by and colnum are identical. Flag and save to special file if not
                    #print('SUM', sum(sel_sum))

                    #print('1', master_db.loc[sel_sum, 'recorded_by'])

                    #print(master_db.loc[sel_sum, 'recorded_by'].shape)

                    if len(master_db.loc[sel_sum, 'recorded_by'])==1:
                        logging.info('only 1')
                        test_mastername = master_db.loc[sel_sum, 'recorded_by'].item()
                    else:
                        print('more than 2')
                        tmp1 = pd.DataFrame(master_db.loc[sel_sum, 'recorded_by'])
                        print(tmp1)
                        tmp1.reset_index(inplace=True, drop=True)
                        tmp2 = tmp1.loc[0,]
                        print(tmp2)
                        test_mastername = tmp2.item()


                    #print('2', exp_dat.loc[i, 'recorded_by'])
                    if test_mastername == (exp_dat.loc[i, 'recorded_by']):
                        logging.info('Names do not match...')
                        logging.info(f'{input}')

                    #----------------------- Imperative Values. Missing is not allowed ----------------------#
                    if exp_dat.loc[i,['accepted_name', 'det_by', 'det_year']].isna().any() == True:
                        # make a visible error message and raise exception (abort)
                        print('\n#--> Something is WRONG here:\n',
                            '\n#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#\n',
                            exp_dat.loc[i,['accepted_name', 'det_by', 'det_year']],
                            '\n#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#\n',
                            '\n I am aborting the program. Please carefully check your input data.\n',
                            '\n#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#\n',)
                        raise(Exception('One of \'accepted_name\', \'det_by\', or \'det_year\' is NA.\n',
                                        'I do not allow such records as expert data....'))
                    # otherwise the crucial data is here so we can proceed...
                    master_db.loc[sel_sum, 'barcode'] = new
                    master_db.loc[sel_sum, 'accepted_name'] = exp_dat.at[i, 'accepted_name'] 
                    master_db.loc[sel_sum, 'det_by'] = exp_dat.at[i, 'det_by'] 
                    master_db.loc[sel_sum, 'det_year'] = exp_dat.at[i, 'det_year'] 
                    
                    #----------------------- Facultative Values. Missing is allowed --------------------------#
                    if ~np.isnan(exp_dat.loc[i, 'ddlat']):
                        master_db.loc[sel_sum, 'ddlat'] = exp_dat.at[i, 'ddlat'] 
                    if ~np.isnan(exp_dat.loc[i, 'ddlong']):
                        master_db.loc[sel_sum, 'ddlong'] = exp_dat.at[i, 'ddlong'] 

                    #----------------------- Automatic Values. Filled anyway --------------------------#
                    master_db.loc[sel_sum, 'status'] = 'ACCEPTED'
                    master_db.loc[sel_sum, 'expert_det'] = 'A_expert_det_file'
                    master_db.loc[sel_sum, 'prefix'] = exp_dat.at[i, 'prefix'] 

                    #----------------------- Cosmetic Values. Missing is allowed --------------------------#
                    # print((pd.Series(exp_dat.loc[i, 'colnum_full']).isin(['-9999','NA','<NA>','NaN'])).any())
                    # print('CNF', exp_dat.loc[i, 'colnum_full'])
                    if pd.Series(exp_dat.loc[i, 'colnum_full']).isna().any():
                        logging.info('colnum full yes NA')
                        master_db.loc[sel_sum, 'colnum_full'] = exp_dat.at[i, 'colnum_full'] 
                    if pd.Series(exp_dat.loc[i, 'locality']).isna().any():
                        master_db.loc[sel_sum, 'locality'] = exp_dat.at[i, 'locality'] 

                    exp_dat.loc[i, 'matched'] = 'FILLED'


        # end of for loop barcodes
                    logging.info(f'done {barcode}')
    # same for exp dat
    exp_dat = exp_dat[exp_dat.matched != 'PROBLEM']
    # print(master_db.shape)
    # print('MATCHED?', exp_dat.matched)
    exp_dat_to_integrate = exp_dat[exp_dat.matched != 'FILLED']
    # print(exp_dat_to_integrate)

    # as we may have some data remaining, we just append it ot the master, as it's expert perfect ( in theory for our purposes)
    master_db =  pd.concat([master_db, exp_dat_to_integrate], axis=0)
    print('master goin out', master_db.shape)
    ## TODO## TODO## TODO## TODO## TODO## TODO## TODO## TODO## TODO## TODO

    return master_db#, #exceptions


def integrate_exp_exceptions(integration_file, exp_dat):
    """
    read and concatenate data manually edited, chekc data lengths 
    """
    imp = codecs.open(integration_file,'r','utf-8') #open for reading with "universal" type set
    re_exp = pd.read_csv(imp, sep = ';',  dtype = str, index_col=0) # read the data
    try:
        new_exp_dat = pd.concat([re_exp, exp_dat])
        if len(new_exp_dat) == (len(re_exp) + len(exp_dat)):
            print('Reintegration successful.')
            logging.info('reintroduction successful')
        else:
            print('reintegration not successful.')
            logging.info('reintegration unsuccessful')
    
    except:
        new_exp_dat = exp_dat
        print('reintegration not successful.')
        logging.info('reintegration unsuccessful')

    return new_exp_dat




def expert_ipni(species_name):
    ''' 
    Check species names against IPNI to get publication year, author name and IPNI link

    INPUT: 'genus'{string}, 'specific_epithet'{string} and 'distribution'{bool}
    OUTPUT: 'ipni_no' the IPNI number assigned to the input name
            'ipni_yr' the publication year of the species

    '''
    #print('Checking uptodate-ness of nomenclature in your dataset...')

    query = species_name.strip()
        #print(query)
    res = ipni.search(query, filters = ipni_filter.specific) # so we don't get a mess with infraspecific names
        #res = ipni.search(query, filters=Filters.species)  # , filters = [Filters.accepted])
    try:
        for r in res:
            #print(r)
            if 'name' in r:
                r['name']
        ipni_pubYr = r['publicationYear']
        ipni_no = 'https://ipni.org/n/' + r['url']
        ipni_author = r['authors']
        #print(ipni_pubYr)
        #logging.debug('IPNI publication year found.')
    except:
        ipni_pubYr = pd.NA
        ipni_no = pd.NA
        ipni_author = pd.NA

    return ipni_pubYr, ipni_no, ipni_author




def exp_run_ipni(exp_data):
        """
        wrapper for swifter apply of above function 'expert_ipni()'
        """
        print(exp_data.columns)
        exp_data[['ipni_species_author', 'ipni_no', 'ipni_pub']] = exp_data.swifter.apply(lambda row: expert_ipni(row['accepted_name']), axis = 1, result_type='expand')

        return exp_data



def deduplicate_small_experts_NOBARCODE(master_db, exp_dat, verbose=True):
    """
    integrate expert determinations into master database, using expert dets with only collector name and number.
    may include coordinate
    may include species name
         but needs at least one of these
    """

    #----------------------- Imperative Values. Missing is not allowed ----------------------#
    if exp_dat[['recorded_by', 'colnum', 'det_by', 'det_year']].isna().any().any() == True:
        # make a visible error message and raise exception (abort)
        print('\n#--> Something is WRONG here:\n',
                '\n#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#\n',
                exp_dat[['recorded_by', 'colnum', 'det_by', 'det_year']],
                '\n#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#\n',
                '\n I am aborting the program. Please carefully check your input data.\n',
                '\n#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#\n',)
        raise(Exception('One of \'recorded_by\', \'colnum\', \'det_by\', or \'det_year\' is NA.\n',
                        'I do not allow such records as expert data....'))
    

    # no exception, continue
    master_db['database_from'] = 'MASTER'
    exp_dat['database_from'] ='EXPERT' 
    # concatenate
    occs = pd.concat([master_db, exp_dat])
    # find duplicates
    occs_dup = occs[occs.duplicated(subset=['recorded_by', 'colnum','sufix', 'prefix' ], keep=False)]
    occs_nondup = occs.drop_duplicates(subset=['recorded_by', 'colnum', 'sufix', 'prefix'], keep=False)

    # deduplicate the matched duplicates
    occs_dup = occs_dup.sort_values(['recorded_by', 'colnum', 'database_from'], ascending = [True, True, True])

    # test if we have a determination, coordinates or both
    print(exp_dat.accepted_name)
    if exp_dat['accepted_name'].notna().all() == True: #   .isna().any() == True:
        print('EXPERT WITH ACCEPTED_NAME')
        if exp_dat['ddlat'].notna().any():
            print('ACCEPTED_NAME and COORDINATES')
            experts_merged = occs_dup.groupby(['recorded_by', 'colnum','sufix', 'prefix'], as_index = False).agg(
                            scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'last'),
                            genus = pd.NamedAgg(column = 'genus', aggfunc =  'last'),
                            specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'last' ),
                            species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'last' ),
                            collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'last' ),
                            recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'last' ),
                            colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                            prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                            colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                            sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                            col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'last' ),
                            col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'last' ),
                            col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'last' ),
                            col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'last' ),
                            det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                            det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                            det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                            det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                            det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                            country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'last' ),
                            country = pd.NamedAgg(column = 'country', aggfunc = 'last' ),
                            continent = pd.NamedAgg(column = 'continent', aggfunc = 'last' ),
                            locality = pd.NamedAgg(column = 'locality', aggfunc = 'last' ),
                            coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'last' ),
                            ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
                            ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
                            institute = pd.NamedAgg(column = 'institute',aggfunc = 'last'),
                            herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = 'last'),
                            barcode = pd.NamedAgg(column = 'barcode', aggfunc='last'), # as we have premerged all barcodes above, it doesn't matter which one we take
                            orig_bc = pd.NamedAgg(column = 'orig_bc',aggfunc = 'last'),
                            coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'last'),
                            huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'last'),
                            geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'last'),
                            source_id = pd.NamedAgg(column = 'source_id',  aggfunc = 'last'),
                            wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'last'),
                            expert_det = pd.NamedAgg(column = 'expert_det', aggfunc =lambda x: 'SMALLEXP'),
                            status = pd.NamedAgg(column = 'status',  aggfunc = 'last'),
                            accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                            ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'last'),
                            link =  pd.NamedAgg(column = 'link',  aggfunc = 'last'),
                            ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'last')
                            )
        else:
            print('ACCEPTED_NAME but no coordinates')
            experts_merged = occs_dup.groupby(['recorded_by', 'colnum','sufix', 'prefix'], as_index = False).agg(
                            scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'last'),
                            genus = pd.NamedAgg(column = 'genus', aggfunc =  'last'),
                            specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'last' ),
                            species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'last' ),
                            collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'last' ),
                            recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'last' ),
                            colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                            prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                            colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                            sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                            col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'last' ),
                            col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'last' ),
                            col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'last' ),
                            col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'last' ),
                            det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                            det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                            det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                            det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                            det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                            country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'last' ),
                            country = pd.NamedAgg(column = 'country', aggfunc = 'last' ),
                            continent = pd.NamedAgg(column = 'continent', aggfunc = 'last' ),
                            locality = pd.NamedAgg(column = 'locality', aggfunc = 'last' ),
                            coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'last' ),
                            ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'last' ),
                            ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'last' ),
                            institute = pd.NamedAgg(column = 'institute', aggfunc = 'last'),
                            herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = 'last'),
                            barcode = pd.NamedAgg(column = 'barcode', aggfunc='last'), # as we have premerged all barcodes above, it doesn't matter which one we take
                            orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc = 'last'),
                            coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'last'),
                            huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'last'),
                            geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'last'),
                            source_id = pd.NamedAgg(column = 'source_id',  aggfunc = 'last'),
                            wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'last'),
                            expert_det = pd.NamedAgg(column = 'expert_det', aggfunc =lambda x: 'SMALLEXP'),
                            status = pd.NamedAgg(column = 'status',  aggfunc = 'last'),
                            accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                            ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'last'),
                            link =  pd.NamedAgg(column = 'link',  aggfunc = 'last'),
                            ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'last')
                            )

    elif exp_dat['ddlat'].notna().any():
        print('EXPERT WITH COORDINATES but no accepted_name')

        experts_merged = occs_dup.groupby(['recorded_by', 'colnum','sufix', 'prefix'], as_index = False).agg(
                        scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'last'),
                        genus = pd.NamedAgg(column = 'genus', aggfunc =  'last'),
                        specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'last' ),
                        species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'last' ),
                        collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'last' ),
                        recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'last' ),
                        colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                        prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                        colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                        sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                        col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'last' ),
                        col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'last' ),
                        col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'last' ),
                        col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'last' ),
                        det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                        det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                        det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                        det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                        det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                        country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'last' ),
                        country = pd.NamedAgg(column = 'country', aggfunc = 'last' ),
                        continent = pd.NamedAgg(column = 'continent', aggfunc = 'last' ),
                        locality = pd.NamedAgg(column = 'locality', aggfunc = 'last' ),
                        coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'last' ),
                        ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
                        ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
                        institute = pd.NamedAgg(column = 'institute', aggfunc = 'last'),
                        herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc =  'last'),
                        barcode = pd.NamedAgg(column = 'barcode', aggfunc='last'), # as we have premerged all barcodes above, it doesn't matter which one we take
                        orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc= 'last'),
                        coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'last'),
                        huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'last'),
                        geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'last'),
                        source_id = pd.NamedAgg(column = 'source_id', aggfunc = 'last'),
                        wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'last'),
                        expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'last'),
                        status = pd.NamedAgg(column = 'status',  aggfunc = 'last'),
                        accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'last'),
                        ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'last'),
                        link =  pd.NamedAgg(column = 'link',  aggfunc= 'last'),
                        ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'last')
                        )
    else:
        print('noting to integrate as both accepted_name and coordinates are recognised as NA!!')

    #experts_merged # is the merged data!

    # retreive EXP data that did not match anything and return to user for 
    no_match = occs_nondup[occs_nondup.database_from == 'EXPERT']

    occs_nondup = occs_nondup[occs_nondup.database_from == 'MASTER']

    master_updated = pd.concat([occs_nondup, experts_merged, no_match])


    ### do some final integration stats
    print('# INTEGRATION OF EXPERT DATA FINISHED\n')
    print('Of the', len(exp_dat), 'expert records,', len(exp_dat) - len(no_match), 'could be integrated.' )
    print('####################################')
    print('Master is of size: ', len(master_updated))

    return master_updated



expert_types = {#'global_id': str,
    #'childData_id': str,
	#'accSpecies_id': str,
    'source_id': str,
	'genus': str,
	'specific_epithet': str,
    'recorded_by': str,
    'colnum_full': str,
	'prefix': str,
	'colnum': str,
	'sufix': str,
	'det_by': str,
	'det_date': str,
    'det_day': pd.Int64Dtype(),
    'det_month': pd.Int64Dtype(),
    'det_year': pd.Int64Dtype(),
	'country_iso3': str,
	'locality': str,
	'ddlong': float,
    'ddlat': float,
    'barcode': str,
    'accepted_name': str,
    'ipni_no': str,
    }





# # # # # # # # --- DEBUGGING LINES ----- # # # # # # # 

# # # debug_master = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/debug/smallexp_debug.csv', sep =';')
# # debug_exp_file = '/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/debug/exp.csv'

# # # print(debug_exp_file)
# # # print(debug_master)
# # # print('---WORKING---')
# # debug_exp = read_expert(debug_exp_file)
# # print(debug_exp)

# # # print(debug_exp)
# # debug_exp = exp_run_ipni(debug_exp)

# # print(debug_exp)
# # # final, exception = deduplicate_experts_minimal(debug_master, debug_exp)

# # # print(final.accepted_name)
# # # print('EXCEPTIONS', exception)
# # test_Y, test_N, testA = expert_ipni(spec)
# # spec = 'Cananga odorata'
#     # print(test_Y, test_N, testA)