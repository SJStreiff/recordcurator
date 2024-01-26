#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Functions for expert det

VERSION 2

2024-01-25 sjrs

- format input data

- check for missing data etc.

- integrate to master DB by different ways, depending on if barcode present or not!

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



import z_dependencies





def read_expert(importfile, verbose=True):
    """
    read file, check columns,
    standardise to same format as rest of data
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
    if {'col_year'}.issubset(exp_dat.columns):   
        print('col_year already present')
        exp_dat['col_date'] = exp_dat.col_year + '/' + exp_dat.col_month + '/' + exp_dat.col_day
    else:
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

    if {'col_year'}.issubset(exp_dat_newnames.columns):   
        print('col_year already present')
        print(exp_dat_newnames.col_year)
    else:
        exp_dat_newnames['col_year'] = pd.NA
        print(exp_dat_newnames.col_year)

    # check barcodes if present, and if NA values are present replace with barcode-placeholder
    if {'barcode'}.issubset(exp_dat_newnames.columns):
        
        if exp_dat_newnames.barcode.isna().all():
            print('barcodes are present but all NA')
            exp_dat_newnames.reset_index(inplace=True)
            # create unique id based on detby and detyear and index. Then make alphanumeric only
            exp_dat_newnames['barcode'] = exp_dat_newnames['det_by'] + exp_dat_newnames['det_year'] + (exp_dat_newnames.index+1)
            exp_dat_newnames['barcode'] = exp_dat_newnames['barcode'].apply(lambda x: ''.join(e for e in x if e.isalnum()))

        elif exp_dat_newnames.barcode.isna().any():
            print('barcodes are present but some NA')
            exp_dat_newnames.reset_index(inplace=True)
            exp_dat_newnames['barcode'] = exp_dat_newnames['barcode'].fillna((exp_dat_newnames['det_by'] + exp_dat_newnames['det_year'] + (exp_dat_newnames.index+1)).apply(lambda x: ''.join(e for e in x if e.isalnum())))
           
        else:
            print('barcodes present')
            # no need to add anything
    else:
        print('No barcodes whatsoever')
        # if no barcodes, add det_by-det_year-unique-id as barcode-ersatz
        exp_dat_newnames.reset_index(inplace=True)
        exp_dat_newnames['barcode'] = exp_dat_newnames['det_by'] + exp_dat_newnames['det_year'] + (exp_dat_newnames.index+1)
        exp_dat_newnames['barcode'] = exp_dat_newnames['barcode'].apply(lambda x: ''.join(e for e in x if e.isalnum()))


    exp_dat_newnames[['huh_name', 'geo_col', 'wiki_url']] = '0'

    return exp_dat_newnames




def deduplicate_small_experts(master, exp_dat):
    """
    Find and update duplicates between master and expert dataset 
    Any values in these records found are overwritten by 'expert' data. This is assuned to be of (much) better quality.
    """
    #> Prepare data 

    # infos
    # print('master:', master.shape)
    print('exp:', exp_dat.det_by.unique())

    #flag for database origin
    master['origin'] = 'MASTER'
    exp_dat['origin'] = 'EXP'

    # clean prefix so no surnames turn up there anymore.
    master['surnames'] = master.recorded_by.astype(str).str.split(',').str[0]
    #print(master.surnames)
    mask = master['prefix'] == master['surnames']
    master.loc[mask, 'prefix'] = pd.NA

    # clean NA strings  
    for col in ['accepted_name', 'recorded_by', 'det_by', 
                'col_day', 'col_month', 'col_year', 'det_date', 'expert_det',
                 'prefix', 'colnum', 'sufix', 'col_date', 'det_day', 'det_month', 'det_year',
                'huh_name', 'geo_col', 'wiki_url','status', 'ipni_no', 'ipni_species_author']:
        
        try: # find all possible delinquent values and set to NA!
            master.loc[master[col] == '', col] = pd.NA
            master.loc[master[col] == ' ', col] = pd.NA
            master.loc[master[col] == '-9999', col] = pd.NA
            master.loc[master[col] == 'nan', col] = pd.NA
            master.loc[master[col] == 'NaN', col] = pd.NA
            master.loc[master[col] == '<NA>', col] = pd.NA
            master[col] = master[col].str.strip(' ')
            master[col] = master[col].str.strip('')

        except:
            master.loc[master[col] == 0, col] = pd.NA
            master.loc[master[col] == -9999, col] = pd.NA


    #> Data ready
# ASK USER IF EXPERT DATA CONTAINS USEABLE BARCODES OR NOT
    
    print('\n ................................\n',
        'Please state whether the expert data being integrated contains fully valid barcodes [YES] or not [NO].')
    barcode_status=input() #'n' # make back to input()
    logging.info(f'-> Your input for barcode status: {barcode_status}')

    if barcode_status == 'YES':
        #deduplicate by barcodes,  before checking for duplicates with collector and number
        print('You entered "barcodes valid!"')

        # EXPERT
        # this combines all duplicated barcodes within a cell
        exp_dat.barcode = exp_dat.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    
        # we can retain Na values as these are not dropped
        exp_dat = exp_dat.reset_index(drop=True)
        # explode barcodes so they are separate and can be individually checked
        exp_dat['barcode_split'] = exp_dat['barcode'].str.split(', ')
        exploded_new_occs = exp_dat.explode('barcode_split')
        exploded_new_occs = exploded_new_occs.reset_index(drop = True)
        print(exploded_new_occs[['barcode_split', 'barcode']])

        # MASTER
        master.loc[master['barcode'].isna(), 'barcode'] = 'no_Barcode'
        master.barcode = master.barcode.apply(lambda x: ', '.join(set(x.split(', '))))  
        # we can retain Na values as these are not dropped
        master = master.reset_index(drop=True)
        # explode barcodes so they are separate and can be individually checked
        master['barcode_split'] = master['barcode'].str.split(', ')
        exploded_master_db = master.explode('barcode_split')
        exploded_master_db = exploded_master_db.reset_index(drop = True)
        exploded_master_db.barcode_split = exploded_master_db.barcode_split.replace('no_Barcode', pd.NA)
       
        tmp_master = pd.concat([exploded_new_occs, exploded_master_db], axis=0)

        tmp_master = tmp_master.astype(z_dependencies.final_col_type)
        sorted_tmaster = tmp_master.sort_values(['origin', 'det_year', 'recorded_by', 'colnum'], ascending=[True, True, True, False])

        sorted_tmaster.orig_bc = sorted_tmaster.orig_bc.fillna('')
        sorted_tmaster.orig_recby = sorted_tmaster.orig_recby.fillna('')
        sorted_tmaster.modified = sorted_tmaster.modified.fillna('')
        sorted_tmaster.geo_issues = sorted_tmaster.geo_issues.fillna('NONE')
        
        # IF COORDINATES
        if {'ddlat'}.issubset(exp_dat.columns):
            # Expert specific deduplication by barcodes. Sets coordinates to Expert values
            master_bc_agg = sorted_tmaster.groupby(['barcode_split'], as_index = False).agg(
                        scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                        genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                        specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                        species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                        collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                        recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                        colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                        prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                        colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                        sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                        col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'last' ),
                        col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'last' ),
                        col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'last' ),
                        col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'last' ),
                        det_by = pd.NamedAgg(column = 'det_by', aggfunc = 'first'),
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
                        institute = pd.NamedAgg(column = 'institute', aggfunc = 'last' ),
                        herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = 'last' ),
                        barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)),
                        orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc = 'last' ),
                        coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'last' ),
                        huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'last'),
                        geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'last'),
                        wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'last'),
                        expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                        status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                        accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                        ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                        ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                        link =  pd.NamedAgg(column = 'link', aggfunc='last'),
                        orig_recby = pd.NamedAgg(column = 'orig_recby', aggfunc='last'),
                        geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc='last'),
                        source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                        origin =  pd.NamedAgg(column = 'origin', aggfunc = 'first'),
                        modified = pd.NamedAgg(column = 'modified',  aggfunc='last')
                        ) 
        else:
            # Expert specific deduplication by barcodes. No coordinates included in expert data so 'last'
            master_bc_agg = sorted_tmaster.groupby(['barcode_split'], as_index = False).agg(
                        scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                        genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                        specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                        species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                        collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                        recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                        colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                        prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                        colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                        sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                        col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'last' ),
                        col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'last' ),
                        col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'last' ),
                        col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'last' ),
                        det_by = pd.NamedAgg(column = 'det_by', aggfunc = 'first'),
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
                        institute = pd.NamedAgg(column = 'institute', aggfunc = 'last' ),
                        herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = 'last' ),
                        barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)),
                        orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc = 'last' ),
                        coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'last' ),
                        huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'last'),
                        geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'last'),
                        wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'last'),
                        expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                        status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                        accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                        ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                        ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                        link =  pd.NamedAgg(column = 'link', aggfunc='last'),
                        orig_recby = pd.NamedAgg(column = 'orig_recby', aggfunc='last'),
                        geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc='last'),
                        source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                        origin =  pd.NamedAgg(column = 'origin', aggfunc = 'first'),
                        modified = pd.NamedAgg(column = 'modified',  aggfunc='last')
                        ) 
    # # # go through and deduplicate by full barcodes:

        master_bc_agg.barcode = master_bc_agg.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell

        # print(master_bc_agg.barcode, 'Issues?')

        master_bc_agg = master_bc_agg.sort_values(['origin', 'det_year', 'recorded_by', 'colnum'], ascending=[True, True, True, False])
        master_bc_agg.orig_bc = master_bc_agg.orig_bc.fillna('')
        master_bc_agg.orig_recby = master_bc_agg.orig_recby.fillna('')
        master_bc_agg.modified = master_bc_agg.modified.fillna('')
        master_bc_agg.geo_issues = master_bc_agg.geo_issues.fillna('')

        barcodes_done = master_bc_agg.groupby(['barcode'], as_index = False).agg(
                    scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                    genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                    specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                    species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                    collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                    recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                    colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                    prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                    colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                    sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                    col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'first' ),
                    col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'first' ),
                    col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'first' ),
                    col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'first' ),
                    det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                    det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                    det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                    det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                    det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                    country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'first' ),
                    country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
                    continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
                    locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
                    coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'first' ),
                    ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
                    ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
                    institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
                    herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
                    barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)),
                    orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x)),
                    coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                    huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                    geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                    wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                    expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                    status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                    accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                    origin =  pd.NamedAgg(column = 'origin', aggfunc = 'first'),
                    ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                    ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                    link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                    geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x)),
                    orig_recby = pd.NamedAgg(column = 'orig_recby', aggfunc=lambda x: ', '.join(x)),
                    source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                    modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
                )

        barcodes_done = barcodes_done.sort_values(['recorded_by', 'colnum'], ascending = [True, True])
        # split into deduplicateable data not for next step
        master_sn  = barcodes_done[barcodes_done[['recorded_by', 'colnum']].isna().any(axis=1)]
        # here exp and master already merged
        all_data = barcodes_done[barcodes_done[['recorded_by', 'colnum']].notna().all(axis=1)]

        

    else:
        # prepare deduplication without barcode information
        master_sn  = master[master[['recorded_by', 'colnum']].isna().any(axis=1)]
        master_num = master[master[['recorded_by', 'colnum']].notna().all(axis=1)]

        # merge master and exp
        all_data = pd.concat([master_num, exp_dat], axis=0)
        print('All data', sum(all_data.duplicated(subset =  ['recorded_by', 'prefix', 'colnum', 'sufix'], keep=False)))

        #deduplicated > extract num and sn for the non-matched barcodes
        print('here1', all_data[all_data.recorded_by.isna()])

    # end of if/else barcodes
    # now proceed normally
    
    if sum(all_data.duplicated(subset =  ['recorded_by', 'prefix', 'colnum', 'sufix'], keep=False)) > 0:
        print('the master database is not fully deduplicated') 
        print('Total records:', len(all_data),
                            '\n By recorded_by, colnum, sufix', 
                            all_data.duplicated([ 'recorded_by', 'colnum', 'sufix'], keep='first').sum(),
                            '\n By BARCODE (!this includes NA barcodes!)', 
                            all_data.duplicated([ 'barcode' ], keep='first').sum(),
                            '\n ................................................. \n ')
        masternum_sorted = all_data.sort_values(['det_year','recorded_by', 'colnum', 'origin'], ascending=[True, True, True, True])
        masternum_sorted.det_year = masternum_sorted.det_year.fillna(-9999)

        to_print = masternum_sorted[masternum_sorted.duplicated(subset =  ['recorded_by', 'prefix', 'colnum', 'sufix'], keep=False)]
        print(to_print[['recorded_by', 'prefix', 'colnum', 'sufix', 'origin', 'det_year']])
        print(to_print[to_print.recorded_by.isna()])

    # only numbered specimesn!!

    # dups has duplicated records
    dups = all_data[all_data.duplicated(subset =  ['recorded_by', 'prefix', 'colnum', 'sufix'], keep=False)]
    # all the non duplicated records
    singletons = all_data[~all_data.duplicated(subset =  ['recorded_by', 'prefix', 'colnum', 'sufix'], keep=False)]

    # need to sort by coordinates etc and deduplicated differently if so.

    dups = dups.astype(z_dependencies.final_col_type)
    # housekeeping
    dups.loc[dups['prefix'] == '<NA>', ['prefix']] = '-9999'
    dups.loc[dups['sufix'] == '<NA>', ['sufix']] = '-9999'



    # sort values most important is EXP (= origin here)
    dups_sorted = dups.sort_values(['recorded_by', 'colnum', 'origin', 'det_year'], ascending=[True, True, True, False])
    print('HERE\n',dups_sorted[['recorded_by', 'colnum', 'det_by', 'accepted_name', 'origin']])

    
    if exp_dat.accepted_name.notna().all():

        # master_out[col] = master_out[col].fillna('-9999')
        dups_sorted.prefix = dups_sorted.prefix.fillna('-9999')
        dups_sorted.sufix = dups_sorted.sufix.fillna('-9999')
        # we have (need!!) fully determined specimens!
        if {'ddlat'}.issubset(exp_dat.columns):
            # IF we have coordinates in the dets use those
            dup_additions = dups_sorted.groupby(['recorded_by', 'prefix', 'colnum', 'sufix'], as_index = False).agg(
                recorded_by = pd.NamedAgg(column='recorded_by', aggfunc='first'), 
                det_by = pd.NamedAgg(column='det_by', aggfunc='first'), 
                det_year = pd.NamedAgg(column='det_year', aggfunc='first'),
                accepted_name = pd.NamedAgg(column='accepted_name', aggfunc='first'),
                expert_det = pd.NamedAgg(column='det_by', aggfunc=lambda x: 'EXP'),
                ipni_no = pd.NamedAgg(column='ipni_no', aggfunc='first'),
                ipni_pub = pd.NamedAgg(column='ipni_pub', aggfunc='first'),
                status = pd.NamedAgg(column='status', aggfunc=lambda x: 'ACCEPTED'),
                ddlat = pd.NamedAgg(column='ddlat', aggfunc='first'),
                ddlong = pd.NamedAgg(column='ddlong', aggfunc='first'),
                barcode = pd.NamedAgg(column='barcode', aggfunc='last')
            )
            print('the insert df',dup_additions)
           # print(dup_additions[dup_additions.det_by == 'Maas, PJM'])

            # subset original (complete data) 

            dups_master = dups[dups.origin == 'MASTER']
            # assign updated values to the original data

            print('the df to be mod:\n', dups_master[['recorded_by', 'det_by', 'colnum', 'accepted_name']])
            dups_master = dups_master.assign(recorded_by = dup_additions.recorded_by,
                                            det_by = dup_additions.det_by,
                                            det_year = dup_additions.det_year,
                                            accepted_name = dup_additions.accepted_name,
                                            expert_det = dup_additions.expert_det,
                                            ddlat = dup_additions.ddlat,
                                            ddlong = dup_additions.ddlong)

        else:
            # No coordinates, just taxonomy
            dup_additions = dups_sorted.groupby(['recorded_by', 'prefix', 'colnum', 'sufix'], as_index = False).agg(
                recorded_by = pd.NamedAgg(column='recorded_by', aggfunc='first'), 
                det_by = pd.NamedAgg(column='det_by', aggfunc='first'), 
                det_year = pd.NamedAgg(column='det_year', aggfunc='first'),
                accepted_name = pd.NamedAgg(column='accepted_name', aggfunc='first'),
                expert_det = pd.NamedAgg(column='det_by', aggfunc=lambda x: 'EXP'),
                ipni_no = pd.NamedAgg(column='ipni_no', aggfunc='first'),
                ipni_pub = pd.NamedAgg(column='ipni_pub', aggfunc='first'),
                status = pd.NamedAgg(column='status', aggfunc=lambda x: 'ACCEPTED'),
                barcode = pd.NamedAgg(column='barcode', aggfunc='last')
            )
            print('the insert df\n',dup_additions[['recorded_by', 'det_by', 'colnum', 'prefix', 'sufix', 'accepted_name', 'barcode']])
           # print('PROBLEMS??????:\n',dup_additions[dup_additions.barcode.isna()][['recorded_by', 'colnum', 'det_by', 'accepted_name']])


            # subset original (complete data) 
            dups_master = dups[dups.origin == 'MASTER']
            # assign updated values to the original data
            print('the df to be mod:\n', dups_master[['recorded_by', 'det_by', 'colnum', 'prefix', 'sufix', 'accepted_name', 'barcode']])

            dups_full = pd.merge(dups_master, dup_additions, on='barcode', how='outer', suffixes=('', '_exp'))
            print(dups_full.columns)

            dups_full['recorded_by'] = dups_full['recorded_by_exp'].combine_first(dups_full['recorded_by'])
            dups_full['det_by'] = dups_full['det_by_exp'].combine_first(dups_full['det_by'])
            dups_full['det_year'] = dups_full['det_year_exp'].combine_first(dups_full['det_year'])
            dups_full['accepted_name'] = dups_full['accepted_name_exp'].combine_first(dups_full['accepted_name'])
            dups_full['expert_det'] = dups_full['expert_det_exp'].combine_first(dups_full['expert_det'])
            dups_full['ipni_no'] = dups_full['ipni_no_exp'].combine_first(dups_full['ipni_no'])
            dups_full['status'] = dups_full['status_exp'].combine_first(dups_full['status'])

            # dups_master = dups_master.assign(recorded_by = dup_additions.recorded_by,
            #                                 det_by = dup_additions.det_by,
            #                                 det_year = dup_additions.det_year,
            #                                 accepted_name = dup_additions.accepted_name,
            #                                 expert_det = dup_additions.expert_det
            #                                 )
        dups_full = dups_full.sort_values(['recorded_by', 'colnum', 'origin', 'det_year'], ascending=[True, True, True, False])


        print(dups_full[['recorded_by', 'colnum', 'origin', 'det_by_exp', 'det_year', 'accepted_name', 'accepted_name_exp', 'expert_det']])


        singl_dedup = pd.concat([singletons, dups_full], axis=0)

        master_out = pd.concat([singl_dedup, master_sn], axis=0)



        master_out.loc[master_out['det_year'] == -9999, 'det_year'] = pd.NA

        return master_out
    else:
        raise(Exception('No determinations in EXPERT data!\n Are you sure this is ok what you want? Double check your data'))
        

    


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
        exp_data['sp_idx'] = exp_data['accepted_name']
        exp_data.set_index(exp_data.sp_idx, inplace = True)
        occs_toquery = exp_data[['accepted_name']].astype(str).copy()
        occs_toquery[['accepted_name']] = occs_toquery[['accepted_name']].replace('nan', pd.NA)
    #occs_toquery[['genus', 'specific_epithet']] = occs_toquery[['genus', 'specific_epithet']].replace('None', pd.NA)
        occs_toquery['sp_idx'] = occs_toquery['accepted_name']
        logging.debug('The is the index and length of taxa column (contains duplicated taxon names; should be same length as input dataframe)')
        logging.debug(f'{occs_toquery.sp_idx}')
        logging.debug(f'{len(occs_toquery.sp_idx)}')
        occs_toquery.set_index(occs_toquery.sp_idx, inplace = True)

        occs_toquery = occs_toquery.dropna(how='all', subset=['accepted_name']) # these are really bad for the query ;-)
    # drop duplicated genus-species combinations (callable by index in final dataframe)
        occs_toquery = occs_toquery.drop_duplicates(subset = 'sp_idx', keep = 'last')
        logging.info(f'Number of unique taxa to check: {len(occs_toquery.sp_idx)}')
    # SWIFTER VERSION
        occs_toquery[[ 'ipni_species_author', 'ipni_no', 'ipni_pub']] = occs_toquery.swifter.apply(lambda row: expert_ipni(row['accepted_name']), axis = 1, result_type='expand')

 
        occs_toquery = occs_toquery.set_index('sp_idx')
        occs_toquery = occs_toquery.drop(['accepted_name'], axis = 1)
        exp_dat_out = exp_data.join(occs_toquery)

        #exp_data[['ipni_species_author', 'ipni_no', 'ipni_pub']] = exp_data.swifter.apply(lambda row: expert_ipni(row['accepted_name']), axis = 1, result_type='expand')

        return exp_dat_out





############### OTHER USEFUL STUFF ###########################

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


expert_min_types= {
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
    'accepted_name': str,
    'ipni_no': str,
    }



############### END ###########################