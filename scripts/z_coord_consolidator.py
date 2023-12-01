#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Coordinate discrepancies consolidator:
    Take duplicates with diverging coordinates and check and correct for obvious errors.

prefix_cleaner:
    checks for prefixes extracted where the collector surname is found in the full collection number
'''

import warnings

import pandas as pd
import numpy as np
# import codecs
# import os
# import regex as re
# import requests
# import swifter
# import logging




def coord_consolidator(occs, verbose=True, debug=False):
    """
    occs: dataframe to check and consolidate coordinate discrepancies
    """

    # remove 0 coordinates
    occs[occs.ddlat == 0 ] = pd.NA
    occs[occs.ddlong == 0 ] = pd.NA

    occs = occs.drop_duplicated()

    # we need useable species
    sp_problem = occs[occs.accepted_name.isna()]
    occs = occs[~occs.accepted_name.isna()]
    # split dataset by s.n. and non s.n.
    sn_occs = occs[occs.colnum.isna()]
    num_occs = occs[~occs.colnum.isna()]

    # COLNUM not NA
    # group by collector, number, prefix, sufix, 
    grouped_num = occs.groupby(['recorded_by', 'prefix', 'sufix', 'colnum', 'country', ])


    # NA-COLNUM 
    # group by collector, number, prefix, sufix, 
    grouped_sn = occs.groupby()






    return occs_cleaned, occs_prob




# debug = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/recordcurator/test_data/test_coord_cons.csv', sep = ';')
# print(debug)






def prefix_cleaner(occs):
    """
    Clean prefixes to get better deduplication from master databases.
    """

    occs['surnames'] = occs.recorded_by.astype(str).str.split(',').str[0]
    print(occs.surnames)

    # housekeeping: NA
    occs.prefix = occs.prefix.replace('NA', pd.NA)
    occs.prefix = occs.prefix.replace('nan', pd.NA)
    
    mask = occs['prefix'] == occs['surnames']
    occs.loc[mask, 'prefix'] = pd.NA

    return occs

test = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_AfrAs_GDB/X_GLOBAL/master_db.csv', sep = ';')
test1 = prefix_cleaner(test)
print(test1)
test1.to_csv('/Users/serafin/Sync/1_Annonaceae/G_AfrAs_GDB/X_GLOBAL/master_db.csv', sep = ';', index=False)