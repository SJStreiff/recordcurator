#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for duplicate detection and tratment

2022-12-13 sjs

CHANGELOG:
    2022-12-13: created
    2024-02-01: cleaned and curated



duplicate_stats():
        does some stats on duplicates found with different combinations

'''


import pandas as pd
import numpy as np
import codecs
import os
import regex as re
# import swifter
import datetime 
import logging

import z_dependencies





def duplicate_stats(occs, working_directory, prefix, out=True, verbose=True, debugging=False):
    '''
    Function that prints a load of stats about potential duplicates
    '''
    #-------------------------------------------------------------------------------
    # these columns are used to identify duplicates (i.e. if a value of both these column is shared
    # for a row, then we flag the records as duplicates)
    dup_cols = ['coll_surname', 'colnum', 'sufix', 'col_year'] # the columns by which duplicates are identified
    #-------------------------------------------------------------------------------

    # cleaning up a bit

    # data types can be annoying
    occs = occs.astype(z_dependencies.final_col_type) # double checking
    #print(occs.dtypes)
    #occs[['recorded_by', 'colnum_full']] = occs[['recorded_by', 'colnum_full']].replace('nan', pd.NA)

    try:
        occs = occs.drop(['to_check', 'to_check_det'], axis = 'columns')
    except:
        logging.debug('No tmp_check to drop')

    #-------------------------------------------------------------------------------
    # MISSING collector information and number
    # remove empty collector and coll num
    occs1 = occs #.dropna(how='all', subset = ['recorded_by']) # no collector information is impossible.
    #occs1 = occs1[occs1.recorded_by != 'nan']
    logging.debug(f'Deleted {len(occs.index) - len(occs1.index)} rows with no collector and no number; {len(occs1.index)} records left')
    
    #-------------------------------------------------------------------------------
    # MISSING col num
    # these are removed, and added later after processing(?)
    subset_col = ['colnum_full']

    occs1 = occs1.replace({'NaN': pd.NA}, regex=False)
    occs1 = occs1.replace({'nan': pd.NA}, regex=False)

    occs_colNum = occs1.dropna(how='all', subset=['colnum'])
    print('I dropped', len(occs1)-len(occs_colNum), 'NA colnum records')
    occs_nocolNum = occs1[occs1['colnum'].isna()]
    print(occs1.colnum.dtypes, occs1['colnum'].isna())

    occs_colNum.ddlong.astype(float)
    occs_nocolNum['ddlong'].astype(float)
    occs_colNum.ddlat.astype(float)
    occs_nocolNum['ddlat'].astype(float)

    occs_colNum['coll_surname'] = occs_colNum['recorded_by'].str.split(',', expand=True)[0]
    logging.info(f'{occs_colNum.coll_surname}')

    #if len(occs_nocolNum)>0:
    #    occs_nocolNum.to_csv(working_directory+prefix+'s_n.csv', index = False, sep = ';' )
    #    print('\n The data with no collector number was saved to', working_directory+prefix+'s_n.csv', '\n')

    #-------------------------------------------------------------------------------
    # Perform some nice stats, just to get an idea what we have

    occs_colNum.colnum_full.astype(str)
    
    print('Total records:', len(occs),';\n Records with colNum_full:', len(occs_colNum),';\n Records with no colNum_full:', len(occs_nocolNum),
                            '\n \n Some stats about potential duplicates: \n .................................................\n',
                            '\n By Collector-name and FULL collector number', occs_colNum[occs_colNum.duplicated(subset=['recorded_by', 'colnum_full'], keep=False)].shape,
                            '\n By NON-STANDARD Collector-name and FULL collector number', occs_colNum[occs_colNum.duplicated(subset=['orig_recby', 'colnum_full'], keep=False)].shape,
                            '\n By Collector-name and FULL collector number, and coordinates', occs_colNum.duplicated(['recorded_by', 'colnum_full', 'ddlat', 'ddlong'], keep=False).sum(),
                            '\n By Collector-name and FULL collector number, genus and specific epithet', occs_colNum.duplicated(['recorded_by', 'colnum_full', 'genus' , 'specific_epithet'], keep=False).sum(),
                            '\n By FULL collector number, genus and specific epithet', occs_colNum.duplicated([ 'colnum_full', 'genus' , 'specific_epithet'], keep=False).sum(),
                            '\n By FULL collector number and genus', occs_colNum.duplicated([ 'colnum_full', 'genus' ], keep=False).sum(),
                            '\n By FULL collector number, collection Year and country', occs_colNum.duplicated([ 'colnum_full', 'col_year' , 'country'], keep=False).sum(),
                            '\n By collection Year and FULL collection number (checking for directionality)', occs_colNum.duplicated([ 'col_year' , 'colnum_full'], keep=False).sum(),
                            '\n By REDUCED collection number and collection Yeear', occs_colNum.duplicated([ 'colnum', 'col_year' ], keep=False).sum(),
                            '\n By locality, REDUCED collection number and collection Year', occs_colNum.duplicated([ 'locality', 'colnum' , 'col_year' ], keep=False).sum(),
                            '\n By SURNAME and COLLECTION NUMBER', occs_colNum.duplicated([ 'coll_surname', 'colnum' ], keep=False).sum(),
                            '\n By SURNAME and FULL COLLECTION NUMBER', occs_colNum.duplicated([ 'coll_surname', 'colnum_full' ], keep=False).sum(),
                            '\n By SURNAME and COLLECTION NUMBER and YEAR', occs_colNum.duplicated([ 'coll_surname', 'colnum', 'col_year' ], keep=False).sum(),
                            '\n By SURNAME and COLLECTION NUMBER, SUFIX and YEAR', occs_colNum.duplicated([ 'coll_surname', 'colnum', 'sufix', 'col_year' ], keep=False).sum(),
                            '\n By HUH-NAME and COLLECTION NUMBER, SUFIX and YEAR', occs_colNum.duplicated([ 'huh_name', 'colnum', 'sufix', 'col_year' ], keep=False).sum(),
                            '\n ................................................. \n ')
    


    # this function only returns records with no collection number!
    if out:
        return occs_colNum, occs_nocolNum


