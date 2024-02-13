#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''

2022-12-13 sjs

CHANGELOG:
    2022-12-13: created
    2024-01-29: cleaned and curated


column_standardiser():
        reads dataframe and subsets columns, adds empty columns for standardised format

'''

import pandas as pd
import codecs
import logging

#custom dependencies
import z_dependencies # can be replaced at some point, but later...




def column_standardiser(importfile, data_source_type, verbose=True, debugging = False):
    ''' reads a file, checks the columns and subsets and adds columns where
    necessary to be workable later down the line.'''


    imp = codecs.open(importfile,'r','utf-8') #open for reading with "universal" type set

    #-------------------------------------------------------------------------------
    # different sources have different columns, here we treat the most common source types,
    # rename the odd columns, and subset the wished ones
    # (dictionary and list of wanted columns in auxilary variable files, can be easily added and modified)

    if(data_source_type == 'P'):
        # for data in the format of herbonautes from P
        logging.info('data type P')
        occs = pd.read_csv(imp, sep = ';',  dtype = str) # read the data
        occs = occs.fillna(pd.NA)
        occs = occs.rename(columns = z_dependencies.herbo_key) # rename columns
        occs = occs[z_dependencies.herbo_subset_cols] # subset just required columns
        occs['source_id'] = 'P_herbo'
        # if verbose:
 	    #       print('Just taking the Philippines for now!')
        # occs = occs[occs['country'] == 'Philippines']

    elif(data_source_type == 'GBIF'):
        # for all data in the darwin core format!!
        logging.info('data type GBIF')
        occs = pd.read_csv(imp, sep = '\t',  dtype = str, na_values=pd.NA, quotechar='"') # read data
        occs = occs[occs['basisOfRecord'] == "PRESERVED_SPECIMEN"] # remove potential iNaturalist data....
        occs = occs[occs['occurrenceStatus'] == 'PRESENT'] # loose absence data from surveys
        try:
            occs['references'] = occs['references'].fillna(occs['bibliographicCitation'])
        except:
            try:
                occs['references'] = occs['references'].fillna('')
            except:
                occs['references'] = ''    
        # here we a column species-tobesplit, as there is no single species columns with only epithet
        occs = occs.rename(columns = z_dependencies.gbif_key) # rename

        occs = occs[z_dependencies.gbif_subset_cols] # and subset
        #occs = occs.fillna(pd.NA) # problems with this NA
        occs['source_id'] = 'gbif'

        # print(occs.link)

    elif(data_source_type == 'BRAHMS'):
        # for data from BRAHMS extracts
        logging.info('data type BRAHMS')
        occs = pd.read_csv(imp, sep = ';',  dtype = str, na_values=pd.NA, quotechar='"') # read data
        occs = occs.rename(columns = z_dependencies.brahms_key) # rename
        occs = occs[z_dependencies.brahms_cols] # and subset
        # print('READ3',occs.columns)
        #occs = occs.fillna(pd.NA) # problems with this NA
        occs['source_id'] = 'brahms'

    elif(data_source_type == 'MO_tropicos'):
        # for data from BRAHMS extracts
        logging.info('data type MO')
        occs = pd.read_csv(imp, sep = ';',  dtype = str, na_values=pd.NA, quotechar='"') # read data
        
        occs = occs.rename(columns = z_dependencies.MO_key) # rename

        occs = occs[z_dependencies.MO_cols] # and subset
        #occs = occs.fillna(pd.NA) # problems with this NA
        occs['source_id'] = 'MO_tropicos'


        # print(occs[occs.scientific_name == 'Mosannona raimondii'])

    elif(data_source_type == 'RAINBIO'):
        # for data from BRAHMS extracts
        logging.info('data type RAINBIO')
        occs = pd.read_csv(imp, sep = ';',  dtype = str, na_values=pd.NA, quotechar='"') # read data
        
        occs = occs.rename(columns = z_dependencies.rainbio_key) # rename

        occs = occs[z_dependencies.rainbio_cols] # and subset
        #occs = occs.fillna(pd.NA) # problems with this NA
        occs['source_id'] = 'RAINBIO'

    elif(data_source_type=='SPLINK'):
         # for data from BRAHMS extracts
        logging.info('data type SPLINK')
        occs = pd.read_csv(imp, sep = '\t',  dtype = str, na_values=pd.NA, quotechar='"') # read data
        
        occs = occs.rename(columns = z_dependencies.splink_key) # rename

        occs = occs[z_dependencies.splink_cols] # and subset
        #occs = occs.fillna(pd.NA) # problems with this NA
        occs['source_id'] = 'SPLINK'




    else:
        logging.warning('datatype not found')
        # maybe think if we want to somehow merge and conserve the plant description
        # for future interest (as in just one column 'plantdesc'???)
    
    logging.info(f'The columns now are: {occs.columns}')

    #occs = occs.replace(';', ',')
    logging.info(f'{occs}')
    #-------------------------------------------------------------------------------
    # add all final columns as empty columns
    # check for missing columns, and then add these, as well as some specific trimming
    # splitting etc...
    miss_col = [i for i in z_dependencies.final_cols if i not in occs.columns]


    logging.debug(f'These columns are missing in the data from source: {miss_col} Empty columns will be added and can later be modified.')
    logging.debug(f'{occs.dtypes}')
    occs[miss_col] = '0'
    #occs = occs.apply(lambda col: pd.to_numeric(col, errors='coerce').astype(float))
#        test_upd_DB.colnum = pd.to_numeric(test_upd_DB.colnum, errors='coerce').astype(pd.Int64Dtype())
    logging.debug(f'Testing new type implementation: {occs.dtypes}')
    logging.info(f'{occs}')

    # print(occs[['col_day', 'col_month', 'col_year']])
    for column in occs.columns:
        # Iterate over each row in the column
        for index, value in occs[column].items():
            try:
                # Try converting the value to float
                float(value)
            except ValueError:
                a =1 
            except TypeError:
                # Print the column, index, and value where the error occurred
                #print(f"Error in column '{column}', index '{index}': {value}")
                b=1
 
    occs = occs.astype(dtype = z_dependencies.final_col_type, errors='raise')
    # print(occs.dtypes)
    #_bc
    return occs
