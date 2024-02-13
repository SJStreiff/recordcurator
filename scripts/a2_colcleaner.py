#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
2022-12-13 sjs

CHANGELOG:
    2022-12-13: created
    2024-01-29: cleaned and curated

column_cleaning():
        takes columns and puts the content into standard/exchangeable format
'''

import pandas as pd
import logging

#custom dependencies
import z_dependencies # can be replaced at some point, but later...



def column_cleaning(occs, data_source_type, working_directory, prefix, verbose=True, debugging = False):
    '''
    Cleaning up the columns to all be the same...
    '''
    logging.info('Standardising column names.')
    if(data_source_type == 'P'):
        """things that need to be done in the herbonautes data:
            - coordinate split (is ['lat, long'], want ['lat'], ['long'])
            - date split coll
            - date split det
            - split ColNum into pre-, main and sufix!
        """
        # COORDINATES
        occs[['ddlat', 'ddlong']] = occs.coordinates.str.split(",", expand = True)
        occs['ddlat'] = occs['ddlat'].str.strip() # trim any trailing spaces
        occs['ddlong'] = occs['ddlong'].str.strip()
        occs.drop(['coordinates'], axis = 'columns', inplace=True)

        # DATES

        # herbonautes data has 2 dates, start and end. We have decided to just take the first.
        # sometimes these are identical, but sometimes these are ranges.

        occs[['col_date_1', 'col_date_2']] = occs.col_date.str.split("-", expand=True,)
        occs[['det_date_1', 'det_date_2']] = occs.det_date.str.split("-", expand=True,)
        #delete the coldate_2 colomn, doesn't have info we need
        occs.drop(['col_date_2'], axis='columns', inplace=True)
        occs.drop(['det_date_2'], axis='columns', inplace=True)



        #split colDate_1 on '/' into three new fields
        occs.col_date_1 = occs.col_date_1.replace('<NA>', '0/0/0')
        occs.det_date_1 = occs.det_date_1.replace('<NA>', '0/0/0')

        occs[['col_day', 'col_month', 'col_year']] = occs.col_date_1.str.split("/", expand=True,).astype(pd.Int64Dtype())
        occs[['det_day', 'det_month', 'det_year']] = occs.det_date_1.str.split("/", expand=True,).astype(pd.Int64Dtype())
  
        # S: keep the very original col and det date cols, remove the colDate_1...
        occs.drop(['col_date_1'], axis='columns', inplace=True)
        occs.drop(['det_date_1'], axis='columns', inplace=True)

        logging.debug(f'Collection date formatting: {occs.col_date}')
        logging.debug('The collection date has now been split into separate (int) columns for day, month and year')


        # COLLECTION NUMBERS
        # keep the original colnum column
        #create prefix, extract text before the number

        occs['prefix'] = occs.colnum_full.str.extract('^([a-zA-Z]*)')
        # delete spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()

        #create sufix , extract text or pattern after the number
        # going from most specific to most general regex, this list takes all together in the end
        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only digits without associated stuff, but including some characters (colNum)
        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\-\d+)', # of structure 00-00
            r'(?:\d+\s\d+\s\d+)', # 00 00 00 or so
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs.colnum_full.str.extract('(' + '|'.join(regex_list_digits) + ')')
        # print(occs[['colnum_full', 'colnum']])
        occs['colnum'] = occs['colnum'].str.strip()


        # set missing coordinates to integer 0, then we can work, modified later
        occs.ddlat[pd.isna(occs.ddlat)] = 0
        occs.ddlong[pd.isna(occs.ddlong)] = 0

        # for completeness we need this
        occs['orig_bc'] = occs['barcode']
        occs = occs.astype(dtype = z_dependencies.final_col_type)
        logging.debug(f'{occs.dtypes}')
        occs = occs.replace({'nan': pd.NA}, regex=False) # remove NAs that aren't proper


    #-------------------------------------------------------------------------------
    # go through data for specific data sources
    if(data_source_type == 'GBIF'):
        """things that need to be done in the GBIF data:
            - BARCODE (just number or Herbcode + number)
            - specific epithet!
            - record number (includes collectors regularly...)
        """
    # -----------------------------------------------------------------------
    # split dates into format we can work with
        logging.info(f'{occs}')
        occs = occs.astype(dtype = z_dependencies.final_col_type)

        # format det date into separate int values yead-month-day
        occs['tmp_det_date'] = occs['det_date'].str.split('T', expand=True)[0]
        try:
            occs[['det_year', 'det_month', 'det_day']] = occs['tmp_det_date'].str.split("-", expand=True)
            occs = occs.drop(['tmp_det_date'], axis='columns')
        except:
            logging.debug('no det dates available...')

        logging.debug(f'{occs.dtypes}')

        occs = occs.replace({'nan': pd.NA}, regex=False)
        logging.debug(occs.det_year)


    # -----------------------------------------------------------------------
    # Barcode issues:
    # sonetimes the herbarium is in the column institute, sometimes herbarium_code, sometimes before the actual barcode number...
            # check for different barcode formats:
            # either just numeric : herb missing
            #     --> merge herbarium and code
        logging.info('Reformatting problematic barcodes and standardising them.')
        logging.debug(f'{occs.barcode}')

        # if there is a nondigit, take it away from the digits, and modify it to contain only letters
        barcode_regex = [
            r'^(\w+)$', # digit and letters
            r'(\d+\-\d+\/\d+)$', # digit separated by - and /
            r'(\d+\:\d+\:\d+)$', # digit separated by :
            r'(\d+\-\d+)$', # digit separated by - 
            r'(\d+\/\d+)$', # digit separated by /
            r'(\d+\.\d+)$', # digit separated by .
            r'(\d+\s\d+)$', # digit separated by space
            r'(\d+)$', # digit
        ]
        # extract the numeric part of the barcode
        bc_extract = occs['barcode'].astype(str).str.extract('(' + '|'.join(barcode_regex) + ')')
        #occs['prel_bc'] = occs['prel_bc'].str.strip()

        logging.debug(f'Numeric part of barcodes extracted {bc_extract}')

        i=0
        while(len(bc_extract.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
            i = i+1
            bc_extract.iloc[:,-1] = bc_extract.iloc[:,-1].fillna(bc_extract.iloc[:,-2])
            bc_extract = bc_extract.drop(bc_extract.columns[-2], axis = 1)
            #print(names_WIP) # for debugging, makes a lot of output
            #print('So many columns:', len(names_WIP.columns), '\n')
        logging.debug(f'barcodes extracted: {bc_extract}')
        # reassign into dataframe
        occs['prel_bc'] = bc_extract

        logging.debug(f'Prelim. barcode: {occs.prel_bc}')
        # now get the herbarium code. First if it was correct to start with, extract from barcode.
        bc = pd.Series(occs['barcode'])
        insti = pd.Series(occs['institute'])
        contains_full_words = lambda x: pd.NA if pd.Series(x).str.contains('[a-z]').any() else x
        insti = insti.apply(contains_full_words)

        prel_herbCode = occs['barcode'].str.extract(r'(^[A-Z]+\-[A-Z]+\-)') # gets most issues... ABC-DE-000000
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'(^[A-Z]+\s[A-Z]+)')) # ABC DE00000
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'(^[A-Z]+\-)')) # ABC-00000
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'(^[A-Z]+)')) #ABC000
        prel_herbCode = prel_herbCode.replace('nan', '0')

        # If still no luck, take the capital letters in 'Institute'
        prel_herbCode = prel_herbCode.fillna(insti.str.extract(r'(^[A-Z]+)')) # desperately scrape whatever is in the 'institute' column if we still have no indication to where the barcode belongs

        occs = occs.assign(prel_herbCode = prel_herbCode)
        occs['prel_code'] = occs['barcode'].astype(str).str.extract(r'(\D+)')
        occs['prel_code_X'] = occs['barcode'].astype(str).str.extract(r'(\d+\.\d)') # this is just one entry and really f@#$R%g annoying
        #occs.to_csv('debug.csv', sep = ';')

        logging.debug(f'Prelim. barcode Type: {type(occs.prel_code)}')
        logging.debug(f'Institute would be: {occs.institute}')
        
        logging.debug(f'Prel. Herbarium code: {occs.prel_herbCode}')

        # if the barcode column was purely numericm integrate
        occs['tmp_hc'] = occs['institute'].str.extract(r'(^[A-Z]+\Z)')
        occs['tmp_hc'] = occs['barcode'].str.extract(r'(^[A-Z]+\-[A-Z]+\-)')
        #occs['hc_tmp_tmp'] = occs['herbarium_code'].str.extract(r'(^[A-Z]+\Z)')
        #occs['tmp_hc'].fillna(occs.hc_tmp_tmp, inplace = True)
        occs['tmp_hc'] = occs['tmp_hc'].replace({'PLANT': 'TAIF'})
        #occs['tmp_hc'] = occs['tmp_hc'].str.replace('PLANT', '')
        occs.prel_herbCode.fillna(occs['tmp_hc'], inplace = True)
        occs.prel_herbCode.fillna('', inplace = True)
        #occs[occs['herbarium_code'] == r'([A-Z]+)', 'tmp_test'] = 'True'
        
        logging.debug(f'TMP herb code: {occs.tmp_hc}')
        # this now works, but,
            # we have an issue with very few institutions messing up the order in which stuff is supposed to be...
            # (TAIF)

        #occs.institute.fillna(occs.prel_herbCode, inplace=True)
        # print('DEBUG:', pd.isna(occs.prel_bc.str.extract(r'([A-Z])')))
        occs['sel_col_bc'] = pd.isna(occs['prel_bc'].str.extract(r'([A-Z])'))
        occs.loc[occs['sel_col_bc'] == False, 'prel_herbCode'] = ''
        logging.debug(f'prel_code: {occs.prel_herbCode}')
        logging.debug(f'{occs.prel_bc}')
        occs.drop


        occs['st_barcode'] = occs['prel_herbCode'] + occs['prel_bc']
        occs['st_barcode'] = occs.st_barcode.astype(str)
        
        logging.debug(f'{occs.st_barcode}')
    #    prel_herbCode trumps all others,
    #        then comes herbarium_code, IF (!) it isn't a word (caps and non-caps) and if it is not (!!) 'PLANT'
    #           then comes institute


        logging.debug(f'Now these columns: {occs.columns}')
# barcodes_new = barcodes_new.replace({'^[A-Z]+0$': 'no_Barcode'}, regex=True)
        occs.st_barcode = occs.st_barcode.replace({'^[A-Z]+0$': 'no_Barcode'}, regex=True)
        occs.loc[occs['st_barcode'] == 'no_Barcode', 'st_barcode'] = pd.NA 
        if occs.st_barcode.isna().sum() > 0:
        
            logging.info('I couldn\'t standardise the barcodes of some records. This includes many records (if from GBIF) with barcode = NA')
            na_bc = occs[occs['st_barcode'].isna()]
            na_bc.to_csv(working_directory + prefix + 'NA_barcodes.csv', index = False, sep = ';', )
            logging.info(f'I have saved {len(na_bc)} occurences to the file {working_directory+prefix}NA_barcodes.csv for corrections')

            logging.info('I am continuing without these.')
            occs = occs[occs['st_barcode'].notna()]
            
            logging.debug(f'Occs: {occs}')

        # and clean up now
        occs = occs.drop(['prel_bc', 'prel_herbCode', 'prel_code', 'prel_code_X', 'tmp_hc', 'sel_col_bc'], axis = 1)
        occs = occs.rename(columns = {'barcode': 'orig_bc'})
        occs = occs.rename(columns = {'st_barcode': 'barcode'})
        
        logging.debug(f'{occs.columns}')
        # -----------------------------------------------------------------------
        # COLLECTION NUMBERS
        # keep the original colnum column
        # split to get a purely numeric colnum, plus a prefix for any preceding characters,
        # and sufix for trailing characters
        
        logging.debug(f'{occs.colnum_full}')

        #create prefix, extract text before the number
        occs['prefix'] = occs.colnum_full.str.extract('^([a-zA-Z]*)')
        ##this code deletes spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()

        #create sufix , extract text or pattern after the number
        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only number (colNam)

        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\-\d+)', # of structure 00-00
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_digits) + ')')
        # print(occs[['colnum_full', 'colnum']])

        occs['colnum'] = occs['colnum'].str.strip()

        occs['colnum'].replace('nan', pd.NA, inplace=True)

        #print(occs)
        # ok this looks good for the moment.

        # SPECIFIC EPITHET
        # darwin core specific...
        occs[['genus', 'specific_epithet']] = occs['species-tobesplit'].str.split(' ', expand = True)
        #print(occs.tmp)

        occs[['tmp', 'specific_epithet']] = occs['species-tobesplit'].str.split(' ', expand = True)
        logging.debug(f'{occs.tmp}')

        occs.drop(['tmp'], axis='columns', inplace=True)
        occs.drop(['species-tobesplit'], axis='columns', inplace=True)


    #-------------------------------------------------------------------------------
    if(data_source_type == 'BRAHMS'):
        """things that need to be done in the BRAHMS data:
            - colnum_full for completeness (= prefix+colnum+sufix)
        """
        #remove odd na values
        occs.prefix = occs.prefix.str.replace('nan', '')
        occs.sufix = occs.sufix.str.replace('nan', '')
        # make colnum_full
        occs.colnum_full = occs.prefix + occs.colnum + occs.sufix
        # for completeness we need this
        occs['orig_bc'] = occs['barcode']
        occs['country_id'] = pd.NA


    #-------------------------------------------------------------------------------
    if(data_source_type == 'MO_tropicos'):
        """things that need to be done in the MO data:
            - barcode: add 'MO'
            - region: merge upper and lower
            - colnum/colnum full cross fill, add prefix/sufix as NA (NA in data...)
        """
        #remove odd na values
        #create prefix, extract text before the number
        occs['prefix'] = occs.colnum_full.str.extract('^([a-zA-Z]*)')
        ##this code deletes spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()

        #create sufix , extract text or pattern after the number
        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only number (colNam)

        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\-\d+)', # of structure 00-00
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_digits) + ')')
        # print(occs[['colnum_full', 'colnum']])

        occs['colnum'] = occs['colnum'].str.strip()

        occs['colnum'].replace('nan', pd.NA, inplace=True)


        # make colnum_full
        occs['region'] = occs['upper-region'] + occs['lower-region']
        occs['colnum_full'] = occs.colnum
        # for completeness we need this
        occs['orig_bc'] = occs['barcode']
        occs['country_id'] = pd.NA
        occs['barcode'] = 'MO'+occs['barcode']
       
    #----------------------------------------------------------------------------------
    if(data_source_type == 'RAINBIO'):
        """ things needed:
         - colnum splitting
         """
        # COLLECTION NUMBERS

        # keep the original colnum column
        # print(occs.colnum_full)
   
        occs['prefix'] = occs.colnum_full.str.extract('^([a-zA-Z]*)')
        ##this code deletes spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()
        # print(occs.prefix)
    
        # going from most specific to most general regex, this list takes all together in the end
        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only digits without associated stuff, but including some characters (colNam)
        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\-\d+)', # of structure 00-00
            r'(?:\d+\s\d+\s\d+)', # 00 00 00 or so
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs.colnum_full.str.extract('(' + '|'.join(regex_list_digits) + ')')
        # print(occs[['colnum_full', 'colnum']])
        occs['colnum'] = occs['colnum'].str.strip()

        # print(sum(pd.isna(occs.ddlat)))
        occs.ddlat[pd.isna(occs.ddlat)] = 0
        occs.ddlong[pd.isna(occs.ddlong)] = 0

        occs['orig_bc'] = occs['barcode']
        occs['country_id'] = pd.NA
        occs = occs.astype(dtype = z_dependencies.final_col_type)

        logging.debug(f'{occs.dtypes}')

        occs = occs.replace({'nan': pd.NA}, regex=False) # remove NAs that aren't proper


    #----------------------------------------------------------------------------------
    if(data_source_type == 'SPLINK'):
        """ things needed:
         - colnum splitting
         - BARCODE formatting
         """
    # -----------------------------------------------------------------------
    # Barcode issues:
    # sonetimes the herbarium is in the column institute, sometimes herbarium_code, sometimes before the actual barcode number...
            # check for different barcode formats:
            # either just numeric : herb missing
            #     --> merge herbarium and code

        logging.info('Reformatting problematic barcodes and standardising them.')
        logging.debug(f'{occs.barcode}')

        # if there is a nondigit, take it away from the digits, and modify it to contain only letters
        barcode_regex = [
            r'^(\w+)$', # digit and letters
            r'(\d+\-\d+\/\d+)$', # digit separated by - and /
            r'(\d+\:\d+\:\d+)$', # digit separated by :
            r'(\d+\-\d+)$', # digit separated by - 
            r'(\d+\/\d+)$', # digit separated by /
            r'(\d+\.\d+)$', # digit separated by .
            r'(\d+\s\d+)$', # digit separated by space
            r'(\d+)$', # digit
        ]
        # extract the numeric part of the barcode
        bc_extract = occs['barcode'].astype(str).str.extract('(' + '|'.join(barcode_regex) + ')')
        #occs['prel_bc'] = occs['prel_bc'].str.strip()

        logging.debug(f'Numeric part of barcodes extracted {bc_extract}')

        i=0
        while(len(bc_extract.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
            i = i+1
            bc_extract.iloc[:,-1] = bc_extract.iloc[:,-1].fillna(bc_extract.iloc[:,-2])
            bc_extract = bc_extract.drop(bc_extract.columns[-2], axis = 1)
            #print(names_WIP) # for debugging, makes a lot of output
            #print('So many columns:', len(names_WIP.columns), '\n')
        logging.debug(f'barcodes extracted: {bc_extract}')
        # reassign into dataframe
        occs['prel_bc'] = bc_extract

        logging.debug(f'Prelim. barcode: {occs.prel_bc}')
        # now get the herbarium code. First if it was correct to start with, extract from barcode.
        bc = pd.Series(occs['barcode'])

        prel_herbCode = occs['barcode'].str.extract(r'(^[A-Z]+\-[A-Z]+\-)') # gets most issues... ABC-DE-000000
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'(^[A-Z]+\s[A-Z]+)')) # ABC DE00000
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'(^[A-Z]+\-)')) # ABC-00000
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'(^[A-Z]+)')) #ABC000
        # If still no luck, take the capital letters in 'Institute'
        prel_herbCode = prel_herbCode.fillna(occs['herbarium_code'].str.extract(r'(^[A-Z]+)')) # desperately scrape whatever is in the 'herbarium_code' column if we still have no indication to where the barcode belongs
        prel_herbCode = prel_herbCode.fillna(occs['institute'].str.extract(r'(^[A-Z]+)')) # desperately scrape whatever is in the 'institute' column if we still have no indication to where the barcode belongs

        occs = occs.assign(prel_herbCode = prel_herbCode)
        occs['prel_code'] = occs['barcode'].astype(str).str.extract(r'(\D+)')
        occs['prel_code_X'] = occs['barcode'].astype(str).str.extract(r'(\d+\.\d)') # this is just one entry and really f@#$R%g annoying

        logging.debug(f'Prelim. barcode Type: {type(occs.prel_code)}')
        logging.debug(f'Institute would be: {occs.institute}')
        
        logging.debug(f'Prel. Herbarium code: {occs.prel_herbCode}')

        # if the barcode column was purely numericm integrate
        occs['tmp_hc'] = occs['institute'].str.extract(r'(^[A-Z]+\Z)')
        occs['tmp_hc'] = occs['barcode'].str.extract(r'(^[A-Z]+\-[A-Z]+\-)')
        #occs['hc_tmp_tmp'] = occs['herbarium_code'].str.extract(r'(^[A-Z]+\Z)')
        #occs['tmp_hc'].fillna(occs.hc_tmp_tmp, inplace = True)
        occs['tmp_hc'] = occs['tmp_hc'].replace({'PLANT': 'TAIF'})
        #occs['tmp_hc'] = occs['tmp_hc'].str.replace('PLANT', '')
        occs.prel_herbCode.fillna(occs['tmp_hc'], inplace = True)
        occs.prel_herbCode.fillna('', inplace = True)
        #occs[occs['herbarium_code'] == r'([A-Z]+)', 'tmp_test'] = 'True'
        
        logging.debug(f'TMP herb code: {occs.tmp_hc}')
        # this now works, but,
            # we have an issue with very few institutions messing up the order in which stuff is supposed to be...
            # (TAIF)

        #occs.institute.fillna(occs.prel_herbCode, inplace=True)
        # print('DEBUG:', pd.isna(occs.prel_bc.str.extract(r'([A-Z])')))
        occs['sel_col_bc'] = pd.isna(occs['prel_bc'].str.extract(r'([A-Z])'))
        occs.loc[occs['sel_col_bc'] == False, 'prel_herbCode'] = ''
        logging.debug(f'prel_code: {occs.prel_herbCode}')
        logging.debug(f'{occs.prel_bc}')
        occs.drop



        occs['st_barcode'] = occs['prel_herbCode'] + occs['prel_bc']
        occs['st_barcode'] = occs.st_barcode.astype(str)

        logging.debug(f'{occs.st_barcode}')
    #    prel_herbCode trumps all others,
    #        then comes herbarium_code, IF (!) it isn't a word (caps and non-caps) and if it is not (!!) 'PLANT'
    #           then comes institute


        logging.debug(f'Now these columns: {occs.columns}')

        if occs.st_barcode.isna().sum() > 0:
        
            logging.info('I couldn\'t standardise the barcodes of some records. This includes many records (if from GBIF) with barcode = NA')
            na_bc = occs[occs['st_barcode'].isna()]
            na_bc.to_csv(working_directory + prefix + 'NA_barcodes.csv', index = False, sep = ';', )
            logging.info(f'I have saved {len(na_bc)} occurences to the file {working_directory+prefix}NA_barcodes.csv for corrections')

            logging.info('I am continuing without these.')
            occs = occs[occs['st_barcode'].notna()]
            
            logging.debug(f'Occs: {occs}')

        # and clean up now
        occs = occs.drop(['prel_bc', 'prel_herbCode', 'prel_code', 'prel_code_X', 'tmp_hc', 'sel_col_bc'], axis = 1)
        occs = occs.rename(columns = {'barcode': 'orig_bc'})
        occs = occs.rename(columns = {'st_barcode': 'barcode'})
        
        logging.debug(f'{occs.columns}')
        # -----------------------------------------------------------------------
        # COLLECTION NUMBERS
        # keep the original colnum column
        # split to get a purely numeric colnum, plus a prefix for any preceding characters,
        # and sufix for trailing characters

        
        logging.debug(f'{occs.colnum_full}')

        #create prefix, extract text before the number
        occs['prefix'] = occs.colnum_full.str.extract('^([a-zA-Z]*)')
        ##this code deletes spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()

        #create sufix , extract text or pattern after the number
        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only number (colNam)

        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\-\d+)', # of structure 00-00
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_digits) + ')')
        # print(occs[['colnum_full', 'colnum']])

        occs['colnum'] = occs['colnum'].str.strip()

        occs['colnum'].replace('nan', pd.NA, inplace=True)

        #print(occs)
        # ok this looks good for the moment.
        """ Still open here: TODO
        remove collector name in prefix (partial match could take care of that)
        """


# end of Data source specific treatment

    #----------------------------------------------------------------------------------
    occs = occs.astype(dtype = z_dependencies.final_col_type) # check data type
    #print(occs.dtypes)
    logging.info('Data has been standardised to conform to the columns we need later on.') #\n It has been saved to: ', out_file)
    logging.info(f'Shape of the final file is: {occs.shape}' )
    logging.debug(f'{occs.columns}')


    occs = occs.astype(dtype = z_dependencies.final_col_type)
    logging.info(f'Shape of the final file is: {occs.shape}')

    #occs.to_csv(out_file, sep = ';')
    
    logging.info('Column standardisation completed successfully.')

    return occs
