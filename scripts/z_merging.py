#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for basic format changes, subsetting, ...

2022-12-19 sjs

CHANGELOG:
    2022-12-19: created


CONTAINS:
    duplicated_barcodes():
        consolidates barcodes for later detecting as duplicates.

    check_premerge():
        checks the new data with the master-DB for potential duplicates and ?

'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import logging
from tqdm import tqdm 
import gc

#custom dependencies
import z_dependencies
import z_functions_b as dupli




def deduplicated_barcodes_v2(master_db, new_occs):
    """
    Try and speed up the barcode step massively, by exploding the barcode field into
    a dataframe that can be deduplicated by the barcodes using the built in duplicated 
    functions of pandas
    """


    # print('MASTER\n', master_db.barcode)
    logging.info(f'new occs are this size {len(new_occs)}')
    logging.info(f'MASTER are this size {len(master_db)}')
    # first some housekeeping: remove duplicated barcodes in input i.e. [barcode1, barcode2, barcode1] becomes [barcode1, barcode2]

    new_occs.barcode = new_occs.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell
    # we can retain Na values as these are not dropped
    new_occs = new_occs.reset_index(drop=True)

    # explode barcodes so they are separate and can be individually checked
    new_occs['barcode_split'] = new_occs['barcode'].str.split(', ')

    exploded_new_occs = new_occs.explode('barcode_split')
    exploded_new_occs = exploded_new_occs.reset_index(drop = True)
    print(exploded_new_occs[['barcode_split', 'barcode']])
    # print(exploded_new_occs.barcode)
    print(exploded_new_occs.shape)
    print(new_occs.shape)

    # do the same with masterdb
    master_db.loc[master_db['barcode'].isna(), 'barcode'] = 'no_Barcode'

    master_db.barcode = master_db.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell
    # we can retain Na values as these are not dropped
    master_db = master_db.reset_index(drop=True)

    # explode barcodes so they are separate and can be individually checked
    master_db['barcode_split'] = master_db['barcode'].str.split(', ')

    exploded_master_db = master_db.explode('barcode_split')
    exploded_master_db = exploded_master_db.reset_index(drop = True)
    exploded_master_db.barcode_split = exploded_master_db.barcode_split.replace('no_Barcode', pd.NA)
    # print(exploded_master_db[['barcode_split', 'barcode']])
    # # print(exploded_master_db.barcode)
    # print(exploded_master_db.shape)
    # print(master_db.shape)

    tmp_master = pd.concat([exploded_new_occs, exploded_master_db], axis=0)

    print(tmp_master.columns)
    print('Total records of exploded barcodes:', len(tmp_master), 
          '\nUsing the same criteria as in recordcleaner step we have so many duplicated records:',
                        '\n By individual BARCODE', tmp_master.duplicated([ 'barcode_split' ], keep='first').sum(),
                        '\n Duplicated specimens (based on previous deduplication, not barcode):',  tmp_master.duplicated([ 'barcode' ], keep='first').sum(),
                        '\n ................................................. *1 \n ')
   
    # print(tmp_master[tmp_master.barcode_split == 'no_Barcode'].shape)
    # print(tmp_master[tmp_master.barcode_split.isna()].shape)
    # print(tmp_master.shape)

    print('Our garbage level is at:',gc.get_count())

    tmp_master = tmp_master.astype(z_dependencies.final_col_type)

    sorted_tmaster = tmp_master.sort_values(['expert_det', 'status', 'det_year'], ascending = [True, True, False])

    sorted_tmaster.orig_bc = sorted_tmaster.orig_bc.fillna('')
    sorted_tmaster.orig_recby = sorted_tmaster.orig_recby.fillna('')
    sorted_tmaster.modified = sorted_tmaster.modified.fillna('')
    sorted_tmaster.geo_issues = sorted_tmaster.geo_issues.fillna('NONE')

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
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                orig_recby = pd.NamedAgg(column = 'orig_recby', aggfunc=lambda x: ', '.join(x)),
                geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x)),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
            )
# # # go through and deduplicate by full barcodes:

    print('Total records after reducind duplicated INDIVIDUAL barcodes:', len(master_bc_agg),
           '\nUsing the same criteria as in recordcleaner step we have so many unique records:',
                        '\n By individual BARCODE (!this includes NA barcodes!)', master_bc_agg.duplicated([ 'barcode_split' ], keep='first').sum(),
                        '\n Duplicated specimens (based on previous deduplication, not barcode):',  master_bc_agg.duplicated([ 'barcode' ], keep='first').sum(),
                        '\n ................................................. *2 \n ')
    print('Our garbage level is at:',gc.get_count())

    master_bc_agg = master_bc_agg.sort_values(['expert_det', 'status', 'det_year'], ascending = [True, True, False])

    master_bc_agg.orig_bc = master_bc_agg.orig_bc.fillna('')
    master_bc_agg.orig_recby = master_bc_agg.orig_recby.fillna('')
    master_bc_agg.modified = master_bc_agg.modified.fillna('')
    master_bc_agg.geo_issues = master_bc_agg.geo_issues.fillna('NONE')

    master = master_bc_agg.groupby(['barcode'], as_index = False).agg(
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
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x)),
                orig_recby = pd.NamedAgg(column = 'orig_recby', aggfunc=lambda x: ', '.join(x)),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
            )


    master = master.sort_values(['recorded_by', 'colnum'], ascending = [True, True])

    print('After reducing all duplicated records by Barcode:', len(master), 
          '\nUsing the same criteria as in recordcleaner step we have so many unique records:',
                        '\n Duplicated specimens (based on previous deduplication, not barcode):',  master.duplicated([ 'barcode' ], keep='first').sum(),
                        '\n ................................................. *3\n ')
   
    print('Our garbage level is at:',gc.get_count())
    return master




# master_db = pd.read_csv('/Users/Serafin/Sync/1_Annonaceae/o_share_DB_WIP/4_DB_tmp/master_db.csv', sep =';')
# new_occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/o_share_DB_WIP/2_data_out/exp_debug_spatialvalid.csv' , sep = ';')
# print('stats: new // master')
# print(new_occs.shape)
# print(master_db.shape)

# test = deduplicated_barcodes_v2(master_db=master_db, new_occs=new_occs, working_directory='/Users/serafin/Desktop/',
#                                 prefix='tmp_', expert='NO', username='testest_')

# test.to_csv('/Users/serafin/Desktop/tmp_barcodesplit.csv', sep =';', index=False)




def check_premerge_v2(mdb, occs, verbose=True, debugging=False):
    '''function to compare the master database to the records to be included, fresh from Cleaning
    I check for duplicates.

    '''

    # drop any rows with no barcode!
    occs.dropna(subset= ['barcode'], inplace = True)


    occs.reset_index(drop = True, inplace = True)
    mdb.reset_index(drop = True, inplace = True)



    logging.info(f'NEW: {occs.barcode}')
    logging.info(f'MASTER: {mdb.barcode}')
    # modify the barcodes to standardise...
    logging.info(f'SIZE BEFORE BARCODE STEPS {len(occs)}')
    #occs = deduplicated_barcodes_v2(master_db = mdb, new_occs = occs, verbose=False, debugging=False)
    tmp_mast = deduplicated_barcodes_v2(master_db=mdb, new_occs = occs)



    logging.info(f'SIZE AFTER BARCODE STEPS {len(occs)}')

    #print('Columns!', tmp_mast.columns)
    if debugging:
        print('\n \n Some stats about potential duplicates being integrated: \n .................................................\n')
        print('\n By surname, number, sufix and col_year & country ID', 
        tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colnum', 'sufix', 'col_year', 'country_id' ], keep=False)].shape)
        print('\n By surname & full collectionnumber', 
        tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colnum_full' ], keep=False)].shape)
        print('\n By surname, number, genus & specific epithet', 
        tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colnum', 'genus', 'specific_epithet' ], keep=False)].shape)

        print('\n By barcode', tmp_mast[tmp_mast.duplicated(subset=['barcode'], keep=False)].shape)

        print('\n .................................................\n')
    if len(tmp_mast[tmp_mast.duplicated(subset=['barcode'], keep=False)]) != 0:
        print(tmp_mast[tmp_mast.duplicated(subset=['barcode'], keep=False)]['barcode'])
    if debugging:
        print(len(mdb), 'the master_db download')
        print(len(occs), 'cleaned occurences')

        print(len(tmp_mast), 'combined')
    tmp_mast = tmp_mast[z_dependencies.final_cols_for_import]


    return tmp_mast



########################## deprecated ##





# def duplicated_barcodes(master_db, new_occs, verbose=True, debugging=False):
#     """
#     FInd all duplicated barcodes from new occurrences in the master database
#     > master_db: Master database for crossreference
#     > new_occs:  The new records to check

#     duplicated_barcodes() goes through and finds any matches of barcodes in 'new_occs' with 'master_db'. Any matches value is copied from the master into the new data. Like this, in subsequent steps duplicates can easily
#     be identified. 
#     """


#     print(new_occs.barcode)
#     print('MASTER\n', master_db.barcode)
#     logging.info(f'new occs are this size {len(new_occs)}')
#     logging.info(f'MASTER are this size {len(master_db)}')
#     # first some housekeeping: remove duplicated barcodes in input i.e. [barcode1, barcode2, barcode1] becomes [barcode1, barcode2]
#     new_occs = new_occs[~new_occs.barcode.isna()]
#     new_occs.barcode = new_occs.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell
#     print('HERE1:\n',master_db[master_db.barcode.isna()])
#     #master_db.barcode = master_db.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell


#     # split new record barcode fields (just to check if there are multiple barcodes there)
#     bc_dupli_split = new_occs['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
#     bc_dupli_split.columns = [f'bc_{i}' for i in range(bc_dupli_split.shape[1])] # give the columns names..
#     bc_dupli_split = bc_dupli_split.apply(lambda x: x.str.strip())
#      # some information if there are issues
#     logging.debug(f'NEW OCCS:\n {bc_dupli_split}')
#     logging.debug(f'NEW OCCS:\n {type(bc_dupli_split)}')
#     master_bc_split = master_db['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
#     master_bc_split.columns = [f'bc_{i}' for i in range(master_bc_split.shape[1])]
#     master_bc_split = master_bc_split.apply(lambda x: x.str.strip())  #important to strip all leading/trailing white spaces!
    
#     logging.debug(f'master OCCS:\n {master_bc_split}')
#     # logging.debug(f'master OCCS:\n {master_bc_split.dtypes}')

#      # some information if there are issues
#     # logging.debug(f'Shape of new_occs {len(new_occs)}')

#     # then iterate through all barcodes of the new occurrences
#     # for every row
#     total_iterations = len(new_occs)
#     print('Crosschecking barcodes.')
#     for i in tqdm(range(total_iterations), desc = 'Processing', unit= 'iteration'):
#         # the tqdm should in theory have a progress bar...
        
    
#     #for i in range(len(new_occs)):
#         # print(bc_dupli_split.loc[i])
#         # print(i, len(new_occs))
#         barcode = list(bc_dupli_split.loc[i].astype(str))
#         # logging.info(f'BARCODE1: {barcode}')
#         # if multiple barcodes in the barcode field, iterate across them
#         for x in  range(len(barcode)):
#             bar = barcode[x]
            
#             # logging.info(f'working on row {i}')
#             # logging.info(f'BC to test:{bc_dupli_split.iloc[i]}') # TODO

#             if bar == 'None':
#             # this happens a lot. skip if this is the case.
#                 a = 'skip'
#                 #logging.info('Values <None> are skipped.')
#             else:
#                 # -> keep working with the barcode
                
#                 # logging.info(f'Working on barcode:\n {bar}')
            

#                 selection_frame = pd.DataFrame()  # df to hold resulting True/False mask  
#                 # now iterate over columns to find any matches
#                 for col in master_bc_split.columns:
#                     # iterate through rows. the 'in' function doesn't work otherwise
                    
#                     #logging.info('checking master columns')
#                     f1 = master_bc_split[col] == bar # get true/false column
#                     selection_frame = pd.concat([selection_frame, f1], axis=1) # and merge with previos columns
#                 # end of loop over columns
            
#                 # when selection frame finished, get out the rows we need including master value
#                 sel_sum = selection_frame.sum(axis = 1)
#                 sel_sum = sel_sum >= 1 # any value >1 is a True => match 
                
#                 # logging.info(f'this should be our final selection object length: {sel_sum.sum()}')
#                 # logging.info(f'Selection: {sel_sum}')
#                 if sel_sum.sum() == 0:
                    
#                     # logging.info('NO MATCHES FOUND!')
#                     out_barcode = pd.DataFrame([bar])
                    
#                     # in this case we do not modify anything!
                    
#                 else:
#                     out_barcode = pd.Series(master_db.barcode[sel_sum]).astype(str)
#                     out_barcode.reset_index(drop = True, inplace = True)
                   
      
#         # replace i-th element of the new barcodes with the matched complete range of barcodes from master
#                     # logging.info(f'i is: {i}')
#                     #logging.info(f'Input: {new_occs.at[i, 'barcode']}')
#                     # logging.info(f'Master: {out_barcode[0]}') # these are the barcodes retreived from the master file
#                     input = str(new_occs.at[i, 'barcode'])
#                     master = str(out_barcode[0])
#                     new = input + ', ' + master
#                     # logging.debug('New value pre processing: {new}')

#                     #new_occs.at[i, 'barcode'] = (new_occs.at[i, 'barcode'] + ', ' + out_barcode).astype(str) # replace original value with new value
#                     new = ', '.join(set(new.split(', ')))
#                     #logging.info('New value after processing {new}')
#                     new_occs.at[i, 'barcode'] = new

#                     master_db.loc[sel_sum, 'barcode'] = new
#                     # print(master_db.loc[sel_sum, 'barcode'])
#                     # print('the replaced value:',  new_occs.at[i, 'barcode'])
#                     # print('the replaced value:',  type(new_occs.at[i, 'barcode']))

#                 # <- end of bar==None condition
#     logging.info(f'Final barcode in new data: {new_occs.barcode}')
#     new_occs.barcode = new_occs.barcode.apply(lambda x: ', '.join(set(sorted(x.split(', ')))))    # this combines all duplicated barcodes within a cell
#     logging.info(f'Final barcode in master data: {master_db.barcode}')
#     logging.info(f'{new_occs.barcode} FINISHED')

 
#         # done.
#     return new_occs


# def check_premerge(mdb, occs, verbose=True, debugging=False):
#     '''function to compare the master database to the records to be included, fresh from Cleaning
#     I check for duplicates.

#     '''

#     # drop any rows with no barcode!
#     occs.dropna(subset= ['barcode'], inplace = True)


#     occs.reset_index(drop = True, inplace = True)
#     mdb.reset_index(drop = True, inplace = True)



#     logging.info(f'NEW: {occs.barcode}')
#     logging.info(f'MASTER: {mdb.barcode}')
#     # modify the barcodes to standardise...
#     logging.info(f'SIZE BEFORE BARCODE STEPS {len(occs)}')
#     occs = duplicated_barcodes(master_db = mdb, new_occs = occs, verbose=False, debugging=False)
    
#     logging.info(f'SIZE AFTER BARCODE STEPS {len(occs)}')

#     # 
#     tmp_mast = pd.concat([mdb, occs])
#     #print('Columns!', tmp_mast.columns)
#     if debugging:
#         print('\n \n Some stats about potential duplicates being integrated: \n .................................................\n')
#         print('\n By surname, number, sufix and col_year & country ID', 
#         tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colnum', 'sufix', 'col_year', 'country_id' ], keep=False)].shape)
#         print('\n By surname & full collectionnumber', 
#         tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colnum_full' ], keep=False)].shape)
#         print('\n By surname, number, genus & specific epithet', 
#         tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colnum', 'genus', 'specific_epithet' ], keep=False)].shape)

#         print('\n By barcode', tmp_mast[tmp_mast.duplicated(subset=['barcode'], keep=False)].shape)

#         print('\n .................................................\n')
#     if len(tmp_mast[tmp_mast.duplicated(subset=['barcode'], keep=False)]) != 0:
#         print(tmp_mast[tmp_mast.duplicated(subset=['barcode'], keep=False)]['barcode'])
#     if debugging:
#         print(len(mdb), 'the master_db download')
#         print(len(occs), 'cleaned occurences')

#         print(len(tmp_mast), 'combined')
#     tmp_mast = tmp_mast[z_dependencies.final_cols_for_import]


#     return tmp_mast



