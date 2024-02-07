#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for duplicate detection and tratment

2022-12-13 sjs

CHANGELOG:
    2022-12-13: created


CONTAINS:
    duplicate_stats():
        does some stats on duplicates found with different combinations
    duplicate_cleaner():
        actually goes in and removes, merges and cleans duplicates
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
import b0_country_coordinates as cc_functions




def duplicate_cleaner(occs, dupli, working_directory, prefix, expert_file, User, step='Raw', verbose=True, debugging=False):
    '''
    This one actually goes and cleans/merges duplicates.
        > occs = occurrence data to de-duplicate
        > dupli: the columns by which we identify duplicates
        > working_directory = path to directory to output intermediate files
        > prefix = filename prefix
        > User = username used to fill the 'modified' field
        > step = {raw, master} = reference for datatype checks
        > expert_file = if 'EXP' skips certain parts in within dataset cleaning and gives priority as below when integrating in master step.
            # IF expert_status = 'EXP', then dets and coordinates get priority over others
                               = 'NO', then dets and coordinates consolidated normally.
    '''

    if step=='Master':
        occs=occs.astype(z_dependencies.final_col_for_import_type)
    else:
        occs = occs.astype(z_dependencies.final_col_type) # double checking
    #print(occs.dtypes)
    #occs = occs.replace(np.nan, pd.NA)

    dup_cols = dupli #['recorded_by', 'colnum', 'sufix', 'col_year'] # the columns by which duplicates are identified

    #-------------------------------------------------------------------------------
   # Housekeeping. Clean up problematic types and values...

    # occs = occs.str.strip()
    occs1 = occs.replace(['', ' '], pd.NA)

    
    #-------------------------------------------------------------------------------
    # find duplicated BARCODES before we do anything
    # this step only takes place in master step.
    if step=='Master':
        logging.info('Deduplication: MASTER-1')
        # ONLY RETAIN DUPLICATED BARCODES 
        duplic_barcodes = occs1[occs1.duplicated(subset=['barcode'], keep=False)] # gets us all same barcodes
        logging.info(f'BARCODES DUPLICATED: {duplic_barcodes.shape}')

        # KEEP UNIQUE BARCODES
        cl_barcodes = occs1.drop_duplicates(subset=['barcode'], keep=False) # throws out all duplicated rows,
        logging.info(f'NO BARCODES DUPLICATED: {cl_barcodes.shape}')

        # ----------- only duplicated
        # all other duplicates follow below.
        if expert_file == 'EXP':
            # if expert we need to take care we integrate the expert data properly
            logging.info('Deduplication: MASTER-1, expert')
            logging.info('Merging duplicated barcodes, EXPERT file')
            duplic_barcodes = duplic_barcodes.sort_values(['barcode', 'expert_det'], ascending = [False, False])
            logging.info(f'{duplic_barcodes.barcode}{duplic_barcodes.recorded_by}{duplic_barcodes.colnum}{duplic_barcodes.expert_det}')

            barcode_merged = duplic_barcodes.groupby(['barcode'], as_index = False).agg(
                    scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                    genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                    specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                    species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                    collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                    recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                    colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
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
                    barcode = pd.NamedAgg(column = 'barcode', aggfunc='first'), # as we have premerged all barcodes above, it doesn't matter which one we take
                    orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                    coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                    huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                    geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                    source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                    wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                    expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                    status = pd.NamedAgg(column = 'status',  aggfunc = 'first'),
                    accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                    ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                    link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x) if pd.notna(x).all() else pd.NA),
                    ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first')
                    )
            logging.info(f'{barcode_merged.barcode}{barcode_merged.recorded_by}{barcode_merged.colnum}{barcode_merged.expert_det}')
            
            logging.info('********* Adding timestamp *******')
            date = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
            barcode_merged['modified'] = User + '_' + date
            logging.info(f'{barcode_merged.modified}')


            logging.info(f'deduplocated barcodes: {barcode_merged.shape} {barcode_merged.modified}')
            
                # merge in master columns to the rest.
            


        else:
            # if not EXP, just bung the data together....
            logging.info('Deduplication: MASTER-1 non-expert')
            logging.info('Merging duplicated barcodes')
            barcode_merged = duplic_barcodes.groupby(['barcode'], as_index = False).agg(
                scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc='first'),
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
                barcode = pd.NamedAgg(column = 'barcode', aggfunc='first'),
                orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x) if pd.notna(x).all() else pd.NA),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first')
                )
        # END of if/else Expert file
        # merge in master columns to the rest.
       



        ### and re merge deduplicated barcodes and unique barcodes
        occs1 = pd.concat([cl_barcodes, barcode_merged], axis=0) ###CHECK axis assignments
        logging.info(f'After deduplocated barcodes entire data: {occs1.shape}')
    # END of  if/else step=MASTER
        
    #-------------------------------------------------------------------------------
    #NOW start with actual deduplication

    occs_dup_col =  occs1.loc[occs1.duplicated(subset=dup_cols, keep=False)]
    #print('Run1', occs_dup_col)
    # get the NON-duplicated records
    occs_unique = occs1.drop_duplicates(subset=dup_cols, keep=False)

    logging.info(f'\n First filtering. \n Total records: {len(occs1)} ;\n records with no duplicates (occs_unique): {len(occs_unique)} \n records with duplicates (occs_dup_col): {len(occs_dup_col)}')

    if len(occs_dup_col) == 0:
        logging.info('Nothing to deduplicate anymore. Either barcodes took care of it, or there were no duplicates.')
         
        return occs_unique


    #-------------------------------------------------------------------------------
    # Duplicates part 1:
    #---------------------------------
    ## A  DIFFERENT COORDINATES
    # Coordinates not identical:
        # small difference --> MEAN
        # big difference --> exctract and either discard or check manually

    # double-check type of coordinate columns
    convert_dict = {'ddlat': float,
                    'ddlong': float}
    occs_dup_col = occs_dup_col.astype(convert_dict)

    logging.info(f'\n The duplicates subset, before cleaning dups has the shape: {occs_dup_col.shape}')
    # in this aggregation step we calculate the variance between the duplicates.
    
    if expert_file ==  'EXP':
        # as it is expert file, we expect coordinates to match within the file. We check coordinates when integrating into master:
        # > Do nothing, and then take expert value at final merge.
        logging.info('We have expert file!')

    else:
        # no expert file, so just do as normal....
        occs_dup_col = occs_dup_col.reset_index(drop = True)

   #     print('PROBLEM:\n', occs_dup_col[dup_cols])
        occs_dup_col[dup_cols] = occs_dup_col[dup_cols].fillna(-9999)
        # print('PROBLEM:\n', occs_dup_col[dup_cols], occs_dup_col.ddlat)

        # calculate variance between grouped objects
        occs_dup_col['ddlat_var'] = occs_dup_col.groupby(dup_cols)['ddlat'].transform('var')
        occs_dup_col['ddlong_var'] = occs_dup_col.groupby(dup_cols)['ddlong'].transform('var')
    
        # subset problematic large variance!
        occs_large_var = occs_dup_col[(occs_dup_col['ddlat_var'] > 0.1) | (occs_dup_col['ddlong_var'] > 0.1)]

        # and drop the large variance data here.
        occs_ok = occs_dup_col[(occs_dup_col['ddlat_var'] <= 0.1) & (occs_dup_col['ddlong_var'] <= 0.1)]

        occs_large_var['coordinate_country'] = occs_large_var.apply(lambda row: cc_functions.get_cc_new(row['ddlat'], row['ddlong']), axis = 1, result_type = 'reduce')
                #occs_large_var['cc_discrepancy'] = (occs_prob_coords['country_id'] != occs_prob_coords['coordinate_country'])
        
        if len(occs_large_var) > 0:
        # if we have records that have excessive variance 
            # read the old problematic values
            try:
                # if file exists then this shold work
                coord_prob_bl = pd.read_csv(working_directory + '0_'+'coordinate_discrepancy.csv', sep = ';')
                # append the new ones
                merged_out = pd.concat([coord_prob_bl, occs_large_var])
            except: 
                merged_out = occs_large_var

            # and write back to file...
            merged_out.to_csv(working_directory + '0_'+'coordinate_discrepancy.csv', index = False, sep = ';')


        
        occs_dup_col = occs_ok

        logging.info(f'\n Input records: {len(occs1)} \n records with no duplicates (occs_unique): {len(occs_unique)}')
        logging.info(f'duplicate records with very disparate coordinates removed: {len(occs_large_var)} If you want to check them, I saved them to {working_directory}TO_CHECK_{prefix}coordinate_issues.csv')

        # fo r smaller differences, take the mean value...
        occs_dup_col['ddlat'] = occs_dup_col.groupby(['col_year', 'colnum_full'])['ddlat'].transform('mean')
        occs_dup_col['ddlong'] = occs_dup_col.groupby(['col_year', 'colnum_full'])['ddlong'].transform('mean')
        logging.info(f'after coordinate consolidation:{occs_dup_col.shape}')


    #---------------------------------
    ## B DIFFERENT IDENTIFICATIONS
    # Different identification between duplicates
    # update by newest det, or remove spp./indet or string 'Annonaceae'

    # expert_list ??? it might be a nice idea to gather a list of somewhat recent identifiers
    # to  use them to force through dets? Does that make sense? We do update the
    # taxonomy at a later date, so dets to earlier concepts might not be a massive issue?

    if step =='Master':
        # if Master, we merge the accepted name field.
        dups_diff_species = occs_dup_col[occs_dup_col.duplicated(['col_year','colnum_full', 'country'],keep=False)&~occs_dup_col.duplicated(['recorded_by','colnum_full','accepted_name'],keep=False)]
        # print('DUPLICATED SPECIES?\n', dups_diff_species[['recorded_by', 'barcode', 'accepted_name', 'det_year']])
        dups_diff_species = dups_diff_species.sort_values(['col_year','colnum_full'], ascending = (True, True))

        logging.info(f'\n We have {len(dups_diff_species)} duplicate specimens with diverging identification.')

        # # backup the old dets
        # occs_dup_col['genus_old'] = occs_dup_col['genus']
        # occs_dup_col['specific_epithet_old'] = occs_dup_col['specific_epithet']
        
        # needs this to be able to group by and work below
        occs_dup_col[dup_cols] = occs_dup_col[dup_cols].fillna(0)
        #occs_dup_col = occs_dup_col.reset_index(drop = True)
        
        #groupby and sort more recent det 
        occs_dup_col = occs_dup_col.groupby(dup_cols, group_keys=False, sort=True).apply(lambda x: x.sort_values(['det_year'], ascending=False))
        occs_dup_col.reset_index(drop=True, inplace=True)

        occs_dup_col['accepted_name'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['accepted_name'].transform('first')
        occs_dup_col['genus'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['genus'].transform('first')
        occs_dup_col['specific_epithet'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['specific_epithet'].transform('first')
        # and values 0 back to NA
        occs_dup_col[dup_cols] = occs_dup_col[dup_cols].replace(0, pd.NA)

        
        ###### HERE no data left in sn step... 
        #print('HERE', occs_dup_col)

        



    else:
        logging.info(f'test1 {occs_dup_col.shape}')
        logging.info(f'occs_dup_col.specific_epithet')
        occs_dup_col['specific_epithet'] = occs_dup_col['specific_epithet'].str.replace('sp.', '')
        occs_dup_col['specific_epithet'] = occs_dup_col['specific_epithet'].str.replace('indet.', '') 
        logging.info(f'test2 {occs_dup_col.shape}')

        dups_diff_species = occs_dup_col[occs_dup_col.duplicated(['col_year','colnum_full', 'country'],keep=False)&~occs_dup_col.duplicated(['recorded_by','colnum_full','specific_epithet','genus'],keep=False)]
        dups_diff_species = dups_diff_species.sort_values(['col_year','colnum_full'], ascending = (True, True))

        logging.info(f'\n We have {len(dups_diff_species)} duplicate specimens with diverging identification.')

        # backup the old dets
        occs_dup_col['genus_old'] = occs_dup_col['genus']
        occs_dup_col['specific_epithet_old'] = occs_dup_col['specific_epithet']
        logging.info(f'test3 {occs_dup_col.shape}')
        
        # replace zeroes with NA, better for downstream sorting
        occs_dup_col['det_year'].replace(0, pd.NA, inplace=True)
                # print(occs_dup_col.det_year)
                # print(occs_dup_col[['recorded_by', 'colnum']])

        # Issues if NA values here in following groupby step
        occs_dup_col.sufix = occs_dup_col.sufix.fillna(-9999) 
       
       # groupby the deduplication columns,  sort by increasing (low to high) det_year
        occs_dup_col = occs_dup_col.groupby(dup_cols, group_keys=False, sort=True).apply(lambda x: x.sort_values('det_year', ascending=False))
        # double checking
        logging.info(f'There used to be errors here,  {occs_dup_col.shape}')

        # return the -9999 value to NA
        occs_dup_col['sufix'].replace(-9999, 0, inplace =True)
        occs_dup_col['sufix'].replace(0, np.nan, inplace =True)
        # for some unknown reason, values of NaN are isna()=True, but does not do fillna(pd.NA) (i.e. it remains at NaN...)
        occs_dup_col.sufix.fillna(pd.NA, inplace=True)
        #        occs_dup_col.sufix = occs_dup_col.sufix.replace({'NaN', pd.NA}, regex=False) # not working, weird error.
        # print(occs_dup_col.sufix.isna())
        # print(occs_dup_col.sufix)

        occs_dup_col.reset_index(drop=True, inplace=True)
        occs_dup_col['genus'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['genus'].transform('first')
        occs_dup_col['specific_epithet'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['specific_epithet'].transform('first')
        logging.info(f'test5 {occs_dup_col.shape}')

        #save a csv with all duplicates beside each other but otherwise cleaned, allegedly.
        
        occs_dup_col.to_csv(working_directory + 'TO_CHECK_' + prefix + 'dupli_dets_cln.csv', index = False, sep = ';')
        logging.debug(f'\n I have saved a checkpoint file of all cleaned and processed duplicates, nicely beside each other, to: {working_directory}TO_CHECK_{prefix}dupli_dets_cln.csv')

    #-------------------------------------------------------------------------------
    # DE-DUPLICATE AND THEN MERGE
    # check type (again)
    occs_dup_col = occs_dup_col.astype(z_dependencies.final_col_type)
    #print(occs_dup_col.dtypes)

    # any empty strings need to be set NA, otherwise sorting gets messed up ('' is at beginning)
    occs_dup_col = occs_dup_col.replace(['', ' '], pd.NA)

    # s.n. needed for deduplication and error reduction. removed later...
    occs_dup_col.colnum_full = occs_dup_col.colnum_full.fillna('s.n.')
    ############################################ Here integrate expert flag
    if step == 'Master':
        logging.info('Deduplication: MASTER-2')
        # Expert level deduplication only between datasets, not in single dataset!
        if expert_file == 'EXP':
            
            logging.info('Deduplication: MASTER-2 expert')
        # We have an expert dataset being integrated into the database.

            # first handle duplicates with all expert values (expert duplicates)
            expert_rows = occs_dup_col[occs_dup_col['expert_det'] == 'expert_det_file']
            expert_rows = expert_rows.sort_values(['status', 'expert_det'], ascending = [True, True])
            
            expert_rows.modified = expert_rows.modified.fillna('')
            expert_rows.geo_issues = expert_rows.geo_issues.fillna('')
            
            expert_merged = expert_rows.groupby(dup_cols, as_index = False).agg(
                scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
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
                barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                #status = pd.NamedAgg(column = 'status', aggfunc=lambda x: 'ACCEPTED'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x) if pd.notna(x).all() else pd.NA),
                modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled with new value at the end of deduplication
                )
            # here quite some data might get lost, so we need to check where we want to just join first,
            # and where we add all values, and then decide on the columns we really want in the final
            # database!!!

            # de-duplicated duplicates sorting to get them ready for merging
            length = len(dup_cols)-2
            order_vec = tuple([True, True] + [False]*length)
            expert_merged = expert_merged.sort_values(dup_cols, ascending = order_vec)

            logging.info('EXPERT-EXPERT deduplicated')     
            logging.info(f'\n There were {len(expert_rows)} duplicated specimens')
            logging.info(f'\n There are {len(expert_merged)} unique records after merging.')


            # then normally handle duplicates with no expert values at all
            non_exp_rows = occs_dup_col[occs_dup_col['expert_det'] != 'expert_det_file' ]
            non_exp_rows = non_exp_rows.sort_values(['status', 'det_year'], ascending = [True, False])

            non_exp_rows.modified = non_exp_rows.modified.fillna('')
            non_exp_rows.geo_issues = non_exp_rows.geo_issues.fillna('NONE')

            non_exp_merged = non_exp_rows.groupby(dup_cols, as_index = False).agg(
                scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
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
                barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x) if pd.notna(x).all() else pd.NA),
                geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
            )

            # here quite some data might get lost, so we need to check where we want to just join first,
            # and where we add all values, and then decide on the columns we really want in the final
            # database!!!

            # de-duplicated duplicates sorting to get them ready for merging
            length = len(dup_cols)-2
            order_vec = tuple([True, True] + [False]*length)
            non_exp_merged = non_exp_merged.sort_values(dup_cols, ascending = order_vec)

            logging.info('EXPERT-EXPERT deduplicated')         
            logging.info(f'\n There were {len(non_exp_rows)} duplicated specimens')
            logging.info(f'\n There are {len(non_exp_merged)} unique records after merging.')

            # SANITY CHECK
            if (len(expert_rows) + len(non_exp_rows)) != len(occs_dup_col):
                logging.warning('SOMETHINGS WRONG HERE!!')

            # now merge expert-only and non-expert-only parts together and deduplicate

            # once deduplicated dataframe
            one_dd_done = pd.concat([expert_merged, non_exp_merged])

            one_dd_done = one_dd_done.sort_values(['status', 'expert_det'], ascending = [True, True])

            # now merge duplicates with expert & non-expert  values (only one of each left if everything above went to plan..)
            occs_merged = one_dd_done.groupby(dup_cols, as_index = False).agg(
                scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc = 'first' ),
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
                barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x) if pd.notna(x).all() else pd.NA),
                modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
                )
            # here quite some data might get lost, so we need to check where we want to just join first,
            # and where we add all values, and then decide on the columns we really want in the final
            # database!!!

            # de-duplicated duplicates sorting to get them ready for merging
            length = len(dup_cols)-2
            order_vec = tuple([True, True] + [False]*length)
            occs_merged = occs_merged.sort_values(dup_cols, ascending = order_vec)

            logging.info(f'\n There were {len(occs_dup_col)} duplicated specimens')
            logging.info(f'\n There are {len(occs_merged)} unique records after merging.')

            ########################## end of expert non expert mess

        else:
            logging.info('Deduplication: MASTER-2 no expert')
                #print(occs_dup_col.status)
            
            occs_dup_col = occs_dup_col.sort_values(['status', 'expert_det'], ascending = [True, True])
            logging.info(f'{occs_dup_col.accepted_name}')
            # master but not expert. proceed as normal.
            print(occs_dup_col.modified.isna())
            occs_dup_col.modified = occs_dup_col.modified.fillna('nan')
            occs_dup_col.geo_issues = occs_dup_col.geo_issues.fillna('NONE')
            occs_merged = occs_dup_col.groupby(dup_cols, as_index = False).agg(
                    scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                    genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                    specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                    species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                    collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                    recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                    colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
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
                    barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                    orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                    coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                    huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                    geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                    wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                    expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                    status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                    accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                    link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x) if pd.notna(x).all() else pd.NA),
                    ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                    ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                    geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                    source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
                    modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
                    )
                # here quite some data might get lost, so we need to check where we want to just join first,
                # and where we add all values, and then decide on the columns we really want in the final
                # database!!!
                # de-duplicated duplicates sorting to get them ready for merging
            length = len(dup_cols)-2
            order_vec = tuple([True, True] + [False]*length)
            occs_merged = occs_merged.sort_values(dup_cols, ascending = order_vec)
            logging.info(f'\n There were {len(occs_dup_col)} duplicated specimens')
            logging.info(f'\n There are {len(occs_merged)} unique records after merging.')
            logging.info(f'The  deduplicated one is: {occs_merged}')

        ########### END of expert/non expert if/else
        # end of if master

    else:
        logging.info('Deduplication: RAW-1')
        occs_dup_col['colnum_full'] = occs_dup_col['colnum_full'].fillna('')
        occs_dup_col['orig_bc'] = occs_dup_col['orig_bc'].fillna('')

        # not master
        occs_dup_col = occs_dup_col.sort_values(['expert_det'], ascending = True)
        logging.info(occs_dup_col[['recorded_by', 'colnum', 'ddlat']])
        occs_merged = occs_dup_col.groupby(dup_cols, as_index = False).agg(
            scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
            genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
            specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
            species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
            collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
            recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
            colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
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
            barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
            orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
            coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
            link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x) if pd.notna(x).all() else pd.NA),
            huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
            geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
            source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x) if pd.notna(x).all() else pd.NA),
            wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first')
            )
        # here quite some data might get lost, so we need to check where we want to just join first,
        # and where we add all values, and then decide on the columns we really want in the final
        # database!!!

    #  END of if/else MASTER
    # REMOVE the s.n. used for deduplicatin s.n. values...    
    try:
        occs_dup_col.colnum_full = occs_dup_col.colnum_full.replace('s.n.', pd.NA)
        logging.info('1 s.n. found')
    except:
        logging.info('1 s.n. NOT found')
    try:
        occs_dup_col.colnum_full = occs_dup_col.colnum_full.replace('s.n., s.n.', pd.NA)
        logging.info('2 s.n. found')
    except:
        logging.info('2 s.n. NOT found')
    try:
        occs_dup_col.colnum_full = occs_dup_col.colnum_full.replace('s.n., s.n., s.n.', pd.NA)
        logging.info('3 s.n. found')
    except:
        logging.info('3 s.n. NOT found')
    try:
        occs_dup_col.colnum_full = occs_dup_col.colnum_full.replace('s.n., s.n., s.n., s.n.', pd.NA)
        logging.info('4 s.n. found')
    except:
        logging.info('4 s.n. NOT found')

    # de-duplicated duplicates sorting to get them ready for merging
    #print(len(dupli))
    length = len(dupli)-2
    order_vec = tuple([True, True] + [False]*length)
    #print(order_vec, len(order_vec))

    occs_merged = occs_merged.sort_values(dup_cols, ascending = order_vec)

    logging.info(f'\n There were {len(occs_dup_col)} duplicated specimens')
    logging.info(f'\n There are {len(occs_merged)} unique records after merging.')
    logging.info(f'The deduplicated one is: {occs_merged}')


    logging.info('\n \n FINAL DUPLICATE STATS:')
    logging.info('-------------------------------------------------------------------------')
    logging.info(f'\n Input data: {len(occs)} ; \n De-duplicated duplicates: {len(occs_merged)}')
    logging.info(f'Non-duplicate data:{len(occs_unique)}')
     #'; \n No collection number. (This is not included in output for now!) :', len(occs_nocolNum),
    logging.info(f';\n total data written: {len(occs_merged) + len(occs_unique)} \n Datapoints removed: {len(occs) - (len(occs_merged) + len(occs_unique))}')
    

    if step=='Master':
        logging.info('********* Adding timestamp *******')
        date = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        occs_merged['modified'] = User + '_' + date
        logging.info(f'{occs_merged.modified}')
        occs_unique['modified'] = User + '_' + date
    #    occs_cleaned = pd.merge(pd.merge(occs_merged,occs_unique,how='outer'))#,occs_nocolNum ,how='outer')
    occs_cleaned = pd.concat([occs_merged, occs_unique])
    occs_cleaned = occs_cleaned.sort_values(dup_cols, ascending = order_vec)
    #print(occs_cleaned.head(50))

    if debugging:
        occs_cleaned.to_csv(working_directory+prefix+'deduplicated.csv', index = False, sep = ';', )
        logging.debug(f'\n The output was saved to {working_directory+prefix}deduplicated.csv', '\n')

    logging.info(f'At the end of deduplication, we have {len(occs_cleaned)} records.')

    return occs_cleaned

