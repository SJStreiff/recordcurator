#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Final_clean: 
    fill/crossfill NA values for otherwise duplicates
    clean-correct barcode problems
    check - correct -extract s.n.-indet combinations -> these are not well deduplicated or anything
'''

import warnings

import pandas as pd
import numpy as np
# import codecs
# import os
import regex as re
# import requests
# import swifter
# import logging

import z_dependencies

################
# def final_clean(occs, verbose=True, debug=False):
#     """
#     occs: dataframe to check 
#     """

#     # first format barcodes one last time

#         # find all barcodes with "[A-Z]0"
################
# occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/GLOBAL_final/2_temp_int_data/PaleoTropics_all_CHECKED.csv', sep = ';')
occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/GLOBAL_final/2_temp_int_data/NeoTropics_all_CHECKED_bc_NEW_check.csv', sep = ';')
# occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GDB_2/X_GLOBAL/master_db.csv', sep = ';')
# occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GLOBAL_Distr/1_inter_steps/0_coordinate_discrepancy.csv', sep = ';')
print(occs)

#occs = occs.head(100)

occs.loc[occs['barcode'].isna(), 'barcode'] = 'no_Barcode'
print(occs)
barcodes = occs.barcode
print(barcodes.head())

# now create df of all barcodes that used to be in one cell
barcodes_indiv = barcodes.str.split(', ', expand = True)

# same for herbarium codes and Institutes
herb_indiv = occs['herbarium_code'].str.split(', ', expand = True)
print(herb_indiv)
inst_indiv = occs['institute'].str.split(', ', expand = True)

word_regex = r'^(?:(?![A-Z][a-z]+.+$))'
bad_df = pd.DataFrame()
# the go through herb and institutes and replace Words with NA and Only capital letter codes
for col in herb_indiv.columns:
     bad = herb_indiv[col].str.contains(word_regex, regex= True)
     bad = bad.fillna(True)
    # change the values directly here in df[col]=''
     herb_indiv.loc[bad, col] = '' 
    #  bad_df[f'{col}_bad'] = bad

for col in inst_indiv.columns:
     bad = inst_indiv[col].str.contains(word_regex, regex= True)
     bad = bad.fillna(True)
    # change the values directly here in df[col]=''
     inst_indiv.loc[bad, col] = '' 
    #  bad_df[f'{col}_bad'] = bad

herb_indiv = herb_indiv.apply(lambda row: ', '.join(map(str, row)), axis=1)
herb_indiv = herb_indiv.str.strip()
herb_indiv = herb_indiv.str.strip(', ')

inst_indiv = inst_indiv.apply(lambda row: ', '.join(map(str, row)), axis=1)
inst_indiv = inst_indiv.str.strip()
inst_indiv = inst_indiv.str.strip(', ')


# Custom function to check conditions and replace values
def check_and_replace(value, row):
    if isinstance(value, str):
        digits_match = re.search(r'^\d+$', value)
        if digits_match:
            digits = digits_match.group()
            for other_column in row.index:
                if isinstance(row[other_column], str):
                    other_digits_match = re.search(r'^\d+$', row[other_column])
                    if other_digits_match and other_digits_match.group() == digits:
                        if row[other_column].endswith(digits):
                            return None
    return value


# Apply the custom function to each row
barcodes_indiv = barcodes_indiv.apply(lambda row: row.apply(lambda x: check_and_replace(x, row)), axis=1)


print(barcodes_indiv)

barcodes_better = barcodes_indiv.replace({'NaN': ''}, regex=False)
barcodes_better = barcodes_better.replace({'None': ''}, regex=False)

# replace BC0 with no_Barcode
barcodes_better = barcodes_better.replace({'^[A-Z]+0$': 'no_Barcode'}, regex=True)
barcodes_better = barcodes_better.apply(lambda row: ', '.join(str(value) for value in row if value is not None), axis=1)
barcodes_better = barcodes_better.str.strip()
barcodes_better = barcodes_better.str.strip(', ')

print(barcodes_better)
barcodes_indiv_2 = barcodes_better.str.split(', ', expand = True)
barcodes_indiv_2 = barcodes_indiv_2.replace({'': None}, regex=False)
def has_only_digits(row):
    return any(str(value)[0].isdigit() for value in row if value is not None)
# Add a new column with the flag
barcodes_better = barcodes_better.replace({' ': 'no_barcode'}, regex=False)

# barcodes_indiv_2.fillna('None', inplace=True)
barcodes_indiv_2['HasOnlyDigits'] = barcodes_indiv_2.apply(has_only_digits, axis=1)


# then merge back together and attach to the barcodes df
barcodes_indiv_2['herb'] = herb_indiv
barcodes_indiv_2['herbarium_code']  = occs['herbarium_code']
barcodes_indiv_2['inst'] = inst_indiv
barcodes_indiv_2['institude']  = occs['institute']
print(barcodes_indiv_2['HasOnlyDigits'])
print(barcodes_indiv_2[barcodes_indiv_2.HasOnlyDigits == True])
# barcodes_indiv_2.dropna(axis=1, inplace=True)

bc2 = barcodes_indiv_2[barcodes_indiv_2.HasOnlyDigits == True]


bc2.to_csv('/Users/serafin/Desktop/NT_20240325.csv', sep = ';')


# barcodes_new = pd.read_csv('/Users/serafin/Desktop/paleotrop_tmp-done.csv', sep = ';', index_col=0)
barcodes_new = pd.read_csv('/Users/serafin/Desktop/NT_20240325_done.csv', sep = ';', index_col=0)
# sort back to index as in occs
# barcodes_new.sort_index(inplace=True)
# barcodes_better = barcodes_better.apply(lambda row: ', '.join(str(value) for value in row if value is not None), axis=1)

# barcodes_new=barcodes_new.fillna('')
print('HERE:',barcodes_new)
print(barcodes_new.columns)
# drop duplicarted columns before merging barcode columns
barcodes_new.drop(['herb', 'herbarium_code', 'inst', 'institude', 'HasOnlyDigits'], axis = 1, inplace=True)

barcodes_new.replace({np.nan: None}, inplace=True)
print('HERE2:',barcodes_new)
barcodes_new = barcodes_new.apply(lambda row: ', '.join(str(value) for value in row if value is not None), axis=1)
print('HERE3:',barcodes_new)
barcodes_new = barcodes_new.str.strip()
barcodes_new = barcodes_new.str.strip(', ')
# barcodes_new = barcodes_new.replace({'NaN': ''}, regex=False)
# occs = pd.concat([occs, barcodes_new])
occs['barcodes_tmp1'] = barcodes_new
print(occs.columns)
print(occs[occs.barcodes_tmp1.notna()])

occs['barcodes_tmp1'] = occs['barcodes_tmp1'].fillna(occs.barcode) 
print(occs[occs.barcodes_tmp1.notna()])

# replace BC0 with no_Barcode
# barcodes_new = barcodes_new.replace({'^[A-Z]+0$': 'no_Barcode'}, regex=True)


# barcodes_new = barcodes_new.apply(lambda row: ', '.join(map(str, row)), axis=1)

print(barcodes_new)#[barcodes_new.columns.isin(occs.columns)])

occs['barcode_tmp'] = occs['barcode']
occs['barcode'] = occs['barcodes_tmp1']   
print(occs.barcode)
print(occs)


occs.to_csv('/Users/serafin/Sync/1_Annonaceae/GLOBAL_final/2_temp_int_data/NeoTropics_all_CHECKED_bc_NEW_check_done.csv', sep = ';', index=False)

# occs.to_csv('/Users/serafin/Sync/1_Annonaceae/GLOBAL_final/2_temp_int_data/PaleoTropics_all_CHECKED_bc.csv', sep = ';', index=False)
# occs.to_csv('/Users/serafin/Sync/1_Annonaceae/G_AfrAs_GDB/1_inter_steps/0_coord_discr_cleaned_NEW.csv', sep = ';', index=False)
# # ################
# #     # # then crossfill 
#     # return occs

# ################


def more_cleaning(master):

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

    master = master.astype(z_dependencies.final_col_type)

    master.det_year = master.det_year.fillna(-9999)
    master = master.sort_values(['expert_det', 'det_year', 'recorded_by', 'colnum', 'accepted_name'], ascending=[True, False, True, True, True])
    master.orig_bc = master.orig_bc.fillna('')
    master.orig_recby = master.orig_recby.fillna('')
    master.modified = master.modified.fillna('')
    master.geo_issues = master.geo_issues.fillna('')


    master_sn  = master[master[['recorded_by', 'colnum']].isna().any(axis=1)]
    master_num = master[master[['recorded_by', 'colnum']].notna().all(axis=1)]

    master_num.loc[master_num['prefix'] == '<NA>', ['prefix']] = '-9999'
    master_num.loc[master_num['sufix'] == '<NA>', ['sufix']] = '-9999'

    master_num.prefix = master_num.prefix.fillna('-9999')
    master_num.sufix = master_num.sufix.fillna('-9999')
    print('Total records:', len(master_num),
                        '\n By recorded_by, colnum, sufix', 
                        master_num.duplicated([ 'recorded_by', 'colnum', 'sufix'], keep='first').sum(),
                        '\n By BARCODE (!this includes NA barcodes!)', 
                        master_num.duplicated([ 'barcode' ], keep='first').sum(),
                        '\n ................................................. \n ')


    full_deduplication = master_num.groupby([ 'recorded_by', 'prefix', 'colnum', 'sufix'], as_index = False).agg(
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
               # origin =  pd.NamedAgg(column = 'origin', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x)),
                orig_recby = pd.NamedAgg(column = 'orig_recby', aggfunc=lambda x: ', '.join(x)),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
            )

    full_deduplication = full_deduplication.sort_values(['recorded_by', 'colnum'], ascending = [True, True])
    
    # merge back together


    print('END Total records:', len(full_deduplication),
                        '\n By recorded_by, colnum, sufix', 
                        full_deduplication.duplicated([ 'recorded_by', 'colnum', 'sufix'], keep='first').sum(),
                        '\n By BARCODE (!this includes NA barcodes!)', 
                        full_deduplication.duplicated([ 'barcode' ], keep='first').sum(),
                        '\n ................................................. \n ')



    master_out = pd.concat([full_deduplication, master_sn])
    for col in master_out.columns:

        try:
            master.loc[master[col] == 'nan', col] = pd.NA
            master[col] = master[col].str.strip()
            master[col] = master[col].str.strip(' ')
            master_out[col] = master_out[col].fillna('-9999')
            print(col, 'NA going to -9999')
            print(master_out[col])
        except:
            master_out[col] = master_out[col].fillna(-9999)
            print(col, 'NA going to int -9999')
    return master_out


# mast = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GDB_2/X_GLOBAL/backup/2024-01-26-5_master_db.csv', sep = ';')
# indet = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GDB_2/X_GLOBAL/indet_backlog.csv', sep = ';')
# occs = pd.concat([mast, indet])

# print(mast[mast.barcode=='-9999'])

# print(occs[['recorded_by', 'prefix', 'colnum', 'sufix']])

# master_upd = more_cleaning(occs)
# print(master_upd[['recorded_by', 'prefix', 'colnum', 'sufix']])

# ARP = master_upd[master_upd.recorded_by == 'Acevedo-Rodriguez, P']
# print(ARP[ARP.colnum == '8355'][['recorded_by', 'colnum', 'det_by', 'accepted_name','prefix', 'sufix']])

# mast_new = master_upd[master_upd.accepted_name != '-9999']
# print(mast_new.shape)

# indet_new = master_upd[master_upd.accepted_name == '-9999']
# print(indet_new.shape)

# print('OLD', occs.shape)
# print('UPD', master_upd.shape)

# mast_new.to_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GDB_2/X_GLOBAL/master_db.csv', sep = ';')
# mast.to_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GDB_2/X_GLOBAL/backup/2024-01-26-LAST_master_db.csv', sep = ';')

# indet_new.to_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GDB_2/X_GLOBAL/indet_backlog.csv', sep = ';')
# indet.to_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GDB_2/X_GLOBAL/backup/2024-01-26-LAST_indet.csv', sep = ';')

# master_upd.to_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GDB_2/X_GLOBAL/master_db.csv', sep = ';')
# occs.to_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GDB_2/X_GLOBAL/backup/2024-01-26-5_master_db.csv', sep = ';')