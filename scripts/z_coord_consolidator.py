#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Coordinate discrepancies consolidator:
    Take duplicates with diverging coordinates and check and correct for obvious errors.

prefix_cleaner:
    checks for prefixes extracted where the collector surname is found in the full collection number
'''

import z_functions_b as dupli
import z_dependencies as z_dependencies
import warnings

import pandas as pd
import numpy as np

import requests
import swifter
# import logging
import country_converter as coco
# import reverse_geocoder as rg

import geocoder


cc = coco.CountryConverter()


working_directory = '/Users/serafin/Sync/1_Annonaceae/G_Am_GLOBAL_Distr/4_post-processing/C_coord_consolidate/'
# def get_cc(ddlat, ddlong):
#     """ 
#     do the actual extraction of the country from coords
#     """
#     # make coordinate query
#     coords = (ddlat, ddlong)
#     # print(coords)
#     #mode=2 only works for multiple queries together. not suitable for the structure here
#     try:
#         res = pd.DataFrame(rg.search(coords, mode=1)) 
#         # print(type(res))
#         # print(res.cc)
#         country = res['cc']
#     except:
#         country = pd.NA


#     return country

def get_cc_new(ddlat, ddlong, iso_3):
    """
    new package to get higher resolution geocoding
    """
    try:
        out2 = geocoder.osm([ddlat, ddlong], method='reverse')
        out_country = out2.country
        print(out_country)
        if out_country == 'Brasil':
            out_country = 'Brazil'    
        out_iso3 = cc.convert(out_country, to='ISO3')
        coord_good = True
    except:
        out_country = 'PROBLEM'
        out_iso3 = 'Prob'
        coord_good = pd.NA

    # out_iso3 = cc.convert(out_country, to='ISO3')
    # check if country fits, otherwise maybe just maybe the coordinates are inverted...
    if pd.isna(out_iso3):
        out_iso3 = 'Probl'
    print('HERE',iso_3)
    if pd.isna(iso_3):
        iso_3 = 'NA country'
    if out_iso3 != iso_3:
        # if the coordinate country doesn't amtch, check if coordinates are inverted ;-)
        try:
            out2 = geocoder.osm([ddlong, ddlat], method='reverse')
            out_country = out2.country
            if out_country == 'Brasil':
                out_country = 'Brazil'    
            out_iso3 = cc.convert(out_country, to='ISO3')
            coord_good = False
        except:
            coord_good = pd.NA
            out_iso3 = pd.NA

    return out_iso3, coord_good


def coord_consolidator(occs, verbose=True, debug=False):
    """
    occs: dataframe to check and consolidate coordinate discrepancies
    """

# remove 0 coordinates
    occs[occs.ddlat == 0 ] = pd.NA
    occs[occs.ddlong == 0 ] = pd.NA
    occs[occs.ddlat == -9999 ] = pd.NA
    occs[occs.ddlong == -9999 ] = pd.NA
    occs = occs.drop_duplicates(keep='first')


    print('Starting', occs.shape)
    # we need useable species
    # sp_problem = occs[occs.accepted_name.isna()]
    # print('indets', sp_problem.shape)

    # occs = occs[~occs.accepted_name.isna()]
    # split dataset by s.n. and non s.n.
    # doesn't make sense -> we want to just crossfill identical records


    ## Mask coordinates whic do not check out with the country field.
    print('1:', occs[['ddlat', 'ddlong']])
    occs = (occs.drop(['ddlat', 'ddlong'], axis=1).join(occs[['ddlat', 'ddlong']].apply(pd.to_numeric, errors='coerce')))


    print(min(occs.ddlat))
    print(max(occs.ddlat))
    print(min(occs.ddlong))
    print(max(occs.ddlong))

    print(occs.dtypes)
    ######--------#########--------######--------#########--------######--------#########--------######--------#########--------######--------#########--------#
        # extract no_Barcode records, 

    # clean barcode (i.e. remove white space, order all in same fashion!)
    occs['barcode'] = occs['barcode'].astype(str)
    occs['barcode'] = occs['barcode'].apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated values within a cell
    occs['barcode'] = occs['barcode'].str.strip()
    occs['barcode'] = occs['barcode'].str.strip(',')
    occs.loc[occs.barcode == 'nan','barcode'] = 'no_Barcode'


    print('TEST', occs[occs.barcode == 'nan'])
    no_BC_occs = occs[occs.barcode == 'no_Barcode']
    print(occs[occs.barcode == 'no_Barcode'].shape)
    print('So many NA barcodes:\n', no_BC_occs.barcode)


    occs_bc = occs.loc[occs['barcode'] != 'no_Barcode']
    occs_bc = occs_bc[occs_bc['barcode'] != 'nan']

 
#    occs_bc = occs_bc.dropna(axis = 0, subset = ['barcode'])
    print('So many full barcodes:\n', occs_bc.barcode)
    print(occs_bc.ddlat)

    for col in ['accepted_name', 'locality', 'recorded_by', 'det_by', 
                'col_day', 'col_month', 'col_year', 'det_by', 'det_date', 'expert_det',
                'region', 'prefix', 'colnum', 'sufix', 'col_date', 'det_day', 'det_month', 'det_year',
                'huh_name', 'geo_col', 'wiki_url','status', 'ipni_no', 'ipni_species_author']:
        # loop through columns
        try:
            occs_bc.loc[occs[col] == '', col] = pd.NA
            occs_bc.loc[occs[col] == '-9999', col] = pd.NA
            occs_bc.loc[occs[col] == 'nan', col] = pd.NA
            occs_bc.loc[occs[col] == 'NaN', col] = pd.NA
        except:
            occs_bc.loc[occs[col] == 0, col] = pd.NA
            occs_bc.loc[occs[col] == -9999, col] = pd.NA
            # int column
        

        #  go through columns and crossreference
        occs_bc[col] = occs_bc.groupby(['barcode'])[col].transform(lambda x: x.fillna(method='ffill').fillna(method='bfill'))

        #print(occs_bc[col], col)


    # just checking
    occs_bc['ddlat'] = occs_bc['ddlat'].astype(float)
    occs_bc['ddlong'] = occs_bc['ddlong'].astype(float)
    occs_bc = occs_bc.astype(z_dependencies.final_col_for_import_type)

    username = 'coord_consolidator'

    print('BEFORE DEDUPLICATION',occs_bc.shape)
    # deduplicate (x2) As we have crossfilled above, it should work quite good?
    occs_dd = dupli.duplicate_cleaner(occs_bc, dupli = ['barcode'], 
                working_directory = working_directory, prefix = 'coord_cons_', User = username, step='Master',
                expert_file = 'NO', verbose=True, debugging=False)
    
    # recombine data 
    print('AFTER DEDUPLICATION',occs_dd.shape)
    # then remerge barcode/non-barcode records
    occs_new = pd.concat([occs_dd, no_BC_occs], axis = 0)

    print('Total records:', len(occs_new), ' Using the same criteria as in recordcleaner step we have so many unique records:',
                        '\n By recorded_by, colnum, sufix, col_year, country_iso3', occs_new.duplicated([ 'recorded_by', 'colnum', 'sufix', 'col_year', 'country_iso3' ], keep='first').sum(),
                        '\n By BARCODE (!this includes NA barcodes!)', occs_new.duplicated([ 'barcode' ], keep='first').sum(),
                        '\n no Barcode records:', occs_new[occs_new.barcode == 'no_Barcode'].shape,  occs_new[occs_new.barcode.isna()].shape,
                        '\n ................................................. \n ')

# deduplicate with 'normal' deduplication step

# no, we have to run the 

    # print('1\n')
    # occs_s_n = occs_new[occs_new.colnum.isna()]
    # occs_num = occs_new.dropna(how='all', subset=['colnum'])
    # print('2\n')
    # # deduplicate (x2)
    # occs_num_dd = dupli.duplicate_cleaner(occs_num, dupli = ['recorded_by', 'colnum', 'sufix', 'col_year', 'country_iso3'], 
    #                                 working_directory = working_directory, prefix = 'Integrating_', User = username, step='Master',
    #                                 expert_file = 'NO', verbose=True, debugging=False)
    # print('3\n')
    # if len(occs_s_n) != 0:
    #     occs_s_n_dd = dupli.duplicate_cleaner(occs_s_n, dupli = ['recorded_by', 'col_year', 'col_month', 'col_day', 'accepted_name', 'country_iso3'], 
    #                             working_directory =  working_directory, prefix = 'Integrating_', User = username, step='Master',
    #                             expert_file = 'NO', verbose=True, debugging=False)
    #     print('4\n')
    #     # recombine data 
    #     occs_final = pd.concat([occs_s_n_dd, occs_num_dd], axis=0)
    #     print('4\n')
    # else:
    #     occs_final = occs_num_dd
    #     print('5\n')
    # print('Total records:', len(occs_final), ' Using the same criteria as in recordcleaner step we have so many unique records:',
    #                     '\n By recorded_by, colnum, sufix, col_year, country_iso3', occs_final.duplicated([ 'recorded_by', 'colnum', 'sufix', 'col_year', 'country_iso3' ], keep='first').sum(),
    #                     '\n By BARCODE (!this includes NA barcodes!)', occs_final.duplicated([ 'barcode' ], keep='first').sum(),
    #                     '\n no Barcode records:', occs_final[occs_final.barcode == 'no_Barcode'].shape,
    #                     '\n ................................................. \n ')

    # if 1==1:
    #     return 'Banana'


    # then recheck the remaining problematic cooridinates
    has_coords = occs[occs.ddlat.notna() ]
    no_coords= occs[occs.ddlat.isna() ] 


    with requests.Session() as session:
        has_coords[['cc_iso3', 'coord_good']] = has_coords.swifter.apply(lambda row: get_cc_new(row['ddlat'], row['ddlong'], row['country_iso3']), axis = 1, result_type = 'expand')
    #print(occs.coordcountry)
    #occs['cc_iso3'] = cc.pandas_convert(series = occs.coordcountry, to='ISO3')

    print('2:', has_coords.shape)
    print(has_coords.columns)
    occs_good = has_coords[has_coords.cc_iso3 == has_coords.country_iso3]
    print('3:', occs_good[['cc_iso3', 'country_iso3']])
    occs_bad  = has_coords[has_coords.cc_iso3 != has_coords.country_iso3]
    print('4:', occs_bad[['barcode','cc_iso3', 'country_iso3']])
    occs_bad['ddlat'] = pd.NA
    occs_bad['ddlong'] = pd.NA
    # print(occs['cc_iso3', 'country_iso3'])
    # here mask coordinates with NA
    occs_cc_checked = pd.concat([occs_bad, occs_good, no_coords], axis = 0)


    # COLNUM not NA
    # group by collector, number, prefix, sufix, 
    occs_cc_checked['colnum_pre_consolidate'] = occs_cc_checked['colnum']
    # group by anything that matches irrespective of colnum
    occs_cc_checked['colnum'] = occs_cc_checked.groupby(['recorded_by','country', 'locality', 'col_day', 'col_month', 'col_year'])['colnum'].transform(lambda x: x.fillna(method='ffill').fillna(method='bfill'))

    occs_cc_checked_check = occs_cc_checked[occs_cc_checked.colnum != occs_cc_checked.colnum_pre_consolidate]
    occs_cc_checked_check = occs_cc_checked_check[occs_cc_checked_check['colnum'].notna()]
    print('CHECKING',occs_cc_checked_check[['colnum', 'colnum_pre_consolidate']])
    # then redo the groupby and ffill/bfill on coordinates

    occs_cc_checked['ddlat'] = occs_cc_checked.groupby(['recorded_by','country','col_year', 'colnum', 'locality'])['ddlat'].transform(lambda x: x.fillna(method='ffill').fillna(method='bfill'))
    occs_cc_checked['ddlong'] = occs_cc_checked.groupby(['recorded_by','country','col_year', 'colnum', 'locality'])['ddlong'].transform(lambda x: x.fillna(method='ffill').fillna(method='bfill'))

    print()
    print('Total records:', len(occs_cc_checked), ' Using the same criteria as in recordcleaner step',
                        '\n By recorded_by, colnum, sufix, col_year, country_iso3', occs_cc_checked.duplicated([ 'recorded_by', 'colnum', 'sufix', 'col_year', 'country_iso3' ], keep=False).sum(),
                        '\n By BARCODE', occs_cc_checked.duplicated([ 'barcode' ], keep=False).sum(),
                        '\n ................................................. \n ')


    return occs




# TESTING
occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GLOBAL_Distr/1_inter_steps/0_coord_discr_cleaned_NEW.csv', sep = ';')
print(occs[occs.recorded_by == 'no_Barcode'])
occs = occs.astype(z_dependencies.final_col_for_import_type)
print(occs[occs.barcode == 'no_Barcode'].shape)

#occs = occs.head(20)
print('BEFORE', occs.shape)
after_occs = coord_consolidator(occs)
print('AFTER', after_occs.shape)

after_occs.to_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GLOBAL_Distr/1_inter_steps/0_coord_discr_cleaned_2.csv', sep = ';', index=False)
out = get_cc_new(-4.238333,-55.791944)
print(out)

#     return occs_cleaned, occs_prob




# debug = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/recordcurator/test_data/test_coord_cons.csv', sep = ';')
# print(debug)




 ###--- get-cc(iso2 countr


# def prefix_cleaner(occs):
#     """
#     Clean prefixes to get better deduplication from master databases.
#     """

#     occs['surnames'] = occs.recorded_by.astype(str).str.split(',').str[0]
#     print(occs.surnames)

#     # housekeeping: NA
#     occs.prefix = occs.prefix.replace('NA', pd.NA)
#     occs.prefix = occs.prefix.replace('nan', pd.NA)
    
#     mask = occs['prefix'] == occs['surnames']
#     occs.loc[mask, 'prefix'] = pd.NA

#     return occs

# # test = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_AfrAs_GDB/X_GLOBAL/master_db.csv', sep = ';')
# # test1 = prefix_cleaner(test)
# # print(test1)
# # test1.to_csv('/Users/serafin/Sync/1_Annonaceae/G_AfrAs_GDB/X_GLOBAL/master_db.csv', sep = ';', index=False)