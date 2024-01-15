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
import requests
import swifter
# import logging
import country_converter as coco
# import reverse_geocoder as rg

import geocoder


cc = coco.CountryConverter()

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

    # out_iso3 = cc.convert(out_country, to='ISO3')
    # check if country fits, otherwise maybe just maybe the coordinates are inverted...
    if pd.isna(out_iso3):
        out_iso3 = 'Probl'
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

# there are issues with switched coordinates (i.e. ddlat => ddlong and ddlong => ddlat)

    has_coords = occs[occs.ddlat.notna() ]
    no_coords= occs[occs.ddlat.isna() ] 



    #num_df = num_df[num_df[data_columns].notnull().all(axis=1)]

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
occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GLOBAL_Distr/1_inter_steps/0_coord_discr_cleaned.csv', sep = ';')
#occs = occs.head(20)
print('BEFORE', occs.shape)
after_occs = coord_consolidator(occs)
print('AFTER', after_occs.shape)

# after_occs.to_csv('/Users/serafin/Sync/1_Annonaceae/G_Am_GLOBAL_Distr/1_inter_steps/0_coord_discr_cleaned_2.csv', sep = ';', index=False)
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