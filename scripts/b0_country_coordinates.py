#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for namechecking the Harvard University Herbarium collectors database

2023-01-10 sjs

CHANGELOG:
    2023-01-10: created
    2023-01-12: it works. BUT it doesn't give what i want. I do not get access to the full name of the collector or ID I need to access more deta

    2024-01-29: cleaned and curated
CONTAINS:

    country_crossfill():
      crossfils between the two country identifier columns (i.e. country_id (ISO 2 letter abbreviation), and country (full name))

'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import requests
import logging
import swifter
import country_converter as coco
import geocoder

cc = coco.CountryConverter()

import z_dependencies # can be replaced at some point, but later...






def country_crossfill(occs, verbose=True):
    """
    Take records and crossfill the country_id and country name columns
    """
    occs.reset_index(drop=True)
    #logging.info(f'Let\'s see if this works {occs.country_id}')

    if {'country_id'}.issubset(occs.columns):
        print('Country id column exists')
        logging.info('Country id column exists')
    else:
        occs['country_id'] = pd.NA
        print('NEW Country id column')
        logging.info('Country id column created NEW')

    try:
        occs.country = occs.country.replace('0', pd.NA)
    except:
        a=1
    try:
        occs.country_id = occs.country_id.replace('0', pd.NA)
    except:
        a=1
    try:
        occs['country_id'] = occs.country_id.fillna(cc.pandas_convert(series = occs.country, to='ISO2'))
        print('FILLNA', occs.country_id)
    except:
        occs['country_id'] = cc.pandas_convert(series=occs.country, to='ISO2')
        print('just plain', occs.country_id)
    try:
        occs['country'] = occs.country.fillna(cc.pandas_convert(series = occs.country_id, to='name_short'))
        print(occs.country)
    except:
        print(occs.country)
        
    occs['country_iso3'] = cc.pandas_convert(series = occs.country, to='ISO3') # needed for later in coordinate cleaner
    
    logging.info('Countries all filled: {occs.country}')

    return occs

#------------------------- country-crossfill ----------------------------------#
def get_cc_new(ddlat, ddlong):
    """
    new package to get higher resolution geocoding
    -> takes coordinate and check if country fits
    """
    try:
        out2 = geocoder.osm([ddlat, ddlong], method='reverse')
        out_country = out2.country
        # print(out_country)
        if out_country == 'Brasil':
            out_country = 'Brazil'    
        out_iso3 = cc.convert(out_country, to='ISO3')
    except:
        out_iso3 = 'ERROR-COORDINATE'

    if pd.isna(out_iso3):
        out_iso3 = 'ERROR-COORDINATE'
    
    return out_iso3 



#------------------------- country-crossfill ----------------------------------#
def cc_missing(occs, verbose=True):
    """
    Big problem is data with no useable country information. needs solving
    use reverse-geocoder to fill in these cases
    """
    
    occs = occs.reset_index(drop=True)
    occs_nf = occs[occs['country_iso3'] == 'not found']
    if len(occs_nf) > 0:
        # if all countries good we need not bother...
        occs_good = occs[occs['country_iso3'] != 'not found']
        occs_nf['country_id'] = occs_nf.swifter.apply(lambda row: get_cc_new(row['ddlat'], row['ddlong']), axis = 1, result_type = 'expand')
        occs_nf['country'] = cc.pandas_convert(series = occs_nf.country_id, to='name_short')
        occs_nf['country_iso3'] = cc.pandas_convert(series = occs_nf.country, to='ISO3')
    


        occs_out = pd.concat([occs_nf, occs_good])

        return occs_out
    else:
        # return unmodified data
        return occs



