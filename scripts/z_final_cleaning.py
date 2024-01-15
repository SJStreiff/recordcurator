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
# import regex as re
# import requests
# import swifter
# import logging



################
# def final_clean(occs, verbose=True, debug=False):
#     """
#     occs: dataframe to check 
#     """

#     # first format barcodes one last time

#         # find all barcodes with "[A-Z]0"
################

occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_AfrAs_GDB/1_inter_steps/0_coordinate_discrepancy.csv', sep = ';')
print(occs)

#occs = occs.head(100)

occs[occs['barcode'].isna()] = 'no_Barcode'
print(occs)
barcodes = occs.barcode
print(barcodes.head())

# # now create df of all barcodes that used to be in one cell
# barcodes_indiv = barcodes.str.split(', ', expand = True)

# # same for herbarium codes and Institutes
# herb_indiv = occs['herbarium_code'].str.split(', ', expand = True)
# print(herb_indiv)
# inst_indiv = occs['institute'].str.split(', ', expand = True)

# word_regex = r'^(?:(?![A-Z][a-z]+.+$))'
# bad_df = pd.DataFrame()
# # the go through herb and institutes and replace Words with NA and Only capital letter codes
# for col in herb_indiv.columns:
#      bad = herb_indiv[col].str.contains(word_regex, regex= True)
#      bad = bad.fillna(True)
#     # change the values directly here in df[col]=''
#      herb_indiv.loc[bad, col] = '' 
#     #  bad_df[f'{col}_bad'] = bad

# for col in inst_indiv.columns:
#      bad = inst_indiv[col].str.contains(word_regex, regex= True)
#      bad = bad.fillna(True)
#     # change the values directly here in df[col]=''
#      inst_indiv.loc[bad, col] = '' 
#     #  bad_df[f'{col}_bad'] = bad

# herb_indiv = herb_indiv.apply(lambda row: ', '.join(map(str, row)), axis=1)
# herb_indiv = herb_indiv.str.strip()
# herb_indiv = herb_indiv.str.strip(', ')

# inst_indiv = inst_indiv.apply(lambda row: ', '.join(map(str, row)), axis=1)
# inst_indiv = inst_indiv.str.strip()
# inst_indiv = inst_indiv.str.strip(', ')
# print(inst_indiv)
# # then merge back together and attach to the barcodes df
# barcodes_indiv['herb'] = herb_indiv
# barcodes_indiv['herbarium_code']  = occs['herbarium_code']
# barcodes_indiv['inst'] = inst_indiv
# barcodes_indiv['institude']  = occs['institute']

# print(barcodes_indiv)
# barcodes_indiv.to_csv('/Users/serafin/Desktop/tmpAFRAS.csv', sep = ';')


barcodes_new = pd.read_csv('/Users/serafin/Desktop/TMP2moddedAfrAs.csv', sep = ';', index_col=0)
# sort back to index as in occs
barcodes_new.sort_index(inplace=True)
barcodes_new=barcodes_new.fillna('')
print(barcodes_new)
# drop duplicarted columns before merging barcode columns
barcodes_new.drop(['herb', 'herbarium_code', 'inst', 'institude'], axis = 1, inplace=True)
barcodes_new = barcodes_new.replace({'NaN': ''}, regex=False)
# replace BC0 with no_Barcode
barcodes_new = barcodes_new.replace({'^[A-Z]+0$': 'no_Barcode'}, regex=True)


barcodes_new = barcodes_new.apply(lambda row: ', '.join(map(str, row)), axis=1)

barcodes_new = barcodes_new.str.strip()
barcodes_new = barcodes_new.str.strip(', ')

print(barcodes_new)#[barcodes_new.columns.isin(occs.columns)])

occs['barcode_tmp'] = occs['barcode']
occs['barcode'] = barcodes_new   
print(occs.barcode)
print(occs)


occs.to_csv('/Users/serafin/Sync/1_Annonaceae/G_AfrAs_GDB/1_inter_steps/0_coord_discr_cleaned.csv', sep = ';', index=False)
# ################
#     # # then crossfill 
#     # return occs

# ################