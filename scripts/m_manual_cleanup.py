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


# extract data that needs manual cleaning

# records with more than ?ten barcodes?


master = pd.read_csv('~/Sync/1_Annonaceae/')