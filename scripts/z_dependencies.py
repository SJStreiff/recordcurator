#!/usr/bin/env 	'python',
# -*- coding: utf-8 -*-

""" 
Here are some variables that account for all potential column names we find in various input datasets.
This can be modified to adjust for new datasets you might want to include.


"""

import pandas as pd
# final columns wished in the final database

# columns required to import data into the final database
final_cols_for_import = list([#'global_id',
    #'childData_id',
	#'accSpecies_id',
    'source_id',
    'scientific_name',
	'genus',
	'specific_epithet',
	'species_author',
	'collector_id',
    'recorded_by',
    'colnum_full',
	'prefix',
	'colnum',
	'sufix',
    'col_date',
    'col_day',
    'col_month',
    'col_year',
	'det_by',
	'det_date',
    'det_day',
    'det_month',
    'det_year',
	'country_iso3',
	'country',
	'continent',
	'locality',
	'coordinate_id',
	'ddlong',
    'ddlat',
	'institute',
    'herbarium_code',
    'barcode',
    'orig_bc',
    'coll_surname',
    'orig_recby',
    'status',
    'accepted_name',
    'ipni_no',
    'huh_name',
    'geo_col',
    'wiki_url',
    'expert_det',
    'ipni_species_author',
    'modified',
    'geo_issues',
    'link'])

final_col_for_import_type = {#'global_id': str,
    #'childData_id': str,
	#'accSpecies_id': str,
    'source_id': str,
    'scientific_name': str,
	'genus': str,
	'specific_epithet': str,
	'species_author': str,
	'collector_id': str,
    'recorded_by': str,
    'colnum_full': str,
	'prefix': str,
	'colnum': str,
	'sufix': str,
    'col_date': str,
    'col_day': pd.Int64Dtype(), # this allows nan within int
    'col_month': pd.Int64Dtype(),
    'col_year': pd.Int64Dtype(),
	'det_by': str,
	'det_date': str,
    'det_day': pd.Int64Dtype(),
    'det_month': pd.Int64Dtype(),
    'det_year': pd.Int64Dtype(),
	'country_iso3': str,
	'country': str,
	'continent': str,
	'locality': str,
	'coordinate_id': str,
	'ddlong': float,
    'ddlat': float,
	'institute': str,
    'herbarium_code': str,
    'barcode': str,
    'orig_bc': str,
    'coll_surname': str,
    'orig_recby': str,
    'status': str,
    'accepted_name': str,
    'ipni_no': str,
    'huh_name': str,
    'geo_col': str,
    'wiki_url': str,
    'expert_det' : str, # NO/ pd.NA or EXP
    'ipni_species_author': str, #not sure we need this, but easier to throw out later
    'modified': str, # last modified list.
    'geo_issues': str,
    'link':str,
    }



final_cols = list(['source_id',
    'scientific_name',
	'genus',
	'specific_epithet',
	'species_author',
	'collector_id',
    'recorded_by',
	'prefix',
	'colnum',
	'sufix',
    'colnum_full',
    'col_date',
    'col_day',
    'col_month',
    'col_year',
	'det_by',
	'det_date',
    'det_day',
    'det_month',
    'det_year',
	'country_iso3',
	'country',
	'continent',
	'locality',
	'coordinate_id',
	'ddlong',
    'ddlat',
	'institute',
    'herbarium_code',
    'barcode',
    'huh_name',
    'geo_col',
    'wiki_url',
    'link'])

final_col_type = {'source_id': str,
    'scientific_name': str,
	'genus': str,
	'specific_epithet': str,
	'species_author': str,
	'collector_id': str,
    'recorded_by': str,
	'prefix': str,
	'colnum': str,
	'sufix': str,
    'colnum_full': str,
    'col_date': str,
    'col_day': pd.Int64Dtype(), # this allows nan within int
    'col_month': pd.Int64Dtype(),
    'col_year': pd.Int64Dtype(),
	'det_by': str,
	'det_date': str,
    'det_day': pd.Int64Dtype(),
    'det_month': pd.Int64Dtype(),
    'det_year': pd.Int64Dtype(),
	'country_iso3': str,
	'country': str,
	'continent': str,
	'locality': str,
	'coordinate_id': str,
	'ddlong': float,
    'ddlat': float,
	'institute': str,
    'herbarium_code': str,
    'barcode': str,
    'huh_name': str,
    'geo_col': str,
    'wiki_url': str,
    'link':str}


# herbonautes data set columns present and wished in database
herbo_key = {
    'specimen...code': 'barcode',
    'collector...collector':'recorded_by',
    #'collector...other_collectors': 'addCol',
    #'prefix': 'prefix',
    'collect_number...number': 'colnum_full',
    #'sufix': 'sufix',
    'identifier...identifier': 'det_by',
    'Date.deter...collect_date': 'det_date',
    'collect_date...collect_date': 'col_date',
    'country...country': 'country',
    'region1...region': 'region',
    'locality...locality': 'locality',
    'geo...position': 'coordinates',
    #'ddlat': 'ddlat',
    # 'ddlong': 'ddlong',
    'specimen...genus': 'genus',
    'specimen...specific_epithet': 'specific_epithet',
    'scientificName': 'scientific_name'}

#
# herbo_cols_to_add = [[plantdesc: 'plantdesc']]
#
herbo_subset_cols = (['recorded_by',
	#'prefix',
	'colnum_full',
	#'sufix',
	#'addCol',
	'det_by',
	'det_date',
	'col_date',
	'region',
	'locality',
	#'ddlat',
	#'ddlong',
    'coordinates',
    'barcode',
	'country',
	'genus',
	'specific_epithet'])

gbif_key = {#'gbifID': 'source_id',
    'genus': 'genus',
    'species': 'species-tobesplit',
    'countryCode': 'country_id',
    'stateProvince': 'region',
    'decimalLatitude': 'ddlat',
    'decimalLongitude': 'ddlong',
    'day':'col_day',
    'month':'col_month',
    'year':'col_year',
    'institutionCode':'institute',
    'collectionCode':'herbarium_code',
    'catalogNumber':'barcode',
    'recordNumber':'colnum_full',
    'identifiedBy':'det_by',
    'dateIdentified':'det_date',
    'recordedBy':'recorded_by',
    'scientificName':'scientific_name',
    'references':'link'}


gbif_subset_cols =([#'source_id',
    'scientific_name',
    'genus',
    'species-tobesplit',
    'country_id',
    'locality',
    'ddlat',
    'ddlong',
    #'collector_id',
    'recorded_by',
    #'collectorNumberPrefix',
    'colnum_full',
    #'collectorNumberSuffix',
    #'colDate',
    'col_day',
    'col_month',
    'col_year',
    'det_by',
    'det_date',
    #'country',
    #'continent',
    #'coordinate_id',
    'institute',
    'herbarium_code',
    'barcode',
    'link'])

brahms_key = {#'gbifID': 'source_id',
    'GENUS': 'genus',
    'SP1': 'specific_epithet',
    'COUNTRY': 'country',
    'MAJORAREA': 'region',
    'LOCNOTES':'locality',
    'LATDEC': 'ddlat',
    'LONGDEC': 'ddlong',
    'DAY':'col_day',
    'MONTH':'col_month',
    'YEAR':'col_year',
    'DETDAY':'det_day',
    'DETMONTH':'det_month',
    'DETYEAR':'det_year',
    'BARCODE':'barcode',
    'PREFIX':'prefix',
    'SUFFIX':'sufix',
    'NUMBER':'colnum',
    'DETBY':'det_by',
    'COLLECTOR':'recorded_by',
    'scientificName':'scientific_name',
    'IMAGELIST':'link'}

brahms_cols =([#'source_id',
    'genus',
    'specific_epithet',
    'country',
    'region',
    'locality',
    'ddlat',
    'ddlong',
    'col_day',
    'col_month',
    'col_year',
    'det_day',
    'det_month',
    'det_year',
    'barcode',
    'prefix',
    'sufix',
    'colnum',
    'det_by',
    'recorded_by',
    'link'])

# RAINBIO HAS PROBLEMS FOR NOW....
rainbio_key = {
    'tax_gen': 'genus',
    'tax_esp': 'specific_epithet',
    'country': 'country',
    'maj_area_original': 'region',
    'loc_notes':'locality',
    'ddlat': 'ddlat',
    'ddlon': 'ddlong',
    'cold':'col_day',
    'colm':'col_month',
    'coly':'col_year',
    'detd':'det_day',
    'detm':'det_month',
    'dety':'det_year',
    'BARCODE':'barcode',
    'detnam':'det_by',
    'colnam':'recorded_by',
    'scientificName':'scientific_name',
    'colnum_full':'colnum_full'
}

rainbio_cols =([#'source_id',
    'genus',
    'specific_epithet',
    'country',
    'region',
    'locality',
    'ddlat',
    'ddlong',
    'col_day',
    'col_month',
    'col_year',
    'det_day',
    'det_month',
    'det_year',
    'barcode',
    'det_by',
    'recorded_by',
    'colnum_full'
    ])

MO_key = {
    'SpecimenID':'barcode',
    'DeterminationFullNameNoAuthors':'scientific_name',
    'genus':'genus',
    'specific_epithet':'specific_epithet',
    'SeniorCollector':'recorded_by',
    'CollectionNumber': 'colnum_full',
    'CountryName': 'country',
    'UpperName'	:'upper-region',
    'LowerName':'lower-region',
    'Locality':'locality',
    'decimalLatitude':'ddlat',
    'decimalLongitude': 'ddlong',
    'CollectionMonth':'col_month',
    'CollectionDay':'col_day',
    'CollectionYear': 'col_year',
    'DeterminedBy': 'det_by',
    'DeterminationYear':'det_year'
}

MO_cols = ([
    'barcode',
    'scientific_name',
    'genus',
    'specific_epithet',
    'upper-region',
    'lower-region',
    'recorded_by',
    'colnum_full',
    'country',
    'locality',
    'ddlat',
    'ddlong',
    'col_month',
    'col_day',
    'col_year',
    'det_by',
    'det_year'])


splink_key = {#s
    'genus': 'genus',
    'species':'specific_epithet',
    'country': 'country',
    'stateprovince': 'region',
    'institutioncode':'institute',
    'collectioncode':'herbarium_code',
    'catalognumber':'barcode',
    'latitude': 'ddlat',
    'longitude': 'ddlong',
    'daycollected':'col_day',
    'monthcollected':'col_month',
    'yearcollected':'col_year',
    'collectornumber':'colnum_full',
    'identifiedby':'det_by',
    'yearidentified':'det_year',
    'monthidentified':'det_month',
    'dayidentified':'det_day',
    'dateIdentified':'det_date',
    'collector':'recorded_by'}


splink_cols =([#'source_id',
    'genus',
    'specific_epithet',
    'country',
    'locality',
    'ddlat',
    'ddlong',
    #'collector_id',
    'recorded_by',
    #'collectorNumberPrefix',
    'colnum_full',
    #'collectorNumberSuffix',
    #'colDate',
    'col_day',
    'col_month',
    'col_year',
    'det_by',
    'det_day',
    'det_month',
    'det_year',
    #'continent',
    #'coordinate_id',
    'institute',
    'herbarium_code',
    'barcode'])