#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This program takes your CLEANED records, compares them to a masterdatabase,
and integrates the new data in to said masterdatabase.

v0.2 tries to optimise the testing and deduplicating over the different backlogs and master DBs

'''


#import y_sql_functions as sql
import z_merging as pre_merge
import b2_deduplication as dedupli
import z_functions_b as dupli
import z_cleanup as cleanup
import z_expert_V2 as expert

import z_dependencies

#import dependencies

import argparse, os, pathlib, codecs
import pandas as pd
import csv
import numpy as np
import datetime 
from getpass import getpass
import logging
import gc


print(os.getcwd())



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='RECORD FILER',
                                     description='RECORD FILER takes your CLEANED data and integrates it into a specified database. It is intended as step 2 in the process, i.e. following rigorous cleaning of your data.',
                                     epilog='If it doesn\'t work, or you would like to chat, feel free to contact me at serafin.streiff<at>ird.fr')
    parser.add_argument('input_file',
                        help='(Cleaned) input file path',
                        type = pathlib.Path),
    # parser.add_argument('MasterDB',
    #                     help='The location of the master database file.',
    #                     type=str)
    parser.add_argument('expert_file',
                         help = 'Specify if input file is of expert source (i.e. all determinations and coordinates etc. are to have priority over other/previous data)',
                         type = str,
                         choices = ['EXP', 'NO', 'SMALLEXP'] ),
    parser.add_argument('db_local',
                         choices=['local','remote'],
                        help='Is the database saved locally or on a server??',
                        type = str)
    parser.add_argument('database_name',
                        help='The name of the SQL database to access, or if db_local = \'local\', then the directory of database',
                        type = str)
    parser.add_argument('hostname',
                        help= 'The hostname for the SQL database',
                        type=str)
    parser.add_argument('tablename',
                        help='The table to extract from the master database (SQL)',
                        type=str)
    parser.add_argument('schema',
                        help='the schema name in which the table resides',
                        type=str)
    parser.add_argument('working_directory',
                        help='the working directory for debugging output',
                        type=str)
    parser.add_argument('na_value',
                        help = 'Value used for NA/NaN/<NA>',
                        type = str)
    
    # parser.add_argument('indets_backlog',
    #                     help='The location of the indets backlog database for crosschecking and indets-rescuing',
    #                     type=str)
    # parser.add_argument('no_coords_backlog',
    #                     help='The location of the backlog database with records of missing data for crosschecking and duplicate-rescuing',
    #                     type=str)
    parser.add_argument('-v', '--verbose',
                        help = 'If true (default), I will print a lot of stuff that might or might not help...',
                        default = True)
    parser.add_argument('-l', '--log_file',
                        help = 'path specifying a location for the output logfile.',
                        type = str)
    args = parser.parse_args()
    # optional, but for debugging. Maybe make it prettier
    #print('Arguments:', args)

    print('-----------------------------------------------------------\n')
    print('#> This is the RECORD FILER step of the pipeline\n',
          'Arguments supplied are:\n',
          'INPUT FILE:', args.input_file,
          '\n Expert status:', args.expert_file,
          '\n Database location (local or remote):', args.db_local,
          '\n Database name:', args.database_name,
          '\n Hostname:', args.hostname,
          '\n Table name:', args.tablename,
          '\n Schema name:', args.schema,
          '\n Working directory:', args.working_directory,
          '\n verbose:', args.verbose)
    print('-----------------------------------------------------------\n')


    logging.basicConfig(filename=args.log_file, encoding='utf-8', level=logging.INFO)
    logging.info('-----------------------------------------------------------\n')
    logging.info('#> This is the RECORD FILER step of the pipeline\n')
    logging.info('Arguments supplied are:')
    logging.info(f'INPUT FILE: {args.input_file}')
    logging.info(f'Expert status: {args.expert_file}')
    logging.info(f'Database location (local or remote): {args.db_local}')
    logging.info(f'Database name: {args.database_name}')
    logging.info(f'\n Hostname: {args.hostname}')
    logging.info(f'Table name: {args.tablename}')
    logging.info(f'Schema name: {args.schema}')
    logging.info(f' Working directory: {args.working_directory}')
    logging.info(f' verbose: {args.verbose}')
    logging.info('-----------------------------------------------------------')


    """
    In record filer we want to implement the following steps.

    - download database subset that we need to integrate into (i.e. subset by genera or region?)

    - check input data for valid barcodes and other details?

    - see which are duplicates of already existing data.
        reference to s.n. collection
        reference to 'missing-coordinate' collection

    - integrate data.

    -reupload data into server.

    """


    #---------------------------------------------------------------------------
    logging.info('\n#> Now we \n')
    logging.info('\t - Check your new records against indets and for duplicates \n')
    logging.info('\t - Merge them into the master database\n---------------------------------------------\n')

    # check input data variation: is it just one genus? just one country?
    imp = codecs.open(args.input_file,'r','utf-8') #open for reading with "universal" type set
    occs = pd.read_csv(imp, sep = ';',  dtype = z_dependencies.final_col_for_import_type, na_values=pd.NA, quotechar='"') # read the data
    occs = occs.fillna(pd.NA)
    logging.info('NA filled!')
    # print('\n ................................\n',
    print('Please type the USERNAME used to annotate changes in the records:')
    username=input() 
        # if ever SQL integration comes back
        # print('\n ................................\n',
        # 'Please type the PASSWORD used to connect to the database for user', username)
        # password=getpass() #'n' # make back to input()
        # print('\n ................................\n',
        # 'Please type the PORT required to connect to the database:')
        # port=input() #'n' # make back to input()


    # give data a (time??)stamp
    date = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    occs['modified'] = username + '_' + date

    
    if args.db_local == 'remote':
         logging.info("Trying to read remote database...")
         logging.info('Not today')
        # TEMPORARILY NOT HAPPENING AS I JUST WANT OT DEBUG THE MERGING OF DATASETS.
        # Step D1
        # # - get database extract from the GLOBAL database.
        # print('Please check if you are connected to the VPN. If not this will not work.')

        
        # m_DB = SQL.fetch_master_db(args.database_name, args.hostname, args.tablename, args.schema)
        # print('The downloaded DB is the following:', m_DB)
        # m_DB.to_csv('./4_DB/sql_data_transf_test.csv', index=False, sep=';')
        # # m_DB =pd.read_csv('./4_DB/sql_data_transf_test.csv')
        # # This works
        # try:
        #     m_DB = m_DB.replace('nan', pd.NA)
        # except Exception as e:
        #     print('NA as nan replacement problematic')
        # try:
        #     m_DB = m_DB.replace(np.nan, pd.NA)
        # except Exception as e:
        #     print('NA as np replacement problematic')
        # 
        # print('Master database read successfully!', len(m_DB), 'records downloaded')
        # #
    
    
    # working with a local database...    
    elif args.db_local == 'local':
        logging.info('Reading local database')
        # database directory 
        mdb_dir = args.database_name
        # indet backlog read
        BL_indets = pd.read_csv(mdb_dir+'/indet_backlog.csv', sep=';', dtype= z_dependencies.final_col_for_import_type, quotechar='"', na_values=args.na_value)
        BL_indets = BL_indets.fillna(pd.NA)
        # coordinate backlog read
        no_coord_bl = pd.read_csv(mdb_dir + '/coord_backlog.csv', sep=';', dtype= z_dependencies.final_col_for_import_type, na_values=args.na_value)
        no_coord_bl = no_coord_bl.fillna(pd.NA)
        # MASTER read
        m_DB = pd.read_csv(mdb_dir + '/master_db.csv', sep =';', dtype= z_dependencies.final_col_for_import_type, na_values=args.na_value)#, na_values=[pd.NA, args.na_value])
        m_DB = m_DB.fillna(pd.NA)
        
        #if for any readon any columns are missing from the df, I will add all the final columns
        miss_col = [i for i in z_dependencies.final_cols_for_import if i not in m_DB.columns]
        m_DB[miss_col] = '0'
        m_DB = m_DB.astype(dtype = z_dependencies.final_col_for_import_type)

    logging.info(f'THE INDET BACKLOG: {len(BL_indets)}')
    logging.info(f'THE COORD BACKLOG: {len(no_coord_bl)}')
    logging.info(f'THE MASTER: {len(m_DB)}')

    # all master+backlogs together to find duplicates etc
    masters = pd.concat([no_coord_bl, BL_indets, m_DB])
    # sanity check
    if len(masters) == (len(no_coord_bl) + len(BL_indets) + len(m_DB)):
        print('Master good!')
    else:
        print('Masters not correct!')
        # maybe check what's going on if you get this message

    logging.info(f'Records master being used):{len(masters)}')

    # No barcode is BAD. (I use the barcode to deduplicate, if it is missing things go wrong!)
    master_nobc = masters[masters.barcode.isna()]
    if len(master_nobc) > 0:
        print('NA BARCODE')
        print(master_nobc[['barcode', 'orig_bc']])
        master_nobc.to_csv(mdb_dir + 'NA_barcode_PROBLEMS.csv', sep=';', index=False,  mode='a') # append to this file.
        print(Warning('THERE IS AN NaN BARCODE!!!'))
    masters = masters[~masters.barcode.isna()]
    # reading master databases + backlogs finished

    ###########################################################################################################
    ###---------------------- Small/minimal expert datasets for integration --------------------------------###
    ###########################################################################################################
    if args.expert_file == 'SMALLEXP':
        logging.info('#> SMALL EXPERT file. separate step')

        exp_occs = occs 
        master_exp_occs = expert.deduplicate_small_experts(master=masters, exp_dat=exp_occs)
        
        # sort out indet and no_coord dat
        exp_occs_final = cleanup.clean_up_nas(master_exp_occs, args.na_value)
        exp_occs_final = exp_occs_final[z_dependencies.final_cols_for_import]
        exp_occs_final = exp_occs_final.astype(z_dependencies.final_col_for_import_type)
        exp_occs_final = cleanup.cleanup(exp_occs_final, cols_to_clean=['source_id', 'colnum_full', 'institute', 'herbarium_code', 'barcode', 'orig_bc', 'geo_issues', 'det_by', 'link'], verbose=True)
  
        # INDET
        indet_to_backlog = exp_occs_final[exp_occs_final.accepted_name == args.na_value] # ==NA !!      
        occs = exp_occs_final[exp_occs_final.accepted_name != args.na_value] # NOT NA!
        # COORDS
        no_coords_to_backlog = occs[occs.ddlat == args.na_value] # ==NA !!
        # MASTER
        deduplid = occs[occs.ddlat != args.na_value] # NOT NA!

        # print(deduplid.shape)

        logging.info('SMALLXP handling completed')
        # final data is written below after else:

        print(sum(deduplid.accepted_name.isna()))
  
    ###########################################################################################################
    ###------------------------------ NORMAL DATASET INTEGRATION -------------------------------------------###
    ###########################################################################################################
        
    # i.e. EXPERT == EXP or NO
    else:
        if len(masters) > 0:
            # this checks for duplicated barcodes, and format
            upd_masters = pre_merge.check_premerge_v2(mdb= masters, occs=occs, verbose=True)
            upd_masters = upd_masters.astype(z_dependencies.final_col_for_import_type)
            for col in upd_masters.columns:
                try:
                    upd_masters[col] = upd_masters[col].replace('-9999', pd.NA)
                except:
                    upd_masters[col] = upd_masters[col].replace(-9999, pd.NA)
        else:
            upd_masters = occs
            upd_masters = upd_masters.astype(z_dependencies.final_col_for_import_type)



        upd_masters.colnum = upd_masters.colnum.replace({'nan': pd.NA}, regex=False)
        upd_masters.colnum = upd_masters.colnum.replace({'-9999': pd.NA}, regex=False)

        occs_s_n = upd_masters[upd_masters.colnum.isna()]
        occs_num = upd_masters.dropna(how='all', subset=['colnum'])
        
        logging.info(f'The numbered records being deduplicated {len(occs_num)}')
        logging.info(f'The s.n. records being deduplicated {len(occs_s_n)}')
        # deduplicate (x2)
        occs_num_dd = dedupli.duplicate_cleaner(occs_num, dupli = ['recorded_by', 'colnum', 'sufix', 'col_year', 'country_iso3'], 
                                        working_directory = args.working_directory, prefix = 'Integrating_', User = username, step='Master',
                                        expert_file = args.expert_file, verbose=True, debugging=False)
        if len(occs_s_n) != 0:
            occs_s_n_dd = dedupli.duplicate_cleaner(occs_s_n, dupli = ['recorded_by', 'col_year', 'col_month', 'col_day', 'accepted_name', 'country_iso3'], 
                                    working_directory =  args.working_directory, prefix = 'Integrating_', User = username, step='Master',
                                    expert_file = args.expert_file, verbose=True, debugging=False)
            # recombine data 
            occs = pd.concat([occs_s_n_dd, occs_num_dd], axis=0)

        else:
            occs = occs_num_dd

        # FILTER OUT BACKLOG-DATA
        occs.accepted_name = occs.accepted_name.replace('nan', None)
        occs.accepted_name = occs.accepted_name.replace('<NA>', None)
        occs.accepted_name = occs.accepted_name.replace(' ', None)              

        occs.ddlat = occs.ddlat.replace('0', np.nan)
        occs.ddlong = occs.ddlong.replace('0', np.nan)
        
        # clean up and NAs
        occs_final = cleanup.clean_up_nas(occs, args.na_value)
        # double check types, subset only the required columns
        occs_final = occs_final[z_dependencies.final_cols_for_import]
        occs_final = occs_final.astype(z_dependencies.final_col_for_import_type)
        # clean up duplicated values within cells
        occs_final = cleanup.cleanup(occs_final, cols_to_clean=['source_id', 'colnum_full', 'institute', 
                                                                'herbarium_code', 'orig_bc', 'geo_issues',
                                                                'det_by', 'link', 'orig_recby'], verbose=True)
        # check and double check NA values, set to a numeric value (for SQL)
        occs_final = cleanup.clean_up_nas(occs_final, args.na_value)

        # BACKLOGS
        # indets
        indet_to_backlog = occs_final[occs_final.accepted_name == args.na_value] # ==NA !!
        occs_final = occs_final[occs_final.accepted_name != args.na_value] # NOT NA!
        # coordinates
        no_coords_to_backlog = occs_final[occs_final.ddlat == int(args.na_value)] # ==NA !!
        # master
        deduplid = occs_final[occs_final.ddlat != int(args.na_value)] # NOT NA!
        # check for really problematic NA values in 'recorded_by' and 'colnum' (if NA in both, remove!)
        print('Before removing NA (collector+number):\n', deduplid.shape)
        deduplid = deduplid[(deduplid.recorded_by != args.na_value) & (deduplid.colnum != args.na_value)]
        problem_records = deduplid[(deduplid.recorded_by == args.na_value) & (deduplid.colnum == args.na_value)]
        problem_records.to_csv(mdb_dir+'unrecoverable_problems/unrecoverable_problems.csv', index=False, sep =';', mode='a')
        print('After removing NA (collector+number):\n', deduplid.shape)
        deduplid = deduplid[(deduplid.recorded_by.notna()) & (deduplid.colnum.notna())]

    # END of exp_small if/else

    ###########################################################################################################
    ###------------------------------ BACKUP DATASET, WRITE FILES ------------------------------------------###
    ###########################################################################################################

    # BACK-UP previous files    
    no_coord_bl.to_csv(mdb_dir + '/backups/coord/'+date+'_coord_backlog.csv', sep=';')
    BL_indets.to_csv(mdb_dir + '/backups/indet/'+date+'_indet_backlog.csv', sep=';')
    m_DB.to_csv(mdb_dir + '/backups/'+date+'_master_backup.csv', sep = ';', index = False)#, mode='x')
    # the mode=x prevents overwriting an existing file...

    logging.info('\n#> Merging steps complete.\n------------------------------------------------')

    logging.info(f'Trimming master database before writing: {len(deduplid)}')
    # another double check
    deduplid = deduplid[z_dependencies.final_cols_for_import]
    print('Final size:', len(deduplid))#, 'With columns:', deduplid.columns)
    print('Indet backlog size:', len(indet_to_backlog))
    print('No-Coord backlog size:', len(no_coords_to_backlog))
    # this is now the new master database...
    deduplid = deduplid.reset_index(drop=True)
   
    indet_to_backlog.to_csv(mdb_dir + 'indet_backlog.csv', sep=';', index=False)
    no_coords_to_backlog.to_csv(mdb_dir + 'coord_backlog.csv', sep=';', index=False)
    deduplid.to_csv(mdb_dir + 'master_db.csv', sep=';', index=False)

    logging.info(f'------------------------------------------------\n#> {len(deduplid)} Records filed away into master database.\n')
    

#################################################################################################################################################
