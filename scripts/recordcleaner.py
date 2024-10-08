#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
This program takes your raw records and compares them to existing data if you have it,
cleaning up column names, removing duplicates and making it more pleasing in general

Launch this script with the associated bash config file from the command line in your specified conda environment.
'''

import z_functions_b as stepB
#import z_functions_c as stepB2
import z_expert_V2 as small_exp
#import z_merging as stepD

# import functions for pipeline
import a1_colstandardiser as A1
import a2_colcleaner as A2
import a3_collectornames as A3
import b0_country_coordinates as B0
import b1_duplistats as B1
import b2_deduplication as B2
import c_nomenclature as C

import z_HUH_query as huh_query


import z_dependencies

#import dependencies

import argparse, os, pathlib, codecs
import pandas as pd
import numpy as np
import logging


print(os.getcwd())



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='RECORDCLEANER',
                                     description='RECORDCLEANER takes your raw occurence records and compares them to an existing database if you have it, cleaning up column names, removing duplicates and making it more pleasing in general',
                                     epilog='If it doesn\'t work, or you would like to chat, feel free to contact me at serafin.streiff<at>ird.fr')
    parser.add_argument('input_file',
                        help='Raw input file path',
                        type = pathlib.Path)
    parser.add_argument('data_type',
                        help = 'File format. So far I can only handle Darwin core (GBIF) or herbonautes (P)',
                        type = str, choices=['GBIF', 'P', 'BRAHMS', 'BRAHMS_NEW', 'MO', 'RAINBIO', 'MO_tropicos', 'SPLINK']) # modify if anything else becomes available.
    parser.add_argument('expert_file',
                         help = 'Specify if input file is of expert source (i.e. all determinations and coordinates etc. are to have priority over other/previous data)',
                         type = str,
                         choices = ['EXP', 'NO', 'SMALLEXP'] )
    parser.add_argument('working_directory',
                        help = 'the directory in which to deposit all intermediate working files. Files that need reviewing start with "TO_CHECK_"',
                        type = str)
    parser.add_argument('output_directory',
                        help = 'the wished output directory',
                        type = str)
    parser.add_argument('prefix',
                        help = 'prefix for ouput filenames',
                        type = str)
    # parser.add_argument('--nonamecln',
    #                     help = 'if specified, collector names are expected in the format <Surname>, <F>irstname, or if deviating standardised to be identical across all datasets. The collector names will not be standardised! Use with caution!',
    #                     type = int)
    parser.add_argument('-v', '--verbose',
                        help = 'If true (default), I will print a lot of stuff that might or might not help...',
                        default = True)
    parser.add_argument('-l', '--log_file',
                        help = 'path specifying a location for the output logfile.',
                        type = str)
    args = parser.parse_args()

#----------------------- Configuration output to CLI and Logfile ------------------------------------#
    print('-----------------------------------------------------------\n',
          'This is the RECORD CLEANER step of the pipeline\n',
          'Arguments supplied are:\n',
          'INPUT FILE:', args.input_file,
          '\n Data type:', args.data_type,
          '\n Expert status:', args.expert_file,
          '\n Working directory:', args.working_directory,
          '\n Output directory:', args.output_directory,
          '\n Prefix:', args.prefix,
          '\n verbose:', args.verbose,
          '\n Log file:', args.log_file,
          '\n-----------------------------------------------------------\n')
    
    logging.basicConfig(filename=args.log_file, encoding='utf-8', level=logging.DEBUG)
    logging.info('-----------------------------------------------------------\n')
    logging.info('This is the RECORD CLEANER step of the pipeline\n')
    logging.info('Arguments supplied are:')
    logging.info(f'INPUT FILE: {args.input_file}')
    logging.info(f'Data type: {args.data_type}')
    logging.info(f'Expert status: {args.expert_file}')
    logging.info(f' Working directory: {args.working_directory}')
    logging.info(f' Output directory: {args.output_directory}')
    logging.info(f' Prefix: {args.prefix}')
    logging.info(f' verbose: {args.verbose}')
    logging.info('-----------------------------------------------------------')
#-------------------------------------------------------------------------------------------------------#


    #####################################################################################################
    ###------------------------------------------ Small expert dataset ------------------------------####
    #####################################################################################################

    if args.expert_file == 'SMALLEXP':
        print('small Expert setting')
        logging.info('#> SMALL EXPERT file. separate step')
        exp_occs = small_exp.read_expert(args.input_file)
        logging.info('#> SMALL EXPERT file. HUH crossreference')
        exp_occs_2 = huh_query.huh_wrapper(exp_occs, verbose = False, debugging = False) 
        logging.info('#> SMALL EXPERT file. IPNI crossreference')
        exp_occs_3 = small_exp.exp_run_ipni(exp_occs_2)

        if any(col in {'country', 'country_id'} for col in exp_occs_3.columns):
            logging.info('#> SMALL EXPERT file. Coutry/coordinate check')
            exp_occs_4 = B0.country_crossfill(exp_occs_3, verbose=True)
            if {'ddlat', 'ddlong'}.issubset(exp_occs_3.columns):
                exp_occs_4 = B0.cc_missing(exp_occs_4, verbose=True)
            else:
                logging.info('#> SMALL EXPERT file. No coordinates in data')
                print('No coordinates to fill in missing countries')
        else:
            exp_occs_4 = exp_occs_3
        #done
        try:
            print(exp_occs_4.columns)
            print(exp_occs_4)
            exp_occs_4 = exp_occs_4.astype(small_exp.expert_types)
        except:
            exp_occs_4 = exp_occs_4.astype(small_exp.expert_min_types)
        exp_occs_4.to_csv(args.output_directory+args.prefix+'cleaned.csv', index=False, sep=';')
        logging.info('#> SMALL EXPERT file FINISHED. file written to output directory.')


#----------------------------------------------------------------------------------------------------------#

    ########################################################################################################
    ###------------------------------------------ 'Normal' dataset -------------------------------------####
    ########################################################################################################
    
    else:
        logging.info('#> NORMAL recordcurator')
        ###------------------------------------------ Step A -------------------------------------------####
        logging.info('#> A1: Column standardisation')
        # Data preprocessing: Column standardisation 
        # Step A1: Standardise selection  and subset columns....
        tmp_occs = A1.column_standardiser(args.input_file, args.data_type, verbose = True, debugging = False) # verbose by default true
        #-----------------------------------------------
        logging.info('\n#> A2: Column cleaning\n')
        # Step A2: Clean colunms and first step of standardising data (barcodes, event dates, ...)
        tmp_occs_2 = A2.column_cleaning(tmp_occs, args.data_type, args.working_directory, args.prefix, verbose=True, debugging=False)
        if args.expert_file == 'EXP': # add expert flag or not
            tmp_occs_2['expert_det'] = 'expert_det_file'
        if args.expert_file == 'NO':
            tmp_occs_2['expert_det'] = pd.NA

        #-----------------------------------------------
        logging.info('\n#> A3: Collector name processing\n')
        # Step A3: Standardise collector names 
            # Here we check if the user wants to check the collector names, 
            # and if yes, the user can reinsert checked non-conforming names into the workflow
        tmp_occs_3, frame_to_check = A3.collector_names(tmp_occs_2, args.working_directory, args.prefix, verbose=False, debugging=False)
        # should we reinsert the names we could not deal with?
        print(tmp_occs_3[['colnum', 'colnum_full', 'prefix', 'sufix']])
        print('\n ................................\n',
        'Would you like to reinsert the collector names I couldn\'t handle?',
        'Please take care of encoding (usually best is UTF-8) when opening (especially in Microsoft Excel!!)',
        'If you would like to reinsert your checked names, please indicate the path to your modified file. Otherwise type "n"')
        reinsert=input() #'n' # make back to input()
        logging.info(f'-> Your input for name reconciling: {reinsert}')

        if reinsert == 'n':
            logging.info('Ok I will continue without any reinsertion')
        else:
            # check for fileending etc
            logging.info(f'reinserting the file {reinsert}')
            
            try:
                logging.info('\n#> Reintegrating data\n')
                tmp_occs_3 = A3.reinsertion(tmp_occs_3, frame_to_check, reinsert, debugging=False)
                logging.info('Reintegration successful!')
                
            except:
                logging.error('ERROR: I couldn\'t read the file from the path you provided. Try again.')
                print('Integration anomalous, check log and confirm all working as expected.')
        logging.info('\n ................................\n')
        
        
        #-----------------------------------------------
        # Here I am blacklisting some herbaria, as I cannot work with their data. (no proper barcodes, mixed up columns)
        # For now this is not much data loss. Update as necessary.
        HERB_TO_RM = [['AAU']]
        logging.info(f'Before removing dataproblematic institutions: {tmp_occs_3.shape}')
        tmp_occs_3 = tmp_occs_3[tmp_occs_3['institute'] != 'AU']
        tmp_occs_3 = tmp_occs_3[tmp_occs_3['institute'] != 'AAU']
        logging.info(f'After removing dataproblematic institutions: {tmp_occs_3.shape}')

        #-----------------------------------------------
        # Step A4: Query botanist names to HUH botanists database
        # HUH name query
        logging.info('\n#> A4: HUH name query\n')
        print('This may take a minute to initialise the HUH crossreference')
        tmp_occs_3 = huh_query.huh_wrapper(tmp_occs_3, verbose = False, debugging = False)
        tmp_occs_3 = tmp_occs_3.reset_index(drop=True)
        logging.info('\n #> STEP A complete.\n')
        #-----------------------------------------------
        # Step B4: crossfill country names
        logging.info('\n#> B4: Crossfill country values\n')
        tmp_occs_3 = B0.country_crossfill(tmp_occs_3, verbose=True)
        tmp_occs_3 = B0.cc_missing(tmp_occs_3, verbose=True)


        ###------------------------------------------ Step B -------------------------------------------####
        # Data deduplication

        logging.info('\n#> B1: Duplication statistics etc\n')
        # Step B1: Duplication (sensitivity) statistics and separating out of << s.n. >> Collection numbers
        tmp_colnum, tmp_s_n = B1.duplicate_stats(tmp_occs_3, args.working_directory, args.prefix)

        # for records with a collection number
        dup_cols = ['recorded_by', 'colnum', 'sufix', 'col_year', 'country_iso3'] # the columns by which duplicates are identified

        #-----------------------------------------------
        logging.info('\n#> B2: Duplicates - merge duplicate records \n')
        # Step B2: deduplicate data: merge duplicate records
        tmp_occs_4 = B2.duplicate_cleaner(tmp_colnum, dupli = dup_cols, working_directory = args.working_directory, prefix = args.prefix, User='NA', expert_file = args.expert_file, verbose=False, debugging=False)
        logging.info(f'Length of TMP 4:{len(tmp_occs_4)}')

        print(tmp_occs_4[['recorded_by','colnum','ddlat']])

        
        # Double checking duplication stats, should show 0. (i.e. repeat B1
        # no output as specified in the 'out=False' flag)
        logging.info('\n#> B2: Duplicates - stats after first merge \n')
        B1.duplicate_stats(tmp_occs_4, args.working_directory, args.prefix, out = False)
        #logging.info(tmp_occs_4.columns)
    
        #-----------------------------------------------
        # only for records with no Collection number
        dup_cols1 = ['recorded_by', 'col_year', 'col_month', 'col_day', 'genus', 'specific_epithet', 'country_iso3', 'locality'] # the columns by which duplicates are identified
        logging.info('\n#> B3: Duplicates - merge <s.n.> duplicate records \n')
        # Step B3: s.n. deduplicate
        tmp_s_n_1 = B2.duplicate_cleaner(tmp_s_n, dupli = dup_cols1, working_directory= args.working_directory, prefix= args.prefix, User='NA', expert_file= args.expert_file, verbose=False, debugging=False)
        logging.info(f'S.N.: {len(tmp_s_n_1)}')

        # now recombine numbered and s.n. data
        tmp_occs_5 = pd.concat([tmp_occs_4, tmp_s_n_1])

        print(tmp_occs_5[['recorded_by','colnum','ddlat']])
        print('from here good?')


        ###------------------------------------------ Step C -------------------------------------------####
        # Nomenclature checking
        # This step is skipped if input file is expert determined!
        logging.info('\n#> C: Taxonomy - POWO - check\n')

        if args.expert_file != 'EXP':
            # step C1, nomenclature check with POWO/IPNI
            print('\n.........................................\n')
            print('Checking the taxonomy now. This takes a moment!')
            tmp_occs_6 = C.kew_query(tmp_occs_5, args.working_directory, verbose=True)
            # as i filter later for det or not, re-merge the data   
        

        else:
 
            if args.expert_file == 'EXP':
                tmp_occs_5['status'] = 'ACCEPTED'
                miss_col = [i for i in z_dependencies.final_cols_for_import if i not in tmp_occs_4.columns]
                tmp_occs_5[miss_col] = pd.NA
                tmp_occs_5 = tmp_occs_5.astype(dtype = z_dependencies.final_col_for_import_type)

                tmp_occs_5.accepted_name = tmp_occs_5.genus + ' ' + tmp_occs_5.specific_epithet
                tmp_occs_5.status = 'ACCEPTED'

                if args.verbose:
                    logging.info('As you have an EXPERT file i did not crosscheck the taxonomy (some spp. might not yet be on POWO)')
                    logging.info('therefore some columns are missing: I will fill them with <NA>!')
                    print('Expert file. Did not check taxonomy with POWO. Please make sure no NA was present in your determinations!')
            tmp_occs_6 = tmp_occs_5
            # print(tmp_occs_5.dtypes)


        logging.info('#> C: Taxonomy done.')
        
    ########################################################################################################
    ###------------------------------------------DONE: Write dataset -----------------------------------####
    ########################################################################################################
    
        # output cleaned data to csv
        tmp_occs_6.to_csv(args.output_directory+args.prefix+'cleaned.csv', index=False, sep=';')
        logging.info(f'\n\n---------------------------------------------\n The output of this first processing is saved to: {args.output_directory+args.prefix}cleaned.csv\n---------------------------------------------\n')

    #coordinate checks take place in R. This is launched from the bash command file.
    #---------------------------------------------------------------------------
    logging.info('#> First cleaning steps completed. Next is Coordinate validation. ')
    logging.info('\n\n------------------****** R *****-----------------------\n\n')


########################################################################################################
###-------- END OF RECORDCLEANER ------###
########################################################################################################