#!/usr/bin/env bash

# if you have a somewhat recent Mac, you need to set this to allow multiprocessing
OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES 

##########################################################################################
##########################################################################################
# RECORDCLEANER AND RECORDFILER  CONFIGURATION FILE       *V 1.0*                        #
#                                                                                        #
# RUN NAME: date_template                                                                #
# Date created: 2023-08-11                                                               #
# USERNAME used: serafin                                                                 #
##########################################################################################
##########################################################################################

##########################################################################################
# Step 1 RECORDCLEANER
#---VARIABLES REQUIRED
# INPUT:       path to your input file (for example GBIF download)
# DAT_FORM :   format of your input file,  options are P/GBIF/BRAHMS/MO, as of 2023-08-11
# WDIR:        directory where intermediate files will be written to
# OUT_DIR:     output directory for STEP 1
# PREFIX:      prefix used in all files produced in this run
# EXPERT:      expert dataset, options are EXP/NO/SMALLEXP
#                           # EXP: full dataset with priority over standard data
#                           # NO: not expert, not prioritised
#                           # SMALLEXP: reduce dataset from specialists. This data has priority     
#                           #         !  over all other data! Use with caution on perfectly curated 
#                           #         !  and checked datasets!!
# LOGFILE:     path including filename for logfile. If not specified, all logs are printed to STOUT
#
#---OPTIONAL VARIABLE:
# VERBOSE:     prints intermediate information to STDOUT, which might be helpful for debugging in case of issues.
#              -v 2 also outputs debugging information
##########################################################################################

INPUT='/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/0_data/20230517_Aus.txt' 
DAT_FORM='GBIF' 
WDIR='/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/1_inter_Steps/' 
OUT_DIR='/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/2_final_data/' 
PREFIX='20230517_Aus_'
EXPERT='NO'
LOGFILE='/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/L_logfiles/20230517_Aus_new.log'
VERBOSE='-v 1' 

##########################################################################################
# Step 2 RECORDFILER
# VARIABLES REQUIRED
# INPUT_2 / OUT_2: Input for step 2
# MASTERDB:        Path to master db sheet if local
# HOSTNAME:        Hostname for remote db sheet
# TABLE:           Table name if postgres DB
# SCHEMA:          Schema name if postgres DB
# LOC:             DB local or not? options: local/remote
# NA_VAL:          Value used as NA in postgres database                                 !
##########################################################################################

INPUT_2=$(echo $OUT_DIR$PREFIX'cleaned.csv')
OUT_2=$(echo $OUT_DIR$PREFIX'spatialvalid.csv')
MASTERDB='/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/'
HOSTNAME='10.4.91.57'
TABLE='phil_test_221209'
SCHEMA='serafin_test'
LOC='local'
NA_VAL='-9999'

##########################################################################################
#-----------------------------------------------------------------------------------------
##########################################################################################

# RUN STEP 1
# timestamp for log file
date >> $LOGFILE
python ../share_DB_WIP/3_scripts/recordcleaner.py $INPUT $DAT_FORM $EXPERT $WDIR $OUT_DIR $PREFIX $NOCOLLECTOR $VERBOSE -l $LOGFILE 
date >> $LOGFILE
Rscript ../share_DB_WIP/3_scripts/r_coordinate_check.R --input $INPUT_2 --output $OUT_2 >> $LOGFILE

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

date >> $LOGFILE
python ../share_DB_WIP/3_scripts/recordfiler.py $OUT_2 $EXPERT $LOC $MASTERDB $HOSTNAME $TABLE $SCHEMA $WDIR -l $LOGFILE $NA_VAL


echo 'Finished!' >> $LOGFILE
date >> $LOGFILE
echo "------------------ FINISHED -----------------"

# END
