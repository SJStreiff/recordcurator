# changelog RECORDCLEANER


# 2023-08-01:
    - switched from iso2 to iso3 in country_id (works directly with coordinate_cleaner)...

# 2023-08-02:
    - changed the barcode formatting to include alphanumeric repeating formats of 00000A000A000  (to be validated)
    - changed indet_backlog criteria from 'status' to 'accepted_name': if NA, then in backlog.

# 2023-08-08:
    - added 'link' column, where urls leading to the image can be integrated (remains hit-and-miss for GBIF, as there data entered quite randomly sometimes)
    
# 2023-08-09:
    - added 'BRAHMS' flag, allows integration of BRAHMS extract data
        included slight modifications to name standardisation, recurring NA vs NA-string 

# 2023-08-22:
    - added basic scripts for SQL integration
    - finalised brahms integration

# 2023-08-23:
    - merged branch brahms integration
    - new branch: speed up master merging

# 2023-09-13:
    - new branch deduplication error solving (coordinates);
        * [solved] discovered bug in deduplication leading to data getting lost in groupby (groupby on a column including only NA make data go away)
        * [solved]bug in (pure) colnum not extracting correctly for the format 00-123 or similar...
        * [BUG] coordinate filter (difference between 0.1 mean, otherwise throw out, not working)

# 2023-09-18:
    - coordinate problems worked out. now coordinates more than 0.1 variance are tested for country-coordinate match and if match then kept...

# 2023-09-25:
    - [SOLVED] coordinates now filter eventual duplicates with coordinates with a variance > 0.1

# 2023-09-26/27:
    - cleaned up superfluous files, deprecated scripts...
    - merging branch coordinate debug into master

# 2023-10-10:
    - moved to /SJStreiff/recordcurator, [v0.0.1-alpha] released

# 2023-10-14:
    - created branch 'expert_additions' for adding more expert det integration capabilities
    - bug fix for det_by formatting problems after cleanup step.

# 2023-10-16:
    - added expert det capability (untested!) when expert data only has collector name and collection number
    - streamlined recordfiler (only one large crossreference of datasets instead of 3). file duplicated to 'recordfiler_v0.2'
    
# 2023-11-02:
    - debugging and refining expert pipeline

# 2023-11-16:
    - huh remove country requirement, not used and problem when expert without country information
    - expert running clean without coordinates/countries

# 2023-11-21:
    - merged branch expert_determinations, new branch 'coord_consolidate: save discrepancies in coordinates between putative duplciates.

# 2024-01-16:
    - added coordinate consolidator which cleans barcodes (needs some manual help) and then deduplicates somwhat and does reverse geocoding to check
        coordinates-country relationship

# 2024-01-19:
    - barcode crosscheck has been sped up exponentially! (with a database of 10s of thousands of records, it takes a couple of minutes to 
            check for duplicated barcodes, not hours!!)

# 2024-01-26:
    - expert issues resolved. 'exp_problem' branch
    - exp integration polished, faster, cleaner, more reliable

# WISHLIST:
    - clean up scripts so 1 main function per file = better overview
    - fuzzy name check to standardise names
    - cosnolidate coordinate discrepancies automatically
    - postgres/SQL interaction automatically