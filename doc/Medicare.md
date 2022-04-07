# Medicare Files Handling

## Ingesting Raw Files

### Overview
                                                                   
There are two types of tables:

* Patient summary, aka enrollment, aka denominator
* Inpatient admissions

Unfortunately, the structure of medicare files is different for 
almost every year. 

Summary files for some years come in pairs:

* `mbsf_ab_summary`
* `mbsf_d_cmpnts`

For other years we have a single file:

* `mbsf_abcd_summary`
                                                                   
Inpatient admissions files always follow `medpar_all_file` pattern.

Columns vary from year to year even for similarly named files, 
new columns are being added and column names are sometimes changed.

To add insult to injury, for years prior to 2011 (1999-2010) we do not 
have original files, but preprocessed files with patient summary 
(called denominators) and admissions. They are in SAS 7BDAT format,
however columns are also different for different years.

### Storing in the Database

Given the difference in file structures we create a separate table
for every file. However, to make it easier to join these tables we:

* Add a column containing original file name to every table
* Add generated columns with uniform names for:
  * Year
  * State
  * Bene_Id 
  * Zip code

Originally, these data is stored in columns with the following possible names:

    "bene_id":  (None, ["bene_id", "intbid", "qid", "bid_5333*"]),
    "state":  (None, ["state", "ssa_state", "state_code",
                      "bene_rsdnc_ssa_state_cd", "state_cd",
                      "medpar_bene_rsdnc_ssa_state_cd"]),
    "zip": (None, ["zip", "zipcode", "bene_zip_cd", "bene_zip",
                   "bene_mlg_cntct_zip_cd",
                   "medpar_bene_mlg_cntct_zip_cd"]),
    "year": (None, ["year", "enrolyr", "bene_enrollmt_ref_yr",
                    "rfrnc_yr"])
    
When a table has no natural primary key (admission tables) we add a record 
number column. This column ha sno meaning but allows to trace a record to the 
original data.



### Files for 1999 to 2010 

These files are in SAS7BDAT format. They have been stored on RCE
in two directories:

* denominator
* inpatient

One file per year. SAS7BDAT format contains metadata with column 
names and types. We use this metadata to generate appropriate database schema.

See the code for handling these files:

* [Metadata and data model](../src/python/cms/tools/mcr_sas2yaml.py)
* [Ingesting data](../src/python/cms/tools/mcr_sas2db.py)

### Files for Years 2011 and later

These files are original files from Resdac. They come in Fixed Width Format
(FWF). For each file the structure is described in File Transfer 
Summary (FTS) file. Unfortunately these files are intended for reading by 
a human and is difficult to parse automatically. A 
[partial parser](../src/python/cms/fts2yaml.py) that
relies on a known file type is implemented in Python. The information 
extarcted by the parser is used to:

* Generate data model (database schema)
* Generate metadata for the FWF Reader

                                              
