# Medicare Files Handling

<!--TOC-->
<!--TOC-->

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

## Combining raw files into a single view

Once all raw files are ingested into teh database they are combined 
into two views:

1. Patient summary (aka MBSF, aka Beneficiary summary)
2. Inpatient Admissions (aka hospitalizations, aka medpar)

### Creating Federated Patient Summary
        
The federated patient summary is created in two steps.
The division into two steps is purely because of technical reasons
given some limitations of readability in SQL.


#### First step: Initial in-database data conditioning

The first step creates a view called `medicare.ps`. 
        
This step technically combines all `cms.mbsf_ab*` and `cms.mcr_bene_*`
tables into a single view using `CREATE VIEW` SQL statement.

It also cleanses and conditions data from teh following columns:

* `year` 
  * If it is a string in original file, it is converted to integer
  * If it is two-digit, it is converted to 4 digit
* `state`: added a column with text state id
* `fips2`: added a column with two digit state FIPS code
* `zip`: if original file uses 9-digit zip code, it is split
  into two separate columns, 5 digit `zip` and 4-digit `zip4`.
  The value is also converted to integer value.
* `zip4`: added, when available - the last four digits of 9-digit
  zip code
* `dob`: converted to SQL `DATE` type, from either character or
  SAS numeric form
* `dod` (date of death): converted to SQL `DATE` type,
  from either character or SAS numeric form
* 


The following 
[CWL tool](../src/cwl/medicare_ps.cwl)
is responsible to perform it.

#### Second step: Mapping to county FIPS codes

At the second step, a view called `medicare._ps` is created.  
The only difference between  `medicare.ps` and `medicare._ps`
is that the latter has county FIPS code (`fips3` column)
inferred either SSA county code (`ssa3` column), if it is
available or from the zip code (`zip` column)
if SSA county code is absent. The reason this has to happen
in a separate second step is that both `ssa3` and `zip` are
being cleansed in the first step.

The second step is performed by a general loader utility
based on the 
[Medicare data model definition](../src/python/cms/models/medicare.yaml).

### Creating Beneficiaries table

This is also a two steps operation. The first step
creates an SQL view and the second step stores the data
as a real table.

Essentially it is a `medicare.ps` view grouped by beneficiary id
(`bene_id` column). This step also takes care of documenting any
discrepancies in the data related to:

* dob
* dod
* race
* sex

If there is any discrepancy for a given `bene_id`, then:

* The earliest _**DOB**_ is selected as `dob`
* The latest _**DOD**_ (date of death) is selected  as `dod`
* A comma-separated string containing all race codes is used for `race`
* comma-separated string containing all sex codes is used for `sex`

The following columns are added:

* `duplciates`: a numeric column showing the number of inconsistent
  values for this beneficiary. If it is greater than 1, it means
  there is a discrepancy in the data for this beneficiary
* `dob_latest`: the latest DOB found in the records for this 
  beneficiary. The value of this column is NULL for consistent records
* `dod_earliest`: the earliest DOD found in the records for this 
  beneficiary. The value of this column is NULL for consistent records

