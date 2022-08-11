# Importing Medicaid Data Processed by the Legacy Pipeline

<!-- toc -->

- [Status of this Document](#status-of-this-document)
- [Links to Legacy Documentation](#links-to-legacy-documentation)
  * [main document, describing the data model](#main-document-describing-the-data-model)
  * [Demographics](#demographics)
    + [Data Path](#data-path)
      - [File](#file)
      - [NSAPH VM](#nsaph-vm)
      - [RCE](#rce)
    + [Description of columns](#description-of-columns)
  * [Enrollments](#enrollments)
    + [Documentation](#documentation)
    + [Data Path](#data-path-1)
      - [Files](#files)
      - [NSAPH VM](#nsaph-vm-1)
      - [RCE](#rce-1)
    + [Description of columns](#description-of-columns-1)
  * [Admissions](#admissions)
    + [Data Path](#data-path-2)
      - [Files](#files-1)
      - [NSAPH VM](#nsaph-vm-2)
      - [RCE](#rce-2)
    + [Description of columns](#description-of-columns-2)
- [Examples of ingestion of processed data:](#examples-of-ingestion-of-processed-data)
  * [Demographics:](#demographics)
  * [Enrollments (yearly) and Eligibility (monthly)](#enrollments-yearly-and-eligibility-monthly)
  * [Admissions](#admissions-1)

<!-- tocstop -->

## Status of this Document
This document describes an abandoned attempt to load
already processed medicaid data into NSAPH Data Platform 
PostgreSQL Database. The attempt was abandoned in favor of
creating a reproducible pipeline that ingests raw CMS data
from the packages delivered by ResDac.

See [documentation](Medicaid.md) about the new pipeline.
              
## Links to Legacy Documentation

### main document, describing the data model
https://github.com/NSAPH/data_model

### Demographics
#### Data Path

##### File

    maxdata_demographics.fst

##### NSAPH VM

    /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-demographics_patient/maxdata_demographics.fst
      
##### RCE

    /nfs/nsaph_ci3/data/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-demographics_patient/maxdata_demographics.fst

#### Description of columns

The most information about columns used in the legacy data model can be 
obtained from the R script 
[1_create_demographics_data.R](https://github.com/NSAPH/data_model/blob/master/scripts/medicaid_scripts/processed_data/1_create_demographics_data.R) 
on NSAPH GitHub. The script is also available in the 
[Internal GitLab](https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/dominici_data_pipelines/-/blob/master/medicaid/1_create_demographics_data.R)

### Enrollments    

#### Documentation 

[Medicaid Platform](https://github.com/NSAPH/data_requests/tree/master/request_projects/dec2019_medicaid_platform_cvd)
on NSAPH GitHub

#### Data Path
##### Files

    medicaid_mortality_{YEAR}.fst
                             
for YEAR in [1999:2012]

##### NSAPH VM

    /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-ps_patient-year/

##### RCE

    /nfs/nsaph_ci3/data/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-ps_patient-year/

#### Description of columns

Available in R script 
[2_process_enrollment_data.R](https://github.com/NSAPH/data_model/blob/master/scripts/medicaid_scripts/processed_data/2_process_enrollment_data.R)
on NSAPH GitHub
Also, on [internal GitLab](https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/dominici_data_pipelines/-/blob/master/medicaid/2_process_enrollment_data.R)

### Admissions     

#### Data Path
##### Files

    cvd_admissions_{YEAR}.fst    

for YEAR in [1999:2012]

##### NSAPH VM

    /data/incoming/rce/ci3_health_data/medicaid/cvd/1999_2012/desouza/data

##### RCE

    /nfs/nsaph_ci3/ci3_health_data/medicaid/cvd/1999_2012/desouza/data

#### Description of columns
             
Described in the documented for 
[Medicaid CVD Claims 1999-2012](https://github.com/NSAPH/data_requests/blob/master/request_projects/dec2019_medicaid_platform_cvd/cvd_readme.md)

Also, see R script 
[2_create_cvd_data.R](https://github.com/NSAPH/data_requests/blob/master/request_projects/dec2019_medicaid_platform_cvd/code/2_create_cvd_data.R)
on NSAPH GitHub and on the 
[Internal GitLab](https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/dominici_data_pipelines/-/blob/master/medicaid/code/2_create_cvd_data.R)


## Examples of ingestion of processed data:

All paths are on `nsaph-sandbox01.rc.fas.harvard.edu`

### Demographics:

    python -u -m nsaph.model2 /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/csv/maxdata_demographics.csv.gz

### Enrollments (yearly) and Eligibility (monthly)

    nohup python -u -m nsaph.data_model.model2 --data /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-ps_patient-year/medicaid_mortality_2005.fst -t enrollments_year --threads 4 --page 5000 &

### Admissions

    for f in /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-ip_patient-admission-date/maxdata_*_ip_${year}.fst ; do 
	    date
	    echo $f
	    python -u -m nsaph.data_model.model2 --data $f  -t admissions --page 5000 --log 10000 --threads 2
    done

