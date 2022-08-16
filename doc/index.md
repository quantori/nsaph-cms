# CMS Manipulation Package 

**Pipelines to process CMS data: Medicaid and Medicare**


```{toctree}
---
maxdepth: 4
glob: true
hidden: 
---
LegacyMedicaid.md
Medicaid.md
Medicare.md
QueringMedicaid.md
```

```{contents}
---
local:
---
```


## Overview
                                               
Data processing pipelines included in this package
create a data warehouse with health data (Medicare and Medicaid).
They perform ingestion of raw data into teh database, data 
cleansing and deduplication , when possible, data quality analysis
and optimization of the tables for efficient queries.

Please see the following documents for details:

* Data model and processing of [Medicaid](Medicaid.md) data
* Tips on [querying of Medicaid data](QueringMedicaid.md)

## Project Structure

Top level directories are:

    - doc
    - src

Doc directory contains documentation.

Src directory contains software source code. 
See details in [Software Sources](#software-sources) section.

### Software Sources

The directories under sources are:

    - cwl
    - python

### CWL 

CWL folder contains reusable workflows, packaged as tools 
that can and should be used by
all NSAPH pipelines.

Each processing step of CMS data is packaged as a 
standalone tool that can be run individually. 
Each tool is individually documented.
The tools are combined into a workflow represented by
[medicaid.cwl](pipeline/medicaid.rst)
and 
[medicare.cwl](pipeline/medicare.rst) files.

### Python 

#### Package cms

This package contains modules to generate YAML schema for CMS
data from FTS files provided with CMS medicaid and medicare 
export (raw data).

Module [fts2yaml](members/fts2yaml.rst) is a generic
parser for FTS format for both Medicaid and Medicare.

File transfer summary (FTS) document contains information about 
the data extract. These are plain text files containing
information such as the number of
columns in the data extract, number of rows and the size of the
data file. The FTS document provides the
starting positions, the length and the generic format of 
each of the column (such as character, numeric or date)  

Module 
[create_schema_config](members/create_schema_config) 
generates 
[YAML schema](Medicaid.md#parsing-fts-files-to-generate-schema) 
for CMS medicaid data by parsing FTS files.

##### Subpackage with miscellaneous tools for handling CMS data 

* `nsaph.tools`

This package contains code that was written to try to extract
corrupted medicare data for 2015. Ultimately, this attempt
was unsuccessful.

### SQL

File [procedures](members/procedures.md) 
addresses the problem that creating 
[Medicaid eligibility table](Medicaid.md#eligibility)
in a single transaction requires too much time and memory.
The stored procedures in this file split populating this table
with data either by beneficiary or by year and state. Splitting by beneficiary
(i.e. using one database transaction per beneficiary) works best.

File [functions](members/functions.md) contain helper functions
to parse dates in non-standard formats that are encountered in 
raw medicare files that we have.

## Indices and tables

* [](genindex)
* [](modindex)
