# CMS Data Pipelines
**Pipelines to process CMS data: Medicaid and Medicare**

<!-- toc -->

## Overview

See [Medicaid](doc/Medicaid.md) master document.

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
medicaid.cwl file.

### Python 

#### Package cms

This package contains modules to generate YAML schema for CMS
data from FTS files provided with CMS medicaid and medicare 
export (raw data).

Module [fts2yaml](src/python/cms/fts2yaml.py) is a generic
parser for FTS format for both Medicaid and Medicare.

File transfer summary (FTS) document contains information about 
the data extract. These are plain text files containing
information such as the number of
columns in the data extract, number of rows and the size of the
data file. The FTS document provides the
starting positions, the length and the generic format of 
each of the column (such as character, numeric or date)  

Module 
[create_schema_config](src/python/cms/create_schema_config.py) 
generates 
[YAML schema](doc/Medicaid.md#parsing-fts-files-to-generate-schema) 
for CMS medicaid data by parsing FTS files.



