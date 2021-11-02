# Patient Summary Loader
**Tool** 	[nsaph.loader.data_loader](../../../../platform/doc/members/data_loader.html)

**Source**: [load_ps.cwl](../../src/cwl/load_ps.cwl)

<!-- toc -->

- [Description](#description)
- [Inputs](#inputs)
- [Outputs](#outputs)

<!-- tocstop -->

## Description
This tool loads patient summary data into a database.
It should be run after the data is inspected and
data model is created from FTS files


## Inputs

| Name | Type | Default | Description |
|------|------|---------|-------------|
|registry|File| |A path to the data model file |
|domain|string|`cms`|the name of the domain|
|table|string|`ps`|the name of the table being populated|
|database|File| |Path to database connection file, usually database.ini|
|connection_name|string| |The name of the section in the database.ini file|
|incremental|boolean| |if defined, then the data ingestion is incremental. Transactions are committed after every file is processed and files that have already been processed are skipped |
|input|Directory| |A path to directory, containing unpacked CMS files. The tool will recursively look for data files according to provided pattern |
|pattern|string|`**/maxdata_*_ps_*.csv*`| |
|threads|int|`4`|number of threads, concurrently writing into the database|
|page_size|int|`1000`|explicit page size for the database|
|log_frequency|long|`100000`|informational logging occurs every specified number of records|
|limit|long| |if specified, the process will stop after ingesting the specified number of records |
|depends_on|File| |a special field used to enforce dependencies and execution order|

## Outputs

| Name | Type | Description |
|------|------|-------------|
|log|File| |
