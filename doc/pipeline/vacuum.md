# Index Builder
**Tool** 	[nsaph.loader.vacuum](../../../../platform/doc/members/vacuum.html)

**Source**: [vacuum.cwl](../../src/cwl/vacuum.cwl)

<!-- toc -->

- [Description](#description)
- [Inputs](#inputs)
- [Outputs](#outputs)

<!-- tocstop -->

## Description
This tool runs VACUUM Aanalyze to make subsequent queries
more efficient


## Inputs

| Name | Type | Default | Description |
|------|------|---------|-------------|
|registry|File| |A path to the data model file |
|domain|string| |the name of the domain|
|table|string| |the name of the table|
|database|File| |Path to database connection file, usually database.ini|
|connection_name|string| |The name of the section in the database.ini file|
|depends_on|File| |a special field used to enforce dependencies and execution order|

## Outputs

| Name | Type | Description |
|------|------|-------------|
|log|File| |
|errors|stderr| |
