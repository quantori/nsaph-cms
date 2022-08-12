# Index Builder
**Tool** 	[nsaph.loader.index_builder](../../../../platform/doc/members/index_builder.html)

[//]: # (TODO: use intersphinx)

**Source**: [index.cwl](../members/index_cwl.md)

<!-- toc -->

- [Description](#description)
- [Inputs](#inputs)
- [Outputs](#outputs)

<!-- tocstop -->

## Description
This tool builds all indices for the specified table.
Log file displays real-time progress of building indices


## Inputs

| Name | Type | Default | Description |
|------|------|---------|-------------|
|registry|File| |A path to the data model file |
|domain|string| |the name of the domain|
|table|string| |the name of the table|
|database|File| |Path to database connection file, usually database.ini|
|connection_name|string| |The name of the section in the database.ini file|
|incremental|boolean|`False`| |
|force|boolean|`False`| |
|depends_on|File| |a special field used to enforce dependencies and execution order|

## Outputs

| Name | Type | Description |
|------|------|-------------|
|log|File| |
|errors|stderr| |
